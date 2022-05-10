import sys
import warnings
import socket
import hashlib
import time
import os
import json
from datetime import datetime, timedelta
import sqlite3
from collections import namedtuple

import anidbcli.encryptors as encryptors
from anidbcli.protocol import AnidbApiCall, AnidbApiBanned, AnidbResponse, FileKeyED2K, FileKeyFID, FileRequest

API_ADDRESS = "api.anidb.net"
API_PORT = 9000
SOCKET_TIMEOUT = 10
MAX_RECEIVE_SIZE = 65507 # Max size of an UDP packet is about 1400B anyway
RETRY_COUNT = 3
REQUEST_CONVERGE_MAX_COUNT = 5

API_ENDPOINT_ENCRYPT = "ENCRYPT user=%s&type=1"
API_ENDPOINT_LOGIN = "AUTH user=%s&pass=%s&protover=3&client=anidbcli&clientver=1&enc=UTF8"
API_ENDPOINT_LOGOUT = "LOGOUT s=%s"

ENCRYPTION_ENABLED = 209
LOGIN_ACCEPTED = 200
LOGIN_ACCEPTED_NEW_VERSION_AVAILABLE = 201


def get_persistence_base_path():
    path = os.getenv("APPDATA")
    if path is None: # Unix
        return os.path.join(os.getenv("HOME"), ".anidbcli")
    else:
        return os.path.join(path, "anidbcli")


def get_cache_path():
    return os.path.join(get_persistence_base_path(), "cache.sqlite3")


def get_persistent_file_path():
    return os.path.join(get_persistence_base_path(), "session.json")


class ImplicitField(namedtuple('_ImplicitField', ['name'])):
    pass


class AnidbConnector:
    def __init__(self, credentials, *, bind_addr=None, salt=None, session=None, persistent=False, api_key=None):
        """For class initialization use class methods create_plain or create_secure."""
        self._suppress_network_activity = False
        self._credentials = credentials
        self._crypto = encryptors.PlainTextCrypto()
        self._last_sent_request = 0

        # persistence state + persisted (to disk) information
        self._persistent = bool(persistent)
        self._salt = salt
        self._session = session
        self._bind_addr = None
        if bind_addr:
            self._bind_addr = tuple(bind_addr)

        if self._persistent:
            self._load_persistence()
        if self._salt and api_key:
            if not self._session:
                # TODO: we need to tell the server we're starting encryption?
                # need revalidation for code CFG vs server flow.
                pass
            # if we have a session, assume we're already encryption-enabled?  We'd have to
            # store state telling us whether or not we sent the encrypt packet yet in the persistence file.
            md5 = hashlib.md5(bytes(api_key + salt, "ascii"))
            instance._crypto = encryptors.Aes128TextEncryptor(md5.digest())

        self._initialize_socket()
        self._cache = sqlite3.connect(
            get_cache_path(),
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
        self._cache.executescript('''
            CREATE TABLE IF NOT EXISTS anidb_file_negative_cache (
                id INTEGER PRIMARY KEY,
                ed2k TEXT,
                size INTEGER,
                expiration INTEGER
            );
            CREATE UNIQUE INDEX IF NOT EXISTS anidb_file_negative_cache_ed2k_and_size ON anidb_file_negative_cache(ed2k, size);
            -- used to expire entries quickly
            CREATE INDEX IF NOT EXISTS anidb_file_negative_cache_expiration_id ON anidb_file_negative_cache(expiration, id);

            CREATE TABLE IF NOT EXISTS anidb_files (
                id INTEGER PRIMARY KEY,
                fid INTEGER,
                ed2k TEXT,
                size INTEGER,
                expiration INTEGER
            );
            CREATE UNIQUE INDEX IF NOT EXISTS anidb_files_ed2k_and_size ON anidb_files(ed2k, size);
            CREATE UNIQUE INDEX IF NOT EXISTS anidb_files_fid ON anidb_files(fid);

            CREATE TABLE IF NOT EXISTS metadata (
                object_prop_key TEXT PRIMARY KEY,
                prop_value TEXT,
                expiration INTEGER
            );
            -- used to expire entries quickly
            CREATE INDEX IF NOT EXISTS metadata_expiration_id ON metadata(expiration, object_prop_key);

            
            -- CREATE TABLE IF NOT EXISTS file_lookup_cache (
            --     filename TEXT PRIMARY KEY,
            --     ed2k TEXT,
            --     size INTEGER,
            --     expiration INTEGER
            -- );
            -- CREATE UNIQUE INDEX IF NOT EXISTS file_lookup_cache_ed2k_and_size ON file_lookup_cache(expiration, filename);
        ''').fetchall()

    def inject_negative_cache_record(self, ed2k, size):
        with self._cache:
            expiration = datetime.now() + timedelta(days=300)
            self._cache.execute('''
                INSERT INTO anidb_file_negative_cache(ed2k, size, expiration) VALUES(?, ?, ?)
                    ON CONFLICT(ed2k, size) DO UPDATE SET expiration = excluded.expiration;
            ''', ed2k, size, int(expiration.timestamp()))

    def check_negative_cache(self, req):
        # req. 
        # print(f"\n\n\rXXX :: req = {req!r}\n\n", file=sys.stderr)
        hash_key = None
        if isinstance(req, FileRequest):
            if isinstance(req.key, FileKeyFID):
                # If we have a File ID, then this file cannot be unknown.
                return False
            assert isinstance(req.key, FileKeyED2K)
            hash_key = req.key
        elif isinstance(req, dict):
            ed2k = req.get('ed2k', None)
            size = req.get('size', None)
            if ed2k is not None and size is not None:
                hash_key = FileKeyED2K(ed2k, size)
        elif hash_key is None:
            return False

        with self._cache:
            now = datetime.now()
            if isinstance(hash_key, FileKeyED2K):
                self._cache.execute('DELETE FROM anidb_file_negative_cache WHERE expiration < ?', (int(now.timestamp()), ))
                query = '''
                    SELECT expiration
                    FROM anidb_file_negative_cache
                    WHERE ed2k = ? AND size = ? AND ? <= expiration
                '''
                for (_one, ) in self._cache.execute(query, (hash_key.ed2k, hash_key.size, int(now.timestamp()))):
                    return True
            elif isinstance(hash_key, FileKeyFID):
                # If we have a File ID, then this file cannot be unknown.
                return False
            else:
                cls_name = "{0.__class__.__module__}.{0.__class__.__name__}".format(hash_key)
                allowed = {FileKeyED2K, FileKeyFID}
                raise TypeError("expected hash key (in {0!r}), got {1}: {2}".format(allowed, cls_name, hash_key))
        return False

    def _inject_cache_file_identifier(self, req, accumulated_response):
        # print("_inject_cache_file_identifier(req={!r}, accumulated_response={!r})".format(req, accumulated_response), file=sys.stderr)
        if isinstance(req.key, FileKeyED2K):
            with self._cache:
                self._cache.execute('''
                    INSERT INTO anidb_files(fid, ed2k, size) VALUES(?, ?, ?)
                        ON CONFLICT DO NOTHING;
                ''', (
                    accumulated_response.decoded['fid'],
                    req.key.ed2k, req.key.size,
                ))

    def _inject_cache_file(self, req, accumulated_response):
        object_key = f"f{accumulated_response.decoded['fid']}"
        expiration = datetime.now() + timedelta(days=300)
        with self._cache:
            for (k, v) in accumulated_response.iter_raw_kv(req, suppress_truncation_error=True):
                exec_args = ('{}:{}'.format(object_key, k.name), v, int(expiration.timestamp()))
                self._cache.execute('''
                    INSERT INTO metadata(object_prop_key, prop_value, expiration) VALUES(?, ?, ?)
                        ON CONFLICT(object_prop_key) DO UPDATE SET prop_value = excluded.prop_value;
                ''', exec_args)

    def _load_persistence(self):
        try:
            with open(path, "r") as f:
                persistence_doc = json.load(f)
        except IOError as e:
            if e.errno == errno.ENOENT:
                pass
        if (time.time() - persistence_doc['timestamp']) < 60 * 10:
            pass

    def _initialize_socket(self):
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        if self._bind_addr:
            self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self._socket.bind(self._bind_addr)
        remote_addr = (socket.gethostbyname_ex(API_ADDRESS)[2][0], API_PORT)
        self._socket.connect(remote_addr)
        self._socket.settimeout(SOCKET_TIMEOUT)

    # @classmethod
    # def create_from_session(cls, session, bind_addr, api_key, salt):
    #     """Creates instance from an existing session. If salt is not None, encrypted instance is created."""
    #     return cls(bind_addr=bind_addr, session=session, salt=salt)

    @classmethod
    def create_plain(cls, username, password):
        """Creates unencrypted UDP API connection using the provided credenitals."""
        return cls((username, password))

    # @classmethod
    # def create_secure(cls, username, password, api_key):
    #     """Creates AES128 encrypted UDP API connection using the provided credenitals and users api key."""
    #     instance = cls((username, passord))
    #     enc_res = instance._send_request_raw(API_ENDPOINT_ENCRYPT % username)
    #     if enc_res.code != ENCRYPTION_ENABLED:
    #         raise Exception(enc_res.data)
    #     instance._salt = enc_res.data.split(" ", 1)[0]
    #     md5 = hashlib.md5(bytes(api_key + instance._salt, "ascii"))
    #     instance._crypto = encryptors.Aes128TextEncryptor(md5.digest())
    #     instance._login(username, password)
    #     return instance

    def _send_request_raw(self, data, suppress_encryption=False):
        if self._suppress_network_activity:
            raise Exception('network activity suppressed')
        now = time.monotonic()
        since_last_sent = now - self._last_sent_request
        if since_last_sent < 2.0:
            time.sleep(2.0 - since_last_sent)
        self._last_sent_request = now

        if not suppress_encryption:
            data = self._crypto.Encrypt(data)
        self._socket.send(data)
        response = self._socket.recv(MAX_RECEIVE_SIZE)
        if response.startswith(b'555 '):
            raise AnidbApiBanned(response.decode('utf-8'))
        if not suppress_encryption:
            response = self._crypto.Decrypt(response)
        return AnidbResponse.parse(response.rstrip("\n"))

    def _login(self):
        if self._suppress_network_activity:
           raise Exception('network activity suppressed')
        if self._session:
            return
        (username, password) = self._credentials
        response = self._send_request_raw(API_ENDPOINT_LOGIN % (username, password))
        if response.code == LOGIN_ACCEPTED or response.code == LOGIN_ACCEPTED_NEW_VERSION_AVAILABLE:
            self._session = response.data.split(' ', 1)[0]
            if self._persistent:
                # TODO: write persistence file
                pass
        else:
            raise Exception(response.data)

    def close(self):
        if not self._session:
            return  # already closed.
        self._send_request_raw(API_ENDPOINT_LOGOUT % self._session)
        self._session = None
        self._socket.close()

    def send_request_helper_legacy(self, content):
        """Sends request to the API and returns a dictionary containing response code and data."""
        tries = RETRY_COUNT
        while 0 < tries:
            if not self._session:
                self._login()
            tries -= 1
            try:
                response = self._send_request_raw(f"{content}&s={self._session}")
                if response.code == AnidbResponse.CODE_LOGIN_FIRST:
                    self._session = None
                return response
            except socket.timeout:
                if tries == 0:
                    raise
                else:
                    continue

    def _locally_service_field_values(self, key, fields):
        with self._cache:
            if isinstance(key, FileKeyED2K):
                for (fid, ) in self._cache.execute('SELECT fid FROM anidb_files WHERE ed2k = ? AND size = ?', (key.ed2k, key.size)):
                    key = FileKeyFID(fid)
        if isinstance(key, FileKeyFID):
            yield ImplicitField('fid'), fid
            for f in fields:
                object_prop_key = "{}:{}".format(key, f.name)
                for (prop_value, ) in self._cache.execute('SELECT prop_value FROM metadata WHERE object_prop_key = ?', (object_prop_key, )):
                    yield f, f.filter_value(prop_value)

    def send_request(self, req):
        if self.check_negative_cache(req):
            return AnidbResponse(AnidbResponse.CODE_RESULT_NO_SUCH_FILE, 'NO SUCH FILE (cached)')

        locally_serviced_fields = {}
        want_fields = set(req.fields)

        for (f, v) in self._locally_service_field_values(req.key, req.fields):
            locally_serviced_fields[f.name] = v
            if not isinstance(f, ImplicitField):
                want_fields.remove(f)

        req.fields = [f for f in req.fields if f in want_fields]
        print(f"locally_serviced_fields = {locally_serviced_fields!r}", file=sys.stderr)
        print(f"need network access for = {req.fields!r}", file=sys.stderr)
        if not want_fields:
            return AnidbResponse(AnidbResponse.CODE_RESULT_FILE, '', decoded=locally_serviced_fields)

        is_rich = False
        if isinstance(req, AnidbApiCall):
            # if hasattr(req, 'next_request'):
            #     return self.send_request_helper2(req)
            is_rich = True
            res = self.send_request_helper_legacy(req.serialize())
        else:
            res = self.send_request_helper_legacy(req)
        if is_rich:
            res.decode_with_query(req, suppress_truncation_error=True)

        if is_rich and res.code == AnidbResponse.CODE_RESULT_NO_SUCH_FILE:
            self.inject_negative_cache_record(ed2k, size)
        if is_rich and res.code == AnidbResponse.CODE_RESULT_FILE:
            self._inject_cache_file_identifier(req, res)
            self._inject_cache_file(req, res)
        return res
