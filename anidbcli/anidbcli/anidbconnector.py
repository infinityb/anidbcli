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
from anidbcli.protocol import AnidbApiCall, AnidbApiBanned, AnidbResponse, FileKeyED2K, FileKeyFID, FileRequest, AnidbApiNotFound

from sqlalchemy.sql.expression import func
import sqlalchemy.engine
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy import create_engine, text, MetaData, Table, Column, Integer, String, DateTime, Text, select, Index, UniqueConstraint, delete
from sqlalchemy.schema import CreateTable
from sqlalchemy.sql.functions import count


metadata_obj = MetaData()
anidb_file_negative_cache = Table(
    "anidb_file_negative_cache",
    metadata_obj,
    Column("id", Integer, primary_key=True),
    Column("ed2k", Text, nullable=False),
    Column("size", Integer, nullable=False),
    Column("expiration", Integer, nullable=False),
    UniqueConstraint("ed2k", "size", name="anidb_key"),
)
Index("anidb_file_negative_cache_expiration", anidb_file_negative_cache.c.expiration)

anidb_file_negative_cache2 = Table(
    "anidb_file_negative_cache2",
    metadata_obj,
    Column("id", Integer, primary_key=True),
    Column("ed2k", Text, nullable=False),
    Column("size", Integer, nullable=False),
    Column("failure_count", Integer, nullable=False),
    Column("failed_on", Integer, nullable=False),
    Column("synthesize_failure_until", Integer, nullable=False),
    Column("expiration", Integer, nullable=False),
    UniqueConstraint("ed2k", "size", name="anidb_key"),
)
Index("anidb_file_negative_cache2_expiration", anidb_file_negative_cache.c.expiration)

anidb_files = Table(
    "anidb_files",
    metadata_obj,
    Column("id", Integer, primary_key=True),
    Column("fid", Integer, nullable=False),
    Column("ed2k", Text, nullable=False),
    Column("size", Integer, nullable=False),
    Column("expiration", Integer, nullable=False),
    UniqueConstraint("ed2k", "size", name="anidb_key"),
)
Index("anidb_files_fid", anidb_files.c.expiration)
Index("anidb_files_expiration", anidb_files.c.expiration)

anidb_metadata = Table(
    "metadata",
    metadata_obj,
    Column("object_prop_key", Text, primary_key=True),
    Column("prop_value", Text, nullable=False),
    Column("expiration", Integer, nullable=False),
)

API_ADDRESS = "api.anidb.net"
API_PORT = 9000
SOCKET_TIMEOUT = 10
MAX_RECEIVE_SIZE = 65507
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


def _convert_return_iter_to_list(func):
    def wrapper(*args, **kwargs):
        return list(func(*args, **kwargs))
    return wrapper


class AnidbCacheNoop:
    def check_negative_cache(self, req):
        return False

    def inject_cache(self, req, res):
        return

    def locally_service_field_values(self, key, fields):
        return []


class AnidbCacheSqlAlchemy:
    def __init__(self, engine_url):
        self._sqlite_engine = create_engine(engine_url, echo=False)
        with self._sqlite_engine.connect() as conn:
            metadata_obj.create_all(conn)
            conn.commit()

    def _inject_negative_cache_record(self, req):
        if hasattr(req, 'key') and isinstance(req.key, FileKeyED2K):
            return self._inject_negative_cache_record_file_key_ed2k(req.key.ed2k, req.key.size)
        print("want to insert negative cache record for {!r}, but type is not understood", file=sys.stderr)

    def _inject_negative_cache_record_file_key_ed2k(self, ed2k, size):
        with self._sqlite_engine.connect() as conn:
            metadata_obj.create_all(conn)
            conn.commit()
            now = datetime.now()
            max_negative_ttl = now + timedelta(days=30)
            expiration = now + timedelta(days=300)
            my_upsert = insert(anidb_file_negative_cache).values(
                ed2k=ed2k,
                size=size,
                expiration=int(expiration.timestamp())
            ).on_conflict_do_update(
                index_elements=['ed2k', 'size'],
                set_={'expiration': int(expiration.timestamp())}
            )
            conn.execute(my_upsert)
            conn.commit()
            ##
            failure_count_default = 1
            failed_on = int(now.timestamp())
            insert_stmt = insert(anidb_file_negative_cache2).values(
                ed2k=ed2k,
                size=size,
                failure_count=failure_count_default,
                failed_on=failed_on,
                synthesize_failure_until=failed_on + 3600 * failure_count_default,
                expiration=int(expiration.timestamp()))

            in_one_hour = now + timedelta(hours=1)
            synthesize_failure_until = func.min(
                max_negative_ttl.timestamp(),
                int(in_one_hour.timestamp()) + 3600 * anidb_file_negative_cache2.c.failure_count)
            my_upsert = insert_stmt.on_conflict_do_update(
                index_elements=['ed2k', 'size'],
                set_={
                    'failure_count': anidb_file_negative_cache2.c.failure_count + 1,
                    'synthesize_failure_until': synthesize_failure_until,
                    'expiration': int(expiration.timestamp()),
                })
            conn.execute(my_upsert)
            conn.commit()

    def _inject_cache_file_identifier(self, req, accumulated_response):
        if hasattr(req, 'key') and isinstance(req.key, FileKeyED2K):
            with self._sqlite_engine.connect() as conn:
                query = insert(anidb_files).values(
                    fid=accumulated_response.decoded['fid'],
                    ed2k=req.key.ed2k,
                    size=req.key.size,
                )
                conn.execute(query)
                conn.commit()

    def _inject_cache_file(self, req, accumulated_response):
        object_key = f"f{accumulated_response.decoded['fid']}"
        expiration = datetime.now() + timedelta(days=300)
        with self._sqlite_engine.connect() as conn:
            for (k, v) in accumulated_response.iter_raw_kv(req, suppress_truncation_error=True):
                conn.execute(insert(anidb_metadata).values(**{
                    'object_prop_key': f'{object_key}:{k.name}',
                    'prop_value': v,
                    'expiration': int(expiration.timestamp()),
                }))
            conn.commit()

    def check_negative_cache(self, req):
        """
        returns true if we think this record does not exist,
        returns false if the record may exist
        """
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
        now = datetime.now()
        if isinstance(hash_key, FileKeyED2K):
            with self._sqlite_engine.connect() as conn:
                conn.execute(delete(anidb_file_negative_cache2).where(anidb_file_negative_cache2.c.expiration <= int(now.timestamp())))
                conn.commit()
                query = select(anidb_file_negative_cache2.c.synthesize_failure_until).where(
                    (anidb_file_negative_cache2.c.ed2k == hash_key.ed2k)
                    & (anidb_file_negative_cache2.c.size == hash_key.size)
                    & (int(now.timestamp()) <= anidb_file_negative_cache2.c.synthesize_failure_until))
                with conn.execute(query) as cursor:
                    for _ in cursor:
                        return True
        # if isinstance(hash_key, FileKeyED2K):
        #     with self._sqlite_engine.connect() as conn:
        #         conn.execute(delete(anidb_file_negative_cache).where(anidb_file_negative_cache.c.expiration < int(now.timestamp())))
        #         conn.commit()
        #         query = select(anidb_file_negative_cache.c.expiration).where(
        #             (anidb_file_negative_cache.c.ed2k == hash_key.ed2k)
        #             & (anidb_file_negative_cache.c.size == hash_key.size)
        #             & (int(now.timestamp()) <= anidb_file_negative_cache.c.expiration))
        #         with conn.execute(query) as cursor:
        #             for _ in cursor:
        #                 return True
        elif isinstance(hash_key, FileKeyFID):
            # If we have a File ID, then this file cannot be unknown.
            return False
        else:
            cls_name = "{0.__class__.__module__}.{0.__class__.__name__}".format(hash_key)
            allowed = {FileKeyED2K, FileKeyFID}
            raise TypeError("expected hash key (in {0!r}), got {1}: {2}".format(allowed, cls_name, hash_key))
        return False

    def inject_cache(self, req, res):
        if isinstance(req, FileRequest):
            self._inject_cache_file_identifier(req, res)
            self._inject_cache_file(req, res)

    @_convert_return_iter_to_list
    def locally_service_field_values(self, key, fields):
        if isinstance(key, FileKeyED2K):
            with self._sqlite_engine.connect() as conn:
                query = select(anidb_files.c.fid).where(
                    (anidb_files.c.ed2k == key.ed2k)
                    & (anidb_files.c.size == key.size)
                )
                with conn.execute(query) as iterator:
                    for (fid,) in iterator:
                        key = FileKeyFID(fid)
                        break
        if key is None:
            return
        if isinstance(key, FileKeyFID):
            yield ImplicitField('fid'), key.fid
            field_query_fields_by_key = {"{}:{}".format(key, f.name): f for f in fields}
            obj_prop_keys = list(field_query_fields_by_key.keys())
            with self._sqlite_engine.connect() as conn:
                query = select(anidb_metadata).where(anidb_metadata.c.object_prop_key.in_(obj_prop_keys))
                with conn.execute(query) as iterator:
                    for a_metadata in iterator:
                        f = field_query_fields_by_key[a_metadata.object_prop_key]
                        yield f, f.filter_value(a_metadata.prop_value)

class AnidbConnector:
    DEFAULT_SLEEP_INTERVAL_SECONDS = 2.0
    def __init__(self, credentials, *, bind_addr=None, salt=None, session=None, persistent=False, api_key=None, cache_impl=None):
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
        self._sleep_interval = self.DEFAULT_SLEEP_INTERVAL_SECONDS
        if bind_addr:
            self._bind_addr = tuple(bind_addr)

        self._cache = cache_impl
        if self._cache is None:
            self._cache = AnidbCacheNoop()
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
        try:
            os.mkdir(get_persistence_base_path())
        except FileExistsError:
            pass

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

    @classmethod
    def create_plain(cls, username, password):
        """Creates unencrypted UDP API connection using the provided credenitals."""
        cache_impl = AnidbCacheSqlAlchemy(engine_url = sqlalchemy.engine.URL(
            drivername='sqlite+pysqlite',
            username=None,
            password=None,
            host=None,
            port=None,
            database=get_cache_path(),
            query={},
        ))
        return cls((username, password), cache_impl=cache_impl)

    def _send_request_raw(self, data, suppress_encryption=False):
        if self._suppress_network_activity:
            raise Exception('network activity suppressed')
        now = time.monotonic()
        since_last_sent = now - self._last_sent_request
        if since_last_sent < self._sleep_interval:
            time.sleep(self._sleep_interval - since_last_sent)
        self._last_sent_request = now

        if not suppress_encryption:
            data = self._crypto.Encrypt(data)
        self._socket.send(data)
        response = self._socket.recv(MAX_RECEIVE_SIZE)
        if response.startswith(b'555 '):
            raise AnidbApiBanned(
                response.decode('utf-8'),
                code_received=555)
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

    def send_request(self, req):
        if self._cache.check_negative_cache(req):
            return AnidbResponse(AnidbResponse.CODE_RESULT_NO_SUCH_FILE, 'NO SUCH FILE (cached)')

        locally_serviced_fields = {}
        want_fields = set(req.fields)
        if isinstance(req, FileRequest):
            locally_serviced_fields_keys = []
            for (f, v) in self._cache.locally_service_field_values(req.key, req.fields):
                locally_serviced_fields[f.name] = v
                if not isinstance(f, ImplicitField):
                    locally_serviced_fields_keys.append(f)
                    want_fields.remove(f)
            locally_serviced_fields_msg = ', '.join(f.short_code() for f in locally_serviced_fields_keys)
            print(f"locally_serviced_fields: {locally_serviced_fields_msg}", file=sys.stderr)

        req.fields = [f for f in req.fields if f in want_fields]
        if isinstance(req, FileRequest) and not req.fields:
            return AnidbResponse(AnidbResponse.CODE_RESULT_FILE, '', decoded=locally_serviced_fields)
        
        need_network_access_for = ', '.join(f.short_code() for f in req.fields)
        print(f"need network access for: {need_network_access_for}", file=sys.stderr)
        if isinstance(req, FileRequest) and self._suppress_network_activity:
            return AnidbResponse(AnidbResponse.CODE_RESULT_NO_SUCH_FILE, 'NO SUCH FILE (suppressed query and not cached)')
        
        is_rich = False
        if isinstance(req, AnidbApiCall):
            # if hasattr(req, 'next_request'):
            #     return self.send_request_helper2(req)
            is_rich = True
            res = self.send_request_helper_legacy(req.serialize())
            try:
                req.validate_response_has_valid_code(res)
            except AnidbApiNotFound as e:
                self._cache._inject_negative_cache_record(req)
        else:
            res = self.send_request_helper_legacy(req)
        if is_rich:
            res.decode_with_query(req, suppress_truncation_error=True)
            self._cache.inject_cache(req, res)
        return res
