import socket
import hashlib
import time
import os
import json

import anidbcli.encryptors as encryptors
from anidbcli.protocol import AnidbApiCall, AnidbApiBanned, AnidbResponse

API_ADDRESS = "api.anidb.net"
API_PORT = 9000
SOCKET_TIMEOUT = 5
MAX_RECEIVE_SIZE = 65507 # Max size of an UDP packet is about 1400B anyway
RETRY_COUNT = 3

API_ENDPOINT_ENCRYPT = "ENCRYPT user=%s&type=1"
API_ENDPOINT_LOGIN = "AUTH user=%s&pass=%s&protover=3&client=anidbcli&clientver=1&enc=UTF8"
API_ENDPOINT_LOGOUT = "LOGOUT s=%s"

ENCRYPTION_ENABLED = 209
LOGIN_ACCEPTED = 200
LOGIN_ACCEPTED_NEW_VERSION_AVAILABLE = 201

def get_persistent_file_path():
    path = os.getenv("APPDATA")
    if path is None: # Unix
        path = os.getenv("HOME")
        path = os.path.join(path, ".anidbcli", "session.json")
    else:
        path = os.path.join(path, "anidbcli", "session.json")
    return path


class AnidbConnector:
    def __init__(self, credentials, *, bind_addr=None, salt=None, session=None, persistent=False, api_key=None):
        """For class initialization use class methods create_plain or create_secure."""
        self._credentials = credentials
        self._crypto = encryptors.PlainTextCrypto()
        self._last_sent_request = 0

        # persistence state + persisted (to disk) information
        self._persistent = bool(persistent)
        self._salt = salt
        self._session = session
        self._bind_addr = tuple(bind_addr)

        if self._persistent:
            self._load_persistence()
        if (salt and api_key) and not session:
            # if we have a session, assume we're already encryption-enabled?  We'd have to
            # store state telling us whether or not we sent the encrypt packet yet in the persistence file.
            md5 = hashlib.md5(bytes(api_key + salt, "ascii"))
            instance._crypto = encryptors.Aes128TextEncryptor(md5.digest())
            # TODO: we need to tell the server we're starting encryption.
        self._initialize_socket()

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
        if bind_addr:
            self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self._socket.bind(tuple(bind_addr))
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
        return cls((username, passord))

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
        if self._session:
            return
        (username, password) = self._credentials
        response = self._send_request_raw(API_ENDPOINT_LOGIN % (username, password))
        if response.code == LOGIN_ACCEPTED or response.code == LOGIN_ACCEPTED_NEW_VERSION_AVAILABLE:
            self._session = response.data.split(" ")[0]
            if self._persistent:
                pass
        else:
            raise Exception(response.data)

    def close(self):
        if not self._session:
            return  # already closed.
        self._send_request_raw(API_ENDPOINT_LOGOUT % self._session)
        self._session = None
        self._socket.close()

    # def close(self, persistent, persist_file):
    #     """Logs out the user from current session and closes the connection."""
    #     if not self._session:
    #         raise Exception("Cannot logout: No active session.")
    #     if persistent:
    #         try:
    #             os.makedirs(os.path.dirname(persist_file))
    #         except FileExistsError:
    #             pass
    #         d = dict()
    #         d["session_key"] = self._session
    #         d["timestamp"] = time.time()
    #         d["salt"] = None
    #         d["sockaddr"] = self._socket.getsockname()
    #         if self._salt:
    #             d["salt"] = self._salt
    #         with open(persist_file, "w") as file:
    #             file.writelines(json.dumps(d))
    #     else:
    #         try:
    #             os.remove(persist_file)
    #         except:
    #             pass
    #         self._send_request_raw(API_ENDPOINT_LOGOUT % self._session)
    #     self._socket.close()


    def send_request(self, content):
        """Sends request to the API and returns a dictionary containing response code and data."""
        original_content = content
        if isinstance(original_content, AnidbApiCall):
            content = original_content.serialize()

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
