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



class AnidbConnector:
    def __init__(self, bind_addr = None):
        """For class initialization use class methods create_plain or create_secure."""
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        if bind_addr:
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.socket.bind(tuple(bind_addr))
        self.socket.connect((socket.gethostbyname_ex(API_ADDRESS)[2][0], API_PORT))
        self.socket.settimeout(SOCKET_TIMEOUT)
        self._crypto = encryptors.PlainTextCrypto()
        self._salt = None
        self._last_sent_request = 0

    def _send_request_raw(self, data, tries=1):
        data = self._crypto.Encrypt(data)

        while 0 < tries:
            tries -= 1
            now = time.monotonic()
            since_last_sent = now - self._last_sent_request
            if since_last_sent < 2.0:
                time.sleep(2.0 - since_last_sent)
            self._last_sent_request = now

            self.socket.send(data)
            try:
                response = self.socket.recv(MAX_RECEIVE_SIZE)
            except socket.timeout:
                if tries == 0:
                    raise
                else:
                    continue
            if response.startswith(b'555 '):
                raise AnidbApiBanned(response.decode('utf-8'))
            response = self._crypto.Decrypt(response)
            return AnidbResponse.parse(response.rstrip("\n"))

    @classmethod
    def create_plain(cls, username, password):
        """Creates unencrypted UDP API connection using the provided credenitals."""
        instance = cls()
        instance._login(username, password)
        return instance

    @classmethod
    def create_secure(cls, username, password, api_key):
        """Creates AES128 encrypted UDP API connection using the provided credenitals and users api key."""
        instance = cls()
        enc_res = instance._send_request_raw(API_ENDPOINT_ENCRYPT % username)
        if enc_res.code != ENCRYPTION_ENABLED:
            raise Exception(enc_res.data)
        instance._salt = enc_res.data.split(" ", 1)[0]
        md5 = hashlib.md5(bytes(api_key + instance._salt, "ascii"))
        instance._crypto = encryptors.Aes128TextEncryptor(md5.digest())

        instance._login(username, password)
        return instance

    @classmethod
    def create_from_session(cls, session_key, sock_addr, api_key, salt):
        """Creates instance from an existing session. If salt is not None, encrypted instance is created."""
        instance = cls(sock_addr)
        instance.session = session_key
        if (salt != None):
            instance._salt = salt
            md5 = hashlib.md5(bytes(api_key + instance._salt, "ascii"))
            instance._crypto = encryptors.Aes128TextEncryptor(md5.digest())
        return instance


    def _login(self, username, password):
        response = self._send_request_raw(API_ENDPOINT_LOGIN % (username, password))
        if response.code == LOGIN_ACCEPTED or response.code == LOGIN_ACCEPTED_NEW_VERSION_AVAILABLE:
            self.session = response.data.split(" ")[0]
        else:
            raise Exception(response.data)

    def close(self, persistent, persist_file):
        """Logs out the user from current session and closes the connection."""
        if not self.session:
            raise Exception("Cannot logout: No active session.")
        if persistent:
            try:
                os.makedirs(os.path.dirname(persist_file))
            except: pass # Exists
            d = dict()
            d["session_key"] = self.session
            d["timestamp"] = time.time()
            d["salt"] = None
            d["sockaddr"] = self.socket.getsockname()
            if (self_.salt):
                d["salt"] = self_.salt
            with open(persist_file, "w") as file:
                file.writelines(json.dumps(d))
        else:
            try:
                os.remove(persist_file)
            except:
                pass
            self.send_request(API_ENDPOINT_LOGOUT % self.session, False)
        self.socket.close()


    def send_request(self, content, appendSession=True):
        """Sends request to the API and returns a dictionary containing response code and data."""

        original_content = content
        if isinstance(original_content, AnidbApiCall):
            content = original_content.serialize()

        if appendSession:
            if not self.session:
                raise Exception("No session was set")
            content += "&s=%s" % self.session

        return self._send_request_raw(content, tries=RETRY_COUNT)
