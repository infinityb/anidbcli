from abc import ABC, abstractmethod
from Crypto.Cipher import AES

class TextCrypto:
    @abstractmethod
    def Encrypt(self, message): pass
    @abstractmethod
    def Decrypt(self, message): pass

class PlainTextCrypto(TextCrypto):
    def Encrypt(self, message):
        return bytes(message, "utf-8")

    def Decrypt(self, message):
        return message.decode("utf-8", errors="replace")


BS = 16
pad = lambda s: s + (BS - len(s) % BS) * bytes([BS - len(s) % BS])
unpad = lambda s : s[0:-s[-1]]

class Aes128TextEncryptor(TextCrypto):
    def __init__(self, encryption_key):
        self.aes = AES.new(encryption_key, AES.MODE_ECB)

    def Encrypt(self, message):
        message = bytes(message, "utf-8")
        # print("encrypt::message={}:{!r}".format(len(message), message))
        message = pad(message)
        try:
            ret = self.aes.encrypt(message)
            # print("--> ret={}:{!r}".format(len(ret), ret))
            return ret
        except RuntimeError as e:
            raise

    def Decrypt(self, message):
        if message.startswith(b'598 '):
            raise RuntimeError("invalid session or encryption handshake skipped")
        try:
            # print("decrypt::message={}:{!r}".format(len(message), message))
            ret = unpad(self.aes.decrypt(message))
            # print("--> ret={}:{!r}".format(len(ret), ret))
        except RuntimeError as e:
            raise
        return ret.decode("utf-8", errors="ignore")
