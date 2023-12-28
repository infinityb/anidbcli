import hashlib
import functools
import os
import multiprocessing
import binascii
import ctypes
import time

CHUNK_SIZE = 9728000 # 9500KB
MAX_CORES = 2  # fastest, experimentally chosen.

def get_ed2k_link(file_path, file_hash=None):       
    name = os.path.basename(file_path)
    filesize = os.path.getsize(file_path)
    if file_hash is None:
        md4 = hash_file(file_path)
    else:
        md4 = file_hash
    return "ed2k://|file|%s|%d|%s|" % (name,filesize, md4)

def md4_hash(data):
    m = hashlib.new('md4')
    m.update(data)
    return m.digest()


def hash_file(file_path, parallel=None):
    """ Returns the ed2k hash of a given file. """
    from joblib import Parallel, parallel_backend, delayed

    def generator(f):
        while True:
            buf = f.read(CHUNK_SIZE)
            if not buf:
                break
            yield buf
    with open(file_path, 'rb') as f:
        f.seek(0, os.SEEK_END)
        size = f.tell()
        f.seek(0, os.SEEK_SET)
        cpu_count = multiprocessing.cpu_count()
        cpu_count = 1
        if cpu_count == 1 or size < (4 * CHUNK_SIZE):  # a guess, threads have spin-up cost.
            hashes = [md4_hash(i) for i in generator(f)]
        else:
            # use threading, the loky backend is the same speed as sequential due
            # to the serialization time.  md4 functions shouldn't hold the GIL?
            if parallel is None:
                parallel = Parallel(prefer="threads", n_jobs=min(cpu_count, MAX_CORES))
            hashes = parallel(delayed(md4_hash)(i) for i in generator(f))
        if len(hashes) == 1:
            return hashes[0].hex()
        else:
            return md4_hash(b"".join(hashes)).hex()


class PoolResult(ctypes.Structure):
    _fields_ = [
        ("submission_id", ctypes.c_uint64),
        ("result_code", ctypes.c_int32),
        ("system_errno", ctypes.c_int32),
        ("_ok_res", ctypes.c_byte * 16),
    ]

    def _repr_fields(self):
        if self.submission_id is not None:
            yield ('submission_id', self.submission_id)
        if self.result_code is not None:
            yield ('result_code', self.result_code)
        if self.system_errno is not None:
            yield ('system_errno', self.system_errno)
        if self.ok_res is not None:
            yield ('ok_res', self.ok_res)

    def __repr__(self):
        keys = ', '.join("{}={!r}".format(n, v) for (n, v) in self._repr_fields())
        return "{0.__class__.__module__}.{0.__class__.__name__}({1})".format(self, keys)
    
    @property
    def ok_res(self):
        return bytearray(self._ok_res)


def get_libed2k_handle():
    libed2k = ctypes.cdll.LoadLibrary("/home/sell/compile/anidbcli/ed2k/target/release/libed2k.so")

    libed2k.ed2k_pool_init.restype = ctypes.c_void_p
    libed2k.ed2k_pool_init.argtypes = [ctypes.c_size_t]

    libed2k.ed2k_pool_destroy.restype = None
    libed2k.ed2k_pool_destroy.argtypes = [ctypes.POINTER(PoolResult)]

    libed2k.ed2k_pool_result_destroy.restype = None
    libed2k.ed2k_pool_result_destroy.argtypes = [ctypes.POINTER(PoolResult)]

    libed2k.ed2k_pool_result_read.restype = ctypes.c_int32
    libed2k.ed2k_pool_result_read.argtypes = [
        ctypes.c_void_p,  # PoolStructure
        ctypes.POINTER(PoolResult),
    ]

    libed2k.ed2k_pool_queue_file.restype = None
    libed2k.ed2k_pool_queue_file.argtypes = [
        ctypes.c_void_p,
        ctypes.c_uint64,
        ctypes.c_char_p,
    ]

    return libed2k


class ED2KPool(object):
    try:
        _libed2k = get_libed2k_handle()
    except OSError:
        _libed2k = None

    def __init__(self, threads=None):
        self._threadpool = self._libed2k.ed2k_pool_init(multiprocessing.cpu_count())
    
    def _check_threadpool(self):
        if self._threadpool is None:
            raise Exception("threadpool was closed")

    def queue(self, file_path, txid):
        self._check_threadpool()
        path = ctypes.create_string_buffer(file_path.encode('utf8'))
        self._libed2k.ed2k_pool_queue_file(self._threadpool, txid, path)
    
    def poll(self):
        inst = PoolResult()
        v = self._libed2k.ed2k_pool_result_read(
            self._threadpool,
            ctypes.pointer(inst),
        )
        if v == 0:
            return inst
        if v == -1:  # no outstanding work
            return None
        if v == -2:  # disconnected
            return None

    def close(self):
        if self._threadpool is not None:
            self._libed2k.ed2k_pool_destroy(self._threadpool)
            self._threadpool = None
    
    def __del__(self):
        self.close()


# p = ED2KPool(12)
# txid = 0
# for root, dirs, files in os.walk('/storage/datasets/horriblesubs'):
#     for filename in files:
#         p.queue(os.path.join(root, filename), txid)
#         txid += 1
#         # print("{!r}".format(os.path.join(root, filename)))
# for _ in range(txid):
#     print("{!r}".format(p.poll()))

