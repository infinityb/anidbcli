import hashlib
import functools
import os
import multiprocessing
from joblib import Parallel, parallel_backend, delayed

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


def hash_file(file_path):
    """ Returns the ed2k hash of a given file. """
    def generator(f):
        while True:
            buf = f.read(CHUNK_SIZE)
            if not buf:
                break
            yield buf

    with open(file_path, 'rb') as f:
        cpu_count = multiprocessing.cpu_count()
        if cpu_count == 1:
            hashes = [md4_hash(i) for i in generator(f)]
        else:
            # use threading, the loky backend is the same speed as sequential due
            # to the serialization time.  md4 functions shouldn't hold the GIL?
            with parallel_backend('threading', n_jobs=min(cpu_count, MAX_CORES)):
                hashes = Parallel()(delayed(md4_hash)(i) for i in generator(f))
        if len(hashes) == 1:
            return hashes[0].hex()
        else:
            return md4_hash(b"".join(hashes)).hex()
