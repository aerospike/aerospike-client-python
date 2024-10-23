import threading
from concurrent.futures import ThreadPoolExecutor

import aerospike

def test_unsafe():
    n_threads = 10
    barrier = threading.Barrier(n_threads)
