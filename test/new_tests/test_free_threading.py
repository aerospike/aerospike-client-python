import threading
import pytest
import aerospike


@pytest.mark.usefixtures("as_connection")
class TestFreeThreading:
    def test_unsafe(self):
        key = ("test", "demo", 1)
        BIN_VALUE = 1
        self.as_connection.put(key, bins={"a": BIN_VALUE})

        bin_value_sum = 0

        THREAD_COUNT = 10
        barrier = threading.Barrier(parties=THREAD_COUNT)
        config = {"hosts": [("127.0.0.1", 3000)]}

        def read_bin():
            nonlocal barrier, config
            barrier.wait()
            client = aerospike.client(config)
            _, _, bins = client.get(key)
            nonlocal bin_value_sum
            bin_value_sum += bins["a"]

        workers = []
        for _ in range(THREAD_COUNT):
            workers.append(threading.Thread(target=read_bin))

        for worker in workers:
            worker.start()

        for worker in workers:
            worker.join()

        assert bin_value_sum == THREAD_COUNT * BIN_VALUE
