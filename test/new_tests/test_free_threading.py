import threading
import pytest

import aerospike
from aerospike_helpers.operations import operations


@pytest.mark.usefixtures("as_connection")
class TestFreeThreading:
    def test_unsafe(self):
        key = ("test", "demo", 1)
        INIT_BIN_VALUE = 0
        BIN_NAME = "a"
        BIN_VALUE_AMOUNT_TO_ADD_IN_EACH_THREAD = 1
        self.as_connection.put(key, bins={BIN_NAME: INIT_BIN_VALUE})

        THREAD_COUNT = 10
        barrier = threading.Barrier(parties=THREAD_COUNT)
        config = {"hosts": [("127.0.0.1", 3000)]}

        ops = [
            operations.increment(bin_name=BIN_NAME, amount=BIN_VALUE_AMOUNT_TO_ADD_IN_EACH_THREAD)
        ]

        def increment_bin():
            nonlocal barrier
            barrier.wait()
            nonlocal config
            client = aerospike.client(config)
            nonlocal key, ops
            client.operate(key, ops)

        workers = []
        for _ in range(THREAD_COUNT):
            workers.append(threading.Thread(target=increment_bin))

        for worker in workers:
            worker.start()

        for worker in workers:
            worker.join()

        _, _, bins = self.as_connection.get(key)
        assert bins[BIN_NAME] == INIT_BIN_VALUE + BIN_VALUE_AMOUNT_TO_ADD_IN_EACH_THREAD * THREAD_COUNT
