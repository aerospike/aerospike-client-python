# -*- coding: utf-8 -*-
import pytest

from aerospike import exception as e
from aerospike_helpers.batch import records as br
from aerospike_helpers.operations import operations

# The C client holds the number of operations in a command in 16 bits.
MAX_OPS = 2**16 - 1
KEY = ("test", "demo", "operations_count")


def read_ops(count):
    return [operations.read(f"b{i}") for i in range(count)]


@pytest.mark.usefixtures("as_connection")
class TestOperationsCount:
    @pytest.mark.parametrize("count", [MAX_OPS + 1, MAX_OPS + 2])
    @pytest.mark.parametrize(
        "command",
        ["operate", "operate_ordered", "batch_operate", "batch_write", "query_add_ops", "scan_add_ops"],
    )
    def test_too_many_operations_raises_param_error(self, command, count):
        ops = read_ops(count)
        with pytest.raises(e.ParamError) as excinfo:
            if command == "operate":
                self.as_connection.operate(KEY, ops)
            elif command == "operate_ordered":
                self.as_connection.operate_ordered(KEY, ops)
            elif command == "batch_operate":
                self.as_connection.batch_operate([KEY], ops)
            elif command == "batch_write":
                self.as_connection.batch_write(br.BatchRecords([br.Read(KEY, ops)]))
            elif command == "query_add_ops":
                self.as_connection.query("test", "demo").add_ops(ops)
            else:
                self.as_connection.scan("test", "demo").add_ops(ops)
        assert excinfo.value.msg == f"Too many operations: {count}; the maximum is {MAX_OPS}"

    @pytest.mark.parametrize("command", ["query", "scan"])
    def test_add_ops_accepts_the_maximum(self, command):
        getattr(self.as_connection, command)("test", "demo").add_ops(read_ops(MAX_OPS))
