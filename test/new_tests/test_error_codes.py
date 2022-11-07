import pytest
from aerospike import exception as e
from .as_errors import *


@pytest.mark.parametrize(
    "error, error_name, error_code, base",
    (
        (e.TLSError, "TLSError", AEROSPIKE_ERR_TLS_ERROR, e.ClientError),
        (e.InvalidNodeError, "InvalidNodeError", AEROSPIKE_ERR_INVALID_NODE,
         e.ClientError),
        (e.NoMoreConnectionsError, "NoMoreConnectionsError",
         AEROSPIKE_ERR_NO_MORE_CONNECTIONS, e.ClientError),
        (e.AsyncConnectionError, "AsyncConnectionError",
         AEROSPIKE_ERR_ASYNC_CONNECTION, e.ClientError),
        (e.ClientAbortError, "ClientAbortError", AEROSPIKE_ERR_CLIENT_ABORT,
         e.ClientError),
        (e.ScanAbortedError, "ScanAbortedError", AEROSPIKE_ERR_SCAN_ABORTED,
         e.ServerError),
        (e.ElementNotFoundError, "ElementNotFoundError",
         AEROSPIKE_ERR_FAIL_ELEMENT_NOT_FOUND, e.ServerError),
        (e.ElementExistsError, "ElementExistsError",
         AEROSPIKE_ERR_FAIL_ELEMENT_EXISTS, e.ServerError),
        (e.BatchDisabledError, "BatchDisabledError",
         AEROSPIKE_ERR_BATCH_DISABLED, e.ServerError),
        (e.BatchMaxRequestError, "BatchMaxRequestError",
         AEROSPIKE_ERR_BATCH_MAX_REQUESTS_EXCEEDED, e.ServerError),
        (e.BatchQueueFullError, "BatchQueueFullError",
         AEROSPIKE_ERR_BATCH_QUEUES_FULL, e.ServerError),
        (e.QueryAbortedError, "QueryAbortedError", AEROSPIKE_ERR_QUERY_ABORTED,
         e.ServerError)
    )
)
def test_error_codes(error, error_name, error_code, base):
    with pytest.raises(error) as test_error:
        raise error

    test_error = test_error.value

    assert test_error.code == error_code
    assert type(test_error).__name__ == error_name
    assert issubclass(type(test_error), base)
