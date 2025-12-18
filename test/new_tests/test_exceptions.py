import pytest
# Explicitly test that we can import submodule this way
import aerospike.exception  # noqa: F401
from aerospike import exception as e
from .as_errors import (
    AEROSPIKE_ERR_ASYNC_CONNECTION,
    AEROSPIKE_ERR_BATCH_DISABLED,
    AEROSPIKE_ERR_BATCH_MAX_REQUESTS_EXCEEDED,
    AEROSPIKE_ERR_BATCH_QUEUES_FULL,
    AEROSPIKE_ERR_CLIENT_ABORT,
    AEROSPIKE_ERR_FAIL_ELEMENT_EXISTS,
    AEROSPIKE_ERR_FAIL_ELEMENT_NOT_FOUND,
    AEROSPIKE_ERR_INVALID_NODE,
    AEROSPIKE_ERR_NO_MORE_CONNECTIONS,
    AEROSPIKE_ERR_QUERY_ABORTED,
    AEROSPIKE_ERR_SCAN_ABORTED,
    AEROSPIKE_ERR_TLS_ERROR,
)
from typing import Optional

# Used to test inherited attributes (can be from indirect parent)
base_class_to_attrs = {
    e.AerospikeError: [
        "code",
        "msg",
        "file",
        "line",
        "in_doubt"
    ],
    e.RecordError: [
        "key",
        "bin"
    ],
    e.IndexError: [
        "name"
    ],
    e.UDFError: [
        "module",
        "func"
    ]
}


# TODO: add missing type stubs
# TODO: make sure other places in the tests aren't doing the same thing as here.
# We'll do this by cleaning up the test code. But it's nice to test the API in one place
# TODO: add documentation for the tests in a README
@pytest.mark.parametrize(
    "exc_class, expected_exc_name, expected_error_code, expected_exc_base_class",
    (
        # Don't test error_name fields that are set to None
        # The exception name should be the class name anyways...
        (e.AerospikeError, None, None, Exception),
        (e.ClientError, None, -1, e.AerospikeError),
        # Client errors
        (e.InvalidHostError, None, -4, e.ClientError),
        (e.ParamError, None, -2, e.ClientError),
        (e.MaxErrorRateExceeded, None, -14, e.ClientError),
        (e.MaxRetriesExceeded, None, -12, e.ClientError),
        (e.NoResponse, None, -15, e.ClientError),
        (e.BatchFailed, None, -16, e.ClientError),
        (e.ConnectionError, None, -10, e.ClientError),
        (e.TLSError, "TLSError", AEROSPIKE_ERR_TLS_ERROR, e.ClientError),
        (e.InvalidNodeError, "InvalidNodeError", AEROSPIKE_ERR_INVALID_NODE, e.ClientError),
        (e.NoMoreConnectionsError, "NoMoreConnectionsError", AEROSPIKE_ERR_NO_MORE_CONNECTIONS, e.ClientError),
        (e.AsyncConnectionError, "AsyncConnectionError", AEROSPIKE_ERR_ASYNC_CONNECTION, e.ClientError),
        (e.ClientAbortError, "ClientAbortError", AEROSPIKE_ERR_CLIENT_ABORT, e.ClientError),
        # Server errors
        (e.ServerError, None, 1, e.AerospikeError),
        (e.InvalidRequest, None, 4, e.ServerError),
        (e.OpNotApplicable, None, 26, e.ServerError),
        (e.FilteredOut, None, 27, e.ServerError),
        (e.ServerFull, None, 8, e.ServerError),
        (e.AlwaysForbidden, None, 10, e.ServerError),
        (e.UnsupportedFeature, None, 16, e.ServerError),
        (e.DeviceOverload, None, 18, e.ServerError),
        (e.NamespaceNotFound, None, 20, e.ServerError),
        (e.ForbiddenError, None, 22, e.ServerError),
        (e.LostConflict, None, 28, e.ServerError),
        (e.InvalidGeoJSON, None, 160, e.ServerError),
        (e.ScanAbortedError, "ScanAbortedError", AEROSPIKE_ERR_SCAN_ABORTED, e.ServerError),
        (e.ElementNotFoundError, "ElementNotFoundError", AEROSPIKE_ERR_FAIL_ELEMENT_NOT_FOUND, e.ServerError),
        (e.ElementExistsError, "ElementExistsError", AEROSPIKE_ERR_FAIL_ELEMENT_EXISTS, e.ServerError),
        (e.BatchDisabledError, "BatchDisabledError", AEROSPIKE_ERR_BATCH_DISABLED, e.ServerError),
        (e.BatchMaxRequestError, "BatchMaxRequestError", AEROSPIKE_ERR_BATCH_MAX_REQUESTS_EXCEEDED, e.ServerError),
        (e.BatchQueueFullError, "BatchQueueFullError", AEROSPIKE_ERR_BATCH_QUEUES_FULL, e.ServerError),
        (e.QueryAbortedError, "QueryAbortedError", AEROSPIKE_ERR_QUERY_ABORTED, e.ServerError),
        # Record errors
        (e.RecordError, None, None, e.ServerError),
        (e.RecordKeyMismatch, None, 19, e.RecordError),
        (e.RecordNotFound, None, 2, e.RecordError),
        (e.RecordGenerationError, None, 3, e.RecordError),
        (e.RecordExistsError, None, 5, e.RecordError),
        (e.RecordBusy, None, 14, e.RecordError),
        (e.RecordTooBig, None, 13, e.RecordError),
        (e.BinNameError, None, 21, e.RecordError),
        (e.BinIncompatibleType, None, 12, e.RecordError),
        (e.BinNotFound, None, 17, e.RecordError),
        (e.BinExistsError, None, 6, e.RecordError),
        # Index errors
        (e.IndexError, None, 204, e.ServerError),
        (e.IndexNotFound, None, 201, e.IndexError),
        (e.IndexFoundError, None, 200, e.IndexError),
        (e.IndexOOM, None, 202, e.IndexError),
        (e.IndexNotReadable, None, 203, e.IndexError),
        (e.IndexNameMaxLen, None, 205, e.IndexError),
        (e.IndexNameMaxCount, None, 206, e.IndexError),
        # Query errors
        (e.QueryError, None, 213, e.ServerError),
        (e.QueryQueueFull, None, 211, e.QueryError),
        (e.QueryTimeout, None, 212, e.QueryError),
        # Cluster errors
        (e.ClusterError, None, 11, e.ServerError),
        (e.ClusterChangeError, None, 7, e.ClusterError),
        # Admin errors
        (e.AdminError, None, None, e.ServerError),
        (e.ExpiredPassword, None, 63, e.AdminError),
        (e.ForbiddenPassword, None, 64, e.AdminError),
        (e.IllegalState, None, 56, e.AdminError),
        (e.InvalidCommand, None, 54, e.AdminError),
        (e.InvalidCredential, None, 65, e.AdminError),
        (e.InvalidField, None, 55, e.AdminError),
        (e.InvalidPassword, None, 62, e.AdminError),
        (e.InvalidPrivilege, None, 72, e.AdminError),
        (e.InvalidRole, None, 70, e.AdminError),
        (e.InvalidUser, None, 60, e.AdminError),
        (e.QuotasNotEnabled, None, 74, e.AdminError),
        (e.QuotaExceeded, None, 83, e.AdminError),
        (e.InvalidQuota, None, 75, e.AdminError),
        (e.NotWhitelisted, None, 82, e.AdminError),
        (e.InvalidWhitelist, None, 73, e.AdminError),
        (e.NotAuthenticated, None, 80, e.AdminError),
        (e.RoleExistsError, None, 71, e.AdminError),
        (e.RoleViolation, None, 81, e.AdminError),
        (e.SecurityNotEnabled, None, 52, e.AdminError),
        (e.SecurityNotSupported, None, 51, e.AdminError),
        (e.SecuritySchemeNotSupported, None, 53, e.AdminError),
        (e.UserExistsError, None, 61, e.AdminError),
        # UDF errors
        (e.UDFError, None, 100, e.ServerError),
        (e.UDFNotFound, None, 1301, e.UDFError),
        (e.LuaFileNotFound, None, 1302, e.UDFError),
    ),
)
def test_aerospike_exceptions(
    exc_class: type,
    expected_exc_name: Optional[str],
    expected_error_code: Optional[int],
    expected_exc_base_class: type
):
    with pytest.raises(exc_class) as excinfo:
        raise exc_class

    assert excinfo.value.code == expected_error_code

    if expected_exc_name is not None:
        assert excinfo.type.__name__ == expected_exc_name

    # Test directly inherited class
    assert expected_exc_base_class in excinfo.type.__bases__

    for base_class in base_class_to_attrs:
        if issubclass(excinfo.type, base_class):
            for attr in base_class_to_attrs[base_class]:
                assert hasattr(excinfo.value, attr)
