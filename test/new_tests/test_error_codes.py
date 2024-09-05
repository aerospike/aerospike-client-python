import pytest
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
        # in_doubt is only added when an AerospikeError or subclass of it is raised by the client
        # This attribute is not set when initializing the exception classes in `aerospike.exception`
        # "in_doubt"
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
    "expected_exc_class, expected_exc_name, expected_error_code, expected_exc_base_class",
    (
        # Don't test error_name and error_code fields that are set to None
        # The exception name should be the class name anyways...
        # The API docs just specify the exception class and its base class, so we only test those things
        (e.AerospikeError, None, None, Exception),
        (e.ClientError, None, None, e.AerospikeError),
        # Client errors
        (e.InvalidHostError, None, None, e.ClientError),
        (e.ParamError, None, None, e.ClientError),
        (e.MaxErrorRateExceeded, None, None, e.ClientError),
        (e.MaxRetriesExceeded, None, None, e.ClientError),
        (e.NoResponse, None, None, e.ClientError),
        (e.BatchFailed, None, None, e.ClientError),
        (e.ConnectionError, None, None, e.ClientError),
        (e.TLSError, "TLSError", AEROSPIKE_ERR_TLS_ERROR, e.ClientError),
        (e.InvalidNodeError, "InvalidNodeError", AEROSPIKE_ERR_INVALID_NODE, e.ClientError),
        (e.NoMoreConnectionsError, "NoMoreConnectionsError", AEROSPIKE_ERR_NO_MORE_CONNECTIONS, e.ClientError),
        (e.AsyncConnectionError, "AsyncConnectionError", AEROSPIKE_ERR_ASYNC_CONNECTION, e.ClientError),
        (e.ClientAbortError, "ClientAbortError", AEROSPIKE_ERR_CLIENT_ABORT, e.ClientError),
        # Server errors
        (e.ServerError, None, None, e.AerospikeError),
        (e.InvalidRequest, None, None, e.ServerError),
        (e.OpNotApplicable, None, None, e.ServerError),
        (e.FilteredOut, None, None, e.ServerError),
        (e.ServerFull, None, None, e.ServerError),
        (e.AlwaysForbidden, None, None, e.ServerError),
        (e.UnsupportedFeature, None, None, e.ServerError),
        (e.DeviceOverload, None, None, e.ServerError),
        (e.NamespaceNotFound, None, None, e.ServerError),
        (e.ForbiddenError, None, None, e.ServerError),
        (e.LostConflict, None, None, e.ServerError),
        (e.InvalidGeoJSON, None, None, e.ServerError),
        (e.ScanAbortedError, "ScanAbortedError", AEROSPIKE_ERR_SCAN_ABORTED, e.ServerError),
        (e.ElementNotFoundError, "ElementNotFoundError", AEROSPIKE_ERR_FAIL_ELEMENT_NOT_FOUND, e.ServerError),
        (e.ElementExistsError, "ElementExistsError", AEROSPIKE_ERR_FAIL_ELEMENT_EXISTS, e.ServerError),
        (e.BatchDisabledError, "BatchDisabledError", AEROSPIKE_ERR_BATCH_DISABLED, e.ServerError),
        (e.BatchMaxRequestError, "BatchMaxRequestError", AEROSPIKE_ERR_BATCH_MAX_REQUESTS_EXCEEDED, e.ServerError),
        (e.BatchQueueFullError, "BatchQueueFullError", AEROSPIKE_ERR_BATCH_QUEUES_FULL, e.ServerError),
        (e.QueryAbortedError, "QueryAbortedError", AEROSPIKE_ERR_QUERY_ABORTED, e.ServerError),
        # Record errors
        (e.RecordError, None, None, e.ServerError),
        (e.RecordKeyMismatch, None, None, e.RecordError),
        (e.RecordNotFound, None, None, e.RecordError),
        (e.RecordGenerationError, None, None, e.RecordError),
        (e.RecordExistsError, None, None, e.RecordError),
        (e.RecordBusy, None, None, e.RecordError),
        (e.RecordTooBig, None, None, e.RecordError),
        (e.BinNameError, None, None, e.RecordError),
        (e.BinIncompatibleType, None, None, e.RecordError),
        (e.BinNotFound, None, None, e.RecordError),
        (e.BinExistsError, None, None, e.RecordError),
        # Index errors
        (e.IndexError, None, None, e.ServerError),
        (e.IndexNotFound, None, None, e.IndexError),
        (e.IndexFoundError, None, None, e.IndexError),
        (e.IndexOOM, None, None, e.IndexError),
        (e.IndexNotReadable, None, None, e.IndexError),
        (e.IndexNameMaxLen, None, None, e.IndexError),
        (e.IndexNameMaxCount, None, None, e.IndexError),
        # Query errors
        (e.QueryError, None, None, e.ServerError),
        (e.QueryQueueFull, None, None, e.QueryError),
        (e.QueryTimeout, None, None, e.QueryError),
        # Cluster errors
        (e.ClusterError, None, None, e.ServerError),
        (e.ClusterChangeError, None, None, e.ClusterError),
        # Admin errors
        (e.AdminError, None, None, e.ServerError),
        (e.ExpiredPassword, None, None, e.AdminError),
        (e.ForbiddenPassword, None, None, e.AdminError),
        (e.IllegalState, None, None, e.AdminError),
        (e.InvalidCommand, None, None, e.AdminError),
        (e.InvalidCredential, None, None, e.AdminError),
        (e.InvalidField, None, None, e.AdminError),
        (e.InvalidPassword, None, None, e.AdminError),
        (e.InvalidPrivilege, None, None, e.AdminError),
        (e.InvalidRole, None, None, e.AdminError),
        (e.InvalidUser, None, None, e.AdminError),
        (e.QuotasNotEnabled, None, None, e.AdminError),
        (e.QuotaExceeded, None, None, e.AdminError),
        (e.InvalidQuota, None, None, e.AdminError),
        (e.NotWhitelisted, None, None, e.AdminError),
        (e.InvalidWhitelist, None, None, e.AdminError),
        (e.NotAuthenticated, None, None, e.AdminError),
        (e.RoleExistsError, None, None, e.AdminError),
        (e.RoleViolation, None, None, e.AdminError),
        (e.SecurityNotEnabled, None, None, e.AdminError),
        (e.SecurityNotSupported, None, None, e.AdminError),
        (e.SecuritySchemeNotSupported, None, None, e.AdminError),
        (e.UserExistsError, None, None, e.AdminError),
        # UDF errors
        (e.UDFError, None, None, e.ServerError),
        (e.UDFNotFound, None, None, e.UDFError),
        (e.LuaFileNotFound, None, None, e.UDFError),
    ),
)
def test_error_codes(
    expected_exc_class: type,
    expected_exc_name: Optional[str],
    expected_error_code: Optional[int],
    expected_exc_base_class: type
):
    with pytest.raises(expected_exc_class) as excinfo:
        raise expected_exc_class

    if expected_error_code is not None:
        assert excinfo.value.code == expected_error_code

    if expected_exc_name is not None:
        assert excinfo.type.__name__ == expected_exc_name

    # Test directly inherited class
    assert expected_exc_base_class in excinfo.type.__bases__

    for base_class in base_class_to_attrs:
        if issubclass(excinfo.type, base_class):
            for attr in base_class_to_attrs[base_class]:
                assert hasattr(excinfo.value, attr)
