from typing import Union

class AerospikeError(Exception):
    # When attributes are first assigned to exception class, they have an initial value of None
    code: Union[int, None]
    msg: Union[str, None]
    file: Union[str, None]
    line: Union[int, None]
    in_doubt: Union[bool, None]

class ClientError(AerospikeError):
    pass

class InvalidHostError(ClientError):
    pass

class ParamError(ClientError):
    pass

class TransactionFailed(ClientError):
    pass

class TransactionAlreadyAborted(ClientError):
    pass

class TransactionAlreadyCommitted(ClientError):
    pass

class ServerError(AerospikeError):
    pass

class InvalidRequest(ServerError):
    pass

class OpNotApplicable(ServerError):
    pass

class FilteredOut(ServerError):
    pass

class ServerFull(ServerError):
    pass

class AlwaysForbidden(ServerError):
    pass

class UnsupportedFeature(ServerError):
    pass

class DeviceOverload(ServerError):
    pass

class NamespaceNotFound(ServerError):
    pass

class ForbiddenError(ServerError):
    pass

class ElementExistsError(ServerError):
    pass

class ElementNotFoundError(ServerError):
    pass

class RecordError(ServerError):
    key: Union[tuple, None]
    bin: Union[str, None]

class RecordKeyMismatch(RecordError):
    pass

class RecordNotFound(RecordError):
    pass

class RecordGenerationError(RecordError):
    pass

class RecordExistsError(RecordError):
    pass

class RecordBusy(RecordError):
    pass

class RecordTooBig(RecordError):
    pass

class BinNameError(RecordError):
    pass

class BinIncompatibleType(RecordError):
    pass

class IndexError(ServerError):
    name: Union[str, None]

class IndexNotFound(IndexError):
    pass

class IndexFoundError(IndexError):
    pass

class IndexOOM(IndexError):
    pass

class IndexNotReadable(IndexError):
    pass

class IndexNameMaxLen(IndexError):
    pass

class IndexNameMaxCount(IndexError):
    pass

class QueryError(AerospikeError):
    pass

class QueryQueueFull(QueryError):
    pass

class QueryTimeout(QueryError):
    pass

class ClusterError(AerospikeError):
    pass

class ClusterChangeError(ClusterError):
    pass

class AdminError(ServerError):
    pass

class ExpiredPassword(AdminError):
	pass

class ForbiddenPassword(AdminError):
	pass

class IllegalState(AdminError):
	pass

class InvalidCommand(AdminError):
	pass

class InvalidCredential(AdminError):
	pass

class InvalidField(AdminError):
	pass

class InvalidPassword(AdminError):
	pass

class InvalidPrivilege(AdminError):
	pass

class InvalidRole(AdminError):
	pass

class InvalidUser(AdminError):
	pass

class NotAuthenticated(AdminError):
	pass

class RoleExistsError(AdminError):
	pass

class RoleViolation(AdminError):
	pass

class SecurityNotEnabled(AdminError):
	pass

class SecurityNotSupported(AdminError):
	pass

class SecuritySchemeNotSupported(AdminError):
	pass

class UserExistsError(AdminError):
	pass

class UDFError(ServerError):
    module: Union[str, None]
    func: Union[str, None]

class UDFNotFound(UDFError):
    pass

class LuaFileNotFound(UDFError):
    pass
