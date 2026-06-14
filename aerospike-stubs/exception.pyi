"""Exceptions raised by the Aerospike Python client (``aerospike.exception``).

Catch ``AerospikeError`` (or subclasses) for client and server failures. Each
exception exposes ``code``, ``msg``, ``file``, ``line``, and ``in_doubt`` where
applicable; see the client documentation for status codes and the in-doubt flag.

Full reference: https://aerospike-python-client.readthedocs.io/en/latest/exception.html
"""

from typing import Union

class AerospikeError(Exception):
    """Parent class of all exceptions raised by the Aerospike client.

    Attributes:
        code: Associated status code.
        msg: Human-readable error message.
        file: Source file where the exception was raised (when available).
        line: Source line number (when available).
        in_doubt: True if the command may have succeeded on the server (see client docs).
    """

    code: Union[int, None]
    msg: Union[str, None]
    file: Union[str, None]
    line: Union[int, None]
    in_doubt: Union[bool, None]

class TimeoutError(AerospikeError):
    """Raised when an operation exceeds its timeout."""

class ClientError(AerospikeError):
    """Client-side errors, often due to misconfiguration or invalid API use (code ``-1``)."""

class InvalidHostError(ClientError):
    """Host name could not be resolved (code ``-4``)."""

class ParamError(ClientError):
    """Invalid parameters were supplied (code ``-2``)."""

class TransactionFailed(ClientError):
    """Transaction failed (code ``-17``)."""

class TransactionAlreadyAborted(ClientError):
    """Abort called but the transaction was already aborted (code ``-19``)."""

class TransactionAlreadyCommitted(ClientError):
    """Commit called but the transaction was already committed (code ``-18``)."""

class ServerError(AerospikeError):
    """Parent class for errors returned from the cluster (positive server codes)."""

class InvalidRequest(ServerError):
    """Invalid request or protocol field (code ``4``)."""

class OpNotApplicable(ServerError):
    """The operation cannot be applied to the current bin value (code ``26``)."""

class FilteredOut(ServerError):
    """The command was not performed because an expression evaluated false (code ``27``)."""

class ServerFull(ServerError):
    """The node is low on memory or storage reserved for the namespace (code ``8``)."""

class AlwaysForbidden(ServerError):
    """Operation not allowed in the current configuration (code ``10``)."""

class UnsupportedFeature(ServerError):
    """Unimplemented server feature (code ``16``)."""

class DeviceOverload(ServerError):
    """Storage devices cannot keep up with the write load (code ``18``)."""

class NamespaceNotFound(ServerError):
    """Namespace not found on the server (code ``20``)."""

class ForbiddenError(ServerError):
    """Operation not allowed at this time (code ``22``)."""

class ElementExistsError(ServerError):
    """Map key already exists under a create-only policy (code ``23``)."""

class ElementNotFoundError(ServerError):
    """Map key missing under an update-only policy (code ``24``)."""

class RecordError(ServerError):
    """Base class for record/bin errors during read or write operations."""

    key: Union[tuple, None]
    bin: Union[str, None]

class RecordKeyMismatch(RecordError):
    """Key sent with the command did not match the key stored on the server (code ``19``)."""

class RecordNotFound(RecordError):
    """Record does not exist (code ``2``)."""

class RecordGenerationError(RecordError):
    """Record generation does not satisfy the write policy (code ``3``)."""

class RecordExistsError(RecordError):
    """Record already exists for a create-only write (code ``5``)."""

class RecordBusy(RecordError):
    """Too many concurrent operations on one record (code ``14``)."""

class RecordTooBig(RecordError):
    """Record cannot fit in a storage write block (code ``13``)."""

class BinNameError(RecordError):
    """Invalid bin name length or bin name quota exceeded (code ``21``)."""

class BinIncompatibleType(RecordError):
    """Bin operation incompatible with the existing bin type (code ``12``)."""

class IndexError(ServerError):
    """Base class for secondary index errors (code ``204`` on the parent)."""

    name: Union[str, None]

class IndexNotFound(IndexError):
    """Index not found (code ``201``)."""

class IndexFoundError(IndexError):
    """Index already exists (code ``200``)."""

class IndexOOM(IndexError):
    """Index is out of memory (code ``202``)."""

class IndexNotReadable(IndexError):
    """Index is not readable (code ``203``)."""

class IndexNameMaxLen(IndexError):
    """Index name is too long (code ``205``)."""

class IndexNameMaxCount(IndexError):
    """Maximum number of indexes reached (code ``206``)."""

class QueryError(AerospikeError):
    """Query-related errors (server code ``213`` on the generic case)."""

class QueryQueueFull(QueryError):
    """Query processing queue is full (code ``211``)."""

class QueryTimeout(QueryError):
    """Secondary index query timed out on the server (code ``212``)."""

class ClusterError(AerospikeError):
    """Cluster discovery and connection errors (code ``11`` on the generic case)."""

class ClusterChangeError(ClusterError):
    """Cluster state changed during the request (code ``7``)."""

class AdminError(ServerError):
    """Base class for security / administration API errors."""

class ExpiredPassword(AdminError):
    """Password has expired (code ``63``)."""

class ForbiddenPassword(AdminError):
    """Password is not allowed (code ``64``)."""

class IllegalState(AdminError):
    """Security protocol not followed (code ``56``)."""

class InvalidCommand(AdminError):
    """Invalid administration command (code ``54``)."""

class InvalidCredential(AdminError):
    """Invalid security credential (code ``65``)."""

class InvalidPassword(AdminError):
    """Invalid password (code ``62``)."""

class InvalidPrivilege(AdminError):
    """Invalid privilege (code ``72``)."""

class InvalidRole(AdminError):
    """Invalid role name (code ``70``)."""

class InvalidUser(AdminError):
    """Invalid user name (code ``60``)."""

class NotAuthenticated(AdminError):
    """User must authenticate before database operations (code ``80``)."""

class RoleExistsError(AdminError):
    """Role already exists (code ``71``)."""

class RoleViolation(AdminError):
    """User lacks the required role (code ``81``)."""

class SecurityNotEnabled(AdminError):
    """Security is not enabled on the server (code ``52``)."""

class SecurityNotSupported(AdminError):
    """Security is not supported by the connected server (code ``51``)."""

class SecuritySchemeNotSupported(AdminError):
    """Security scheme is not supported (code ``53``)."""

class UserExistsError(AdminError):
    """User already exists (code ``61``)."""

class UDFError(ServerError):
    """Base class for UDF-related errors."""

    module: Union[str, None]
    func: Union[str, None]

class UDFNotFound(UDFError):
    """UDF module does not exist (code ``1301``)."""

class LuaFileNotFound(UDFError):
    """Lua source file not found (code ``1302``)."""
