from typing import Any, Callable, Union, final, Literal, Optional, Final

from aerospike_helpers.batch.records import BatchRecords
from aerospike_helpers.metrics import MetricsPolicy, ClusterStats

AS_BOOL: Literal[1]
"""Represent a boolean as an integer."""
AS_BYTES_BLOB: Literal[4]
"""Represent a blob of bytes."""
AS_BYTES_BOOL: Literal[17]
"""Represent a boolean."""
AS_BYTES_CSHARP: Literal[8]
"""Represent a C# object."""
AS_BYTES_DOUBLE: Literal[2]
"""Represent a double."""
AS_BYTES_ERLANG: Literal[12]
"""Represent an Erlang object."""
AS_BYTES_GEOJSON: Literal[23]
"""An index whose values are of the aerospike GeoJSON data type."""
AS_BYTES_HLL: Literal[18]
"""Represent a HyperLogLog object."""
AS_BYTES_INTEGER: Literal[1]
"""Represent an integer."""
AS_BYTES_JAVA: Literal[7]
"""Represent a Java object."""
AS_BYTES_LIST: Literal[20]
"""Represent a list."""
AS_BYTES_MAP: Literal[19]
"""Represent a map."""
AS_BYTES_PHP: Literal[11]
"""Represent a PHP object."""
AS_BYTES_PYTHON: Literal[9]
"""Represent a Python object."""
AS_BYTES_RUBY: Literal[10]
"""Represent a Ruby object."""
AS_BYTES_STRING: Literal[3]
"""Represent a string."""
AS_BYTES_TYPE_MAX: Literal[24]
AS_BYTES_UNDEF: Literal[0]
AUTH_EXTERNAL: Literal[1]
"""The user is authenticated using an external mechanism, such as LDAP."""
AUTH_EXTERNAL_INSECURE: Literal[2]
"""The user is authenticated using an external mechanism, such as LDAP, but without TLS."""
AUTH_INTERNAL: Literal[0]
"""The user is authenticated using the internal Aerospike mechanism."""
AUTH_PKI: Literal[3]
"""The user is authenticated using PKI."""
BIT_OVERFLOW_FAIL: Literal[0]
"""Fail the operation if an overflow occurs."""
BIT_OVERFLOW_SATURATE: Literal[2]
"""Saturate the value if an overflow occurs."""
BIT_OVERFLOW_WRAP: Literal[4]
"""Wrap the value if an overflow occurs."""
BIT_RESIZE_DEFAULT: Literal[0]
"""Default resize behavior."""
BIT_RESIZE_FROM_FRONT: Literal[1]
"""Resize from the front of the bitset."""
BIT_RESIZE_GROW_ONLY: Literal[2]
"""Only allow the bitset to grow."""
BIT_RESIZE_SHRINK_ONLY: Literal[4]
"""Only allow the bitset to shrink."""
BIT_WRITE_CREATE_ONLY: Literal[1]
"""Only create the bitset if it does not exist."""
BIT_WRITE_DEFAULT: Literal[0]
"""Default write behavior."""
BIT_WRITE_NO_FAIL: Literal[4]
"""Do not fail if the write operation cannot be completed."""
BIT_WRITE_PARTIAL: Literal[8]
"""Allow partial write operations."""
BIT_WRITE_UPDATE_ONLY: Literal[2]
"""Only update the bitset if it exists."""
CDT_CTX_LIST_INDEX: Literal[0x10]
"""CDT context for a list index."""
CDT_CTX_LIST_INDEX_CREATE: Literal[0x14]
"""CDT context for creating a list index."""
CDT_CTX_LIST_RANK: Literal[0x11]
"""CDT context for a list rank."""
CDT_CTX_LIST_VALUE: Literal[0x13]
"""CDT context for a list value."""
CDT_CTX_MAP_INDEX: Literal[0x20]
"""CDT context for a map index."""
CDT_CTX_MAP_KEY: Literal[0x22]
"""CDT context for a map key."""
CDT_CTX_MAP_KEY_CREATE: Literal[0x24]
"""CDT context for creating a map key."""
CDT_CTX_MAP_RANK: Literal[0x21]
"""CDT context for a map rank."""
CDT_CTX_MAP_VALUE: Literal[0x23]
"""CDT context for a map value."""
EXP_READ_DEFAULT: Literal[0]
"""Default expression read behavior."""
EXP_READ_EVAL_NO_FAIL: Literal[16]
"""Do not fail if the expression evaluation fails during a read."""
EXP_WRITE_ALLOW_DELETE: Literal[4]
"""Allow deletion during an expression write."""
EXP_WRITE_CREATE_ONLY: Literal[1]
"""Only create if the expression write results in a new record."""
EXP_WRITE_DEFAULT: Literal[0]
"""Default expression write behavior."""
EXP_WRITE_EVAL_NO_FAIL: Literal[16]
"""Do not fail if the expression evaluation fails during a write."""
EXP_WRITE_POLICY_NO_FAIL: Literal[8]
"""Do not fail if the expression write policy is not met."""
EXP_WRITE_UPDATE_ONLY: Literal[2]
"""Only update if the expression write results in an existing record."""
HLL_WRITE_ALLOW_FOLD: Literal[8]
"""Allow folding during an HLL write."""
HLL_WRITE_CREATE_ONLY: Literal[1]
"""Only create if the HLL write results in a new record."""
HLL_WRITE_DEFAULT: Literal[0]
"""Default HLL write behavior."""
HLL_WRITE_NO_FAIL: Literal[4]
"""Do not fail if the HLL write operation cannot be completed."""
HLL_WRITE_UPDATE_ONLY: Literal[2]
"""Only update if the HLL write results in an existing record."""
INDEX_BLOB: Literal[3]
"""Index on a blob bin."""
INDEX_GEO2DSPHERE: Literal[2]
"""Index on a GeoJSON bin."""
INDEX_NUMERIC: Literal[1]
"""Index on a numeric bin."""
INDEX_STRING: Literal[0]
"""Index on a string bin."""
INDEX_TYPE_DEFAULT: Literal[0]
"""Default index type (on the bin value)."""
INDEX_TYPE_LIST: Literal[1]
"""Index on list elements."""
INDEX_TYPE_MAPKEYS: Literal[2]
"""Index on map keys."""
INDEX_TYPE_MAPVALUES: Literal[3]
"""Index on map values."""
INTEGER: Literal[0]
"""Represent an integer."""
JOB_QUERY: Literal["query"]
"""A background query job."""
JOB_SCAN: Literal["scan"]
"""A background scan job."""
JOB_STATUS_COMPLETED: Literal[2]
"""The background job has completed."""
JOB_STATUS_INPROGRESS: Literal[1]
"""The background job is in progress."""
JOB_STATUS_UNDEF: Literal[0]
"""The background job status is undefined."""
LIST_ORDERED: Literal[1]
"""The list is ordered."""
LIST_RETURN_COUNT: Literal[5]
"""Return the number of elements."""
LIST_RETURN_EXISTS: Literal[13]
"""Return whether the elements exist."""
LIST_RETURN_INDEX: Literal[1]
"""Return the index of the elements."""
LIST_RETURN_NONE: Literal[0]
"""Return nothing."""
LIST_RETURN_RANK: Literal[3]
"""Return the rank of the elements."""
LIST_RETURN_REVERSE_INDEX: Literal[2]
"""Return the reverse index of the elements."""
LIST_RETURN_REVERSE_RANK: Literal[4]
"""Return the reverse rank of the elements."""
LIST_RETURN_VALUE: Literal[7]
"""Return the value of the elements."""
LIST_SORT_DEFAULT: Literal[0]
"""Default list sort behavior."""
LIST_SORT_DROP_DUPLICATES: Literal[2]
"""Drop duplicate elements during sort."""
LIST_UNORDERED: Literal[0]
"""The list is unordered."""
LIST_WRITE_DEFAULT: Literal[0]
"""Default list write behavior."""
LIST_WRITE_ADD_UNIQUE: Literal[1]
"""Only add the element if it is unique."""
LIST_WRITE_INSERT_BOUNDED: Literal[2]
"""Insert the element at the specified index, shifting existing elements."""
LIST_WRITE_NO_FAIL: Literal[4]
"""Do not fail if the list write operation cannot be completed."""
LIST_WRITE_PARTIAL: Literal[8]
"""Allow partial list write operations."""
LOG_LEVEL_DEBUG: Literal[3]
"""Debug log level."""
LOG_LEVEL_ERROR: Literal[0]
"""Error log level."""
LOG_LEVEL_INFO: Literal[2]
"""Info log level."""
LOG_LEVEL_OFF: Literal[-1]
"""Logging is turned off."""
LOG_LEVEL_TRACE: Literal[4]
"""Trace log level."""
LOG_LEVEL_WARN: Literal[1]
"""Warning log level."""
MAP_KEY_ORDERED: Literal[1]
"""The map is ordered by key."""
MAP_KEY_VALUE_ORDERED: Literal[3]
"""The map is ordered by key and then value."""
MAP_RETURN_COUNT: Literal[5]
"""Return the number of elements."""
MAP_RETURN_EXISTS: Literal[13]
"""Return whether the elements exist."""
MAP_RETURN_INDEX: Literal[1]
"""Return the index of the elements."""
MAP_RETURN_KEY: Literal[6]
"""Return the keys of the elements."""
MAP_RETURN_KEY_VALUE: Literal[8]
"""Return the keys and values of the elements."""
MAP_RETURN_NONE: Literal[0]
"""Return nothing."""
MAP_RETURN_RANK: Literal[3]
"""Return the rank of the elements."""
MAP_RETURN_REVERSE_INDEX: Literal[2]
"""Return the reverse index of the elements."""
MAP_RETURN_REVERSE_RANK: Literal[4]
"""Return the reverse rank of the elements."""
MAP_RETURN_VALUE: Literal[7]
"""Return the value of the elements."""
MAP_RETURN_ORDERED_MAP: Literal[17]
"""Return an ordered map."""
MAP_RETURN_UNORDERED_MAP: Literal[16]
"""Return an unordered map."""
MAP_UNORDERED: Literal[0]
"""The map is unordered."""
MAP_WRITE_FLAGS_CREATE_ONLY: Literal[1]
"""Only create if the map does not exist."""
MAP_WRITE_FLAGS_DEFAULT: Literal[0]
"""Default map write flags."""
MAP_WRITE_FLAGS_NO_FAIL: Literal[4]
"""Do not fail if the map write operation cannot be completed."""
MAP_WRITE_FLAGS_PARTIAL: Literal[8]
"""Allow partial map write operations."""
MAP_WRITE_FLAGS_UPDATE_ONLY: Literal[2]
"""Only update if the map exists."""
MAP_WRITE_NO_FAIL: Literal[4]
"""Do not fail if the map write operation cannot be completed."""
MAP_WRITE_PARTIAL: Literal[8]
"""Allow partial map write operations."""
OPERATOR_APPEND: Literal[9]
"""Append operator."""
OPERATOR_DELETE: Literal[14]
"""Delete operator."""
OPERATOR_INCR: Literal[6]
"""Increment operator."""
OPERATOR_PREPEND: Literal[10]
"""Prepend operator."""
OPERATOR_READ: Literal[0]
"""Read operator."""
OPERATOR_TOUCH: Literal[11]
"""Touch operator."""
OPERATOR_WRITE: Literal[1]
"""Write operator."""
OP_BIT_ADD: Literal[2010]
OP_BIT_AND: Literal[2006]
OP_BIT_COUNT: Literal[2015]
OP_BIT_GET: Literal[2014]
OP_BIT_GET_INT: Literal[2012]
OP_BIT_INSERT: Literal[2001]
OP_BIT_LSCAN: Literal[2016]
OP_BIT_LSHIFT: Literal[2008]
OP_BIT_NOT: Literal[2007]
OP_BIT_OR: Literal[2004]
OP_BIT_REMOVE: Literal[2002]
OP_BIT_RESIZE: Literal[2000]
OP_BIT_RSCAN: Literal[2017]
OP_BIT_RSHIFT: Literal[2009]
OP_BIT_SET: Literal[2003]
OP_BIT_SET_INT: Literal[2013]
OP_BIT_SUBTRACT: Literal[2011]
OP_BIT_XOR: Literal[2005]
OP_EXPR_READ: Literal[2200]
OP_EXPR_WRITE: Literal[2201]
OP_HLL_ADD: Literal[2100]
OP_HLL_DESCRIBE: Literal[2101]
OP_HLL_FOLD: Literal[2102]
OP_HLL_GET_COUNT: Literal[2103]
OP_HLL_GET_INTERSECT_COUNT: Literal[2104]
OP_HLL_GET_SIMILARITY: Literal[2105]
OP_HLL_GET_UNION: Literal[2106]
OP_HLL_GET_UNION_COUNT: Literal[2107]
OP_HLL_INIT: Literal[2108]
OP_HLL_MAY_CONTAIN: Literal[2111]
OP_HLL_REFRESH_COUNT: Literal[2109]
OP_HLL_SET_UNION: Literal[2110]
OP_LIST_APPEND: Literal[1001]
OP_LIST_APPEND_ITEMS: Literal[1002]
OP_LIST_CLEAR: Literal[1009]
OP_LIST_GET: Literal[1011]
OP_LIST_GET_BY_INDEX: Literal[1016]
OP_LIST_GET_BY_INDEX_RANGE: Literal[1017]
OP_LIST_GET_BY_INDEX_RANGE_TO_END: Literal[1035]
OP_LIST_GET_BY_RANK: Literal[1018]
OP_LIST_GET_BY_RANK_RANGE: Literal[1019]
OP_LIST_GET_BY_RANK_RANGE_TO_END: Literal[1036]
OP_LIST_GET_BY_VALUE: Literal[1020]
OP_LIST_GET_BY_VALUE_LIST: Literal[1021]
OP_LIST_GET_BY_VALUE_RANGE: Literal[1022]
OP_LIST_GET_BY_VALUE_RANK_RANGE_REL: Literal[1033]
OP_LIST_GET_BY_VALUE_RANK_RANGE_REL_TO_END: Literal[1034]
OP_LIST_GET_RANGE: Literal[1012]
OP_LIST_INCREMENT: Literal[1015]
OP_LIST_INSERT: Literal[1003]
OP_LIST_INSERT_ITEMS: Literal[1004]
OP_LIST_POP: Literal[1005]
OP_LIST_POP_RANGE: Literal[1006]
OP_LIST_REMOVE: Literal[1007]
OP_LIST_REMOVE_BY_INDEX: Literal[1023]
OP_LIST_REMOVE_BY_INDEX_RANGE: Literal[1024]
OP_LIST_REMOVE_BY_INDEX_RANGE_TO_END: Literal[1039]
OP_LIST_REMOVE_BY_RANK: Literal[1025]
OP_LIST_REMOVE_BY_RANK_RANGE: Literal[1026]
OP_LIST_REMOVE_BY_RANK_RANGE_TO_END: Literal[1040]
OP_LIST_REMOVE_BY_REL_RANK_RANGE: Literal[1038]
OP_LIST_REMOVE_BY_REL_RANK_RANGE_TO_END: Literal[1037]
OP_LIST_REMOVE_BY_VALUE: Literal[1027]
OP_LIST_REMOVE_BY_VALUE_LIST: Literal[1028]
OP_LIST_REMOVE_BY_VALUE_RANGE: Literal[1029]
OP_LIST_REMOVE_BY_VALUE_RANK_RANGE_REL: Literal[1032]
OP_LIST_REMOVE_RANGE: Literal[1008]
OP_LIST_SET: Literal[1010]
OP_LIST_SET_ORDER: Literal[1030]
OP_LIST_SIZE: Literal[1014]
OP_LIST_SORT: Literal[1031]
OP_LIST_TRIM: Literal[1013]
OP_LIST_CREATE: Literal[1041]
OP_MAP_CLEAR: Literal[1107]
OP_MAP_DECREMENT: Literal[1105]
OP_MAP_GET_BY_INDEX: Literal[1122]
OP_MAP_GET_BY_INDEX_RANGE: Literal[1123]
OP_MAP_GET_BY_INDEX_RANGE_TO_END: Literal[1142]
OP_MAP_GET_BY_KEY: Literal[1118]
OP_MAP_GET_BY_KEY_INDEX_RANGE_REL: Literal[1131]
OP_MAP_GET_BY_KEY_LIST: Literal[1127]
OP_MAP_GET_BY_KEY_RANGE: Literal[1119]
OP_MAP_GET_BY_KEY_REL_INDEX_RANGE: Literal[1140]
OP_MAP_GET_BY_KEY_REL_INDEX_RANGE_TO_END: Literal[1136]
OP_MAP_GET_BY_RANK: Literal[1124]
OP_MAP_GET_BY_RANK_RANGE: Literal[1125]
OP_MAP_GET_BY_RANK_RANGE_TO_END: Literal[1143]
OP_MAP_GET_BY_VALUE: Literal[1120]
OP_MAP_GET_BY_VALUE_LIST: Literal[1126]
OP_MAP_GET_BY_VALUE_RANGE: Literal[1121]
OP_MAP_GET_BY_VALUE_RANK_RANGE_REL: Literal[1130]
OP_MAP_GET_BY_VALUE_RANK_RANGE_REL_TO_END: Literal[1141]
OP_MAP_INCREMENT: Literal[1104]
OP_MAP_PUT: Literal[1102]
OP_MAP_PUT_ITEMS: Literal[1103]
OP_MAP_REMOVE_BY_INDEX: Literal[1114]
OP_MAP_REMOVE_BY_INDEX_RANGE: Literal[1115]
OP_MAP_REMOVE_BY_INDEX_RANGE_TO_END: Literal[1134]
OP_MAP_REMOVE_BY_KEY: Literal[1108]
OP_MAP_REMOVE_BY_KEY_INDEX_RANGE_REL: Literal[1129]
OP_MAP_REMOVE_BY_KEY_LIST: Literal[1109]
OP_MAP_REMOVE_BY_KEY_RANGE: Literal[1110]
OP_MAP_REMOVE_BY_KEY_REL_INDEX_RANGE: Literal[1137]
OP_MAP_REMOVE_BY_KEY_REL_INDEX_RANGE_TO_END: Literal[1132]
OP_MAP_REMOVE_BY_RANK: Literal[1116]
OP_MAP_REMOVE_BY_RANK_RANGE: Literal[1117]
OP_MAP_REMOVE_BY_RANK_RANGE_TO_END: Literal[1135]
OP_MAP_REMOVE_BY_VALUE: Literal[1111]
OP_MAP_REMOVE_BY_VALUE_LIST: Literal[1112]
OP_MAP_REMOVE_BY_VALUE_RANGE: Literal[1113]
OP_MAP_REMOVE_BY_VALUE_RANK_RANGE_REL: Literal[1128]
OP_MAP_REMOVE_BY_VALUE_REL_INDEX_RANGE: Literal[1138]
OP_MAP_REMOVE_BY_VALUE_REL_RANK_RANGE: Literal[1139]
OP_MAP_REMOVE_BY_VALUE_REL_RANK_RANGE_TO_END: Literal[1133]
OP_MAP_CREATE: Literal[1144]
OP_MAP_SET_POLICY: Literal[1101]
OP_MAP_SIZE: Literal[1106]
POLICY_COMMIT_LEVEL_ALL: Literal[0]
POLICY_COMMIT_LEVEL_MASTER: Literal[1]
POLICY_EXISTS_CREATE: Literal[1]
POLICY_EXISTS_CREATE_OR_REPLACE: Literal[4]
POLICY_EXISTS_IGNORE: Literal[0]
POLICY_EXISTS_REPLACE: Literal[3]
POLICY_EXISTS_UPDATE: Literal[2]
POLICY_GEN_EQ: Literal[1]
POLICY_GEN_GT: Literal[2]
POLICY_GEN_IGNORE: Literal[0]
POLICY_KEY_DIGEST: Literal[0]
POLICY_KEY_SEND: Literal[1]
POLICY_READ_MODE_AP_ALL: Literal[1]
POLICY_READ_MODE_AP_ONE: Literal[0]
POLICY_READ_MODE_SC_ALLOW_REPLICA: Literal[2]
POLICY_READ_MODE_SC_ALLOW_UNAVAILABLE: Literal[3]
POLICY_READ_MODE_SC_LINEARIZE: Literal[1]
POLICY_READ_MODE_SC_SESSION: Literal[0]
POLICY_REPLICA_ANY: Literal[1]
POLICY_REPLICA_MASTER: Literal[0]
POLICY_REPLICA_PREFER_RACK: Literal[3]
POLICY_REPLICA_SEQUENCE: Literal[2]
POLICY_REPLICA_RANDOM: Literal[4]
POLICY_RETRY_NONE: Literal[0]
POLICY_RETRY_ONCE: Literal[1]
PRIV_DATA_ADMIN: Literal[2]
PRIV_READ: Literal[10]
PRIV_READ_WRITE: Literal[11]
PRIV_READ_WRITE_UDF: Literal[12]
PRIV_SINDEX_ADMIN: Literal[4]
PRIV_SYS_ADMIN: Literal[1]
PRIV_TRUNCATE: Literal[14]
PRIV_UDF_ADMIN: Literal[3]
PRIV_USER_ADMIN: Literal[0]
PRIV_WRITE: Literal[13]
REGEX_EXTENDED: Literal[1]
REGEX_ICASE: Literal[2]
REGEX_NEWLINE: Literal[8]
REGEX_NONE: Literal[0]
REGEX_NOSUB: Literal[4]
SERIALIZER_JSON: Literal[2]
SERIALIZER_NONE: Literal[0]
SERIALIZER_USER: Literal[3]
TTL_DONT_UPDATE: Literal[0xFFFFFFFE]
TTL_NAMESPACE_DEFAULT: Literal[0]
TTL_NEVER_EXPIRE: Literal[0xFFFFFFFF]
TTL_CLIENT_DEFAULT: Literal[0xFFFFFFFD]
UDF_TYPE_LUA: Literal[0]
QUERY_DURATION_LONG: Literal[0]
QUERY_DURATION_SHORT: Literal[1]
QUERY_DURATION_LONG_RELAX_AP: Literal[2]

COMMIT_OK: Literal[0]
COMMIT_ALREADY_COMMITTED: Literal[1]
COMMIT_ROLL_FORWARD_ABANDONED: Literal[5]
COMMIT_CLOSE_ABANDONED: Literal[6]

ABORT_OK: Literal[0]
ABORT_ALREADY_ABORTED: Literal[1]
ABORT_ROLL_BACK_ABANDONED: Literal[3]
ABORT_CLOSE_ABANDONED: Literal[4]

TXN_STATE_OPEN: Literal[0]
TXN_STATE_VERIFIED: Literal[1]
TXN_STATE_COMMITTED: Literal[2]
TXN_STATE_ABORTED: Literal[3]

EXP_PATH_SELECT_MATCHING_TREE: Literal[0]
EXP_PATH_SELECT_VALUE: Literal[1]
EXP_PATH_SELECT_MAP_VALUE: Literal[1]
EXP_PATH_SELECT_LIST_VALUE: Literal[1]
EXP_PATH_SELECT_MAP_KEY: Literal[2]
EXP_PATH_SELECT_MAP_KEY_VALUE: Literal[3]
EXP_PATH_SELECT_NO_FAIL: Literal[0x10]

EXP_PATH_MODIFY_DEFAULT: Literal[0]
EXP_PATH_MODIFY_NO_FAIL: Literal[0x10]

EXP_LOOPVAR_KEY: Literal[0]
EXP_LOOPVAR_VALUE: Literal[1]
EXP_LOOPVAR_INDEX: Literal[2]


@final
class CDTInfinite:
    """A class representing an infinite value for CDT operations.

    [MISSING_DESCRIPTION]

    Example:
        [MISSING_EXAMPLE]

    Args:
        [MISSING_ARGS]
    Returns:
        [MISSING_RETURNS]

    Raises:
        [MISSING_RAISES]

    See Also:
        [MISSING_SEE_ALSO]
    """
    def __init__(self) -> None:
        """Initialize the CDTInfinite class.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            [MISSING_ARGS]
        Returns:
            None.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
        ...

@final
class CDTWildcard:
    """A class representing a wildcard value for CDT operations.

    [MISSING_DESCRIPTION]

    Example:
        [MISSING_EXAMPLE]

    Args:
        [MISSING_ARGS]
    Returns:
        [MISSING_RETURNS]

    Raises:
        [MISSING_RAISES]

    See Also:
        [MISSING_SEE_ALSO]
    """
    def __init__(self) -> None:
        """Initialize the CDTWildcard class.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            [MISSING_ARGS]
        Returns:
            None.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
        ...

@final
class Transaction:
    """The Transaction class represents a Multi-Record Transaction.

    [MISSING_DESCRIPTION]

    Example:
        [MISSING_EXAMPLE]
    Attributes:
        id: Get the random transaction id that was generated on class instance creation. Read-only.
        in_doubt: Whether the transaction status is in doubt. Read-only.
        state: One of the aerospike.TXN_STATE_* constants. Read-only.
        timeout: Transaction timeout in seconds.

    Args:
        [MISSING_ARGS]
    Returns:
        [MISSING_RETURNS]

    Raises:
        [MISSING_RAISES]

    See Also:
        [MISSING_SEE_ALSO]
    """
    def __init__(self, reads_capacity: int = 128, writes_capacity: int = 128) -> None:
        """Initialize transaction and assign random transaction id.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            reads_capacity: Expected number of record reads in the transaction.
            writes_capacity: Expected number of record writes in the transaction.

        Returns:
            None.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    id: int
    in_doubt: bool
    state: int
    timeout: int

@final
class ConfigProvider:
    """Dynamic configuration provider. Determines how to retrieve cluster policies.

    [MISSING_DESCRIPTION]

    An instance of this class is immutable.

    Example:
        [MISSING_EXAMPLE]
    Attributes:
        path: Dynamic configuration file path. Read-only.
        interval: Interval in milliseconds between dynamic configuration checks. Read-only.

    Args:
        [MISSING_ARGS]
    Returns:
        [MISSING_RETURNS]

    Raises:
        [MISSING_RAISES]

    See Also:
        [MISSING_SEE_ALSO]
    """
    def __new__(cls, path: str, interval: int = 60) -> ConfigProvider:
        """Create a new ConfigProvider instance.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            path: Dynamic configuration file path.
            interval: Interval in milliseconds between checks for file modifications.

        Returns:
            ConfigProvider: A new ConfigProvider instance.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    path: str
    interval: int

class Client:
    """The Client class enables you to build an application in Python with an
    Aerospike cluster as its database.

    [MISSING_DESCRIPTION]

    The client handles the connections, including re-establishing them ahead of
    executing a command. It keeps track of changes to the cluster through
    a cluster-tending thread.

    Example:
        [MISSING_EXAMPLE]
    
    Args:
        [MISSING_ARGS]
    Returns:
        [MISSING_RETURNS]

    Raises:
        [MISSING_RAISES]

    See Also:
        `Client Architecture <https://aerospike.com/docs/database/learn/architecture/clients/>`_
        [MISSING_SEE_ALSO]
    """
    def __init__(self, *args, **kwargs) -> None:
        """Create a new instance of the Client class.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            config: A dictionary of configuration parameters.
            *args: [MISSING_ARGS]
            **kwargs: [MISSING_ARGS]

        Returns:
            None.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def admin_change_password(self, username: str, password: str, policy: dict = ...) -> None:
        """Change the password for a user.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            username: The user whose password to change.
            password: The new password.
            policy: Optional admin policy.

        Returns:
            None.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def admin_create_role(self, role: str, privileges: list, policy: dict = ..., whitelist: list = ..., read_quota: int = ..., write_quota: int = ...) -> None:
        """Create a new role with a list of privileges.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            role: The name of the role.
            privileges: A list of privileges.
            policy: Optional admin policy.
            whitelist: A list of allowed IP addresses.
            read_quota: Max read operations per second.
            write_quota: Max write operations per second.

        Returns:
            None.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def admin_create_pki_user(self, username: str, roles: list, policy: dict = ...) -> None:
        """Create a new PKI user with a list of roles.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            username: The name of the user.
            roles: A list of roles to assign.
            policy: Optional admin policy.

        Returns:
            None.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def admin_create_user(self, username: str, password: str, roles: list, policy: dict = ...) -> None:
        """Create a new user with a list of roles and a password.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            username: The name of the user.
            password: The password for the user.
            roles: A list of roles to assign.
            policy: Optional admin policy.

        Returns:
            None.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def admin_drop_role(self, role: str, policy: dict = ...) -> None:
        """Drop a role.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            role: The name of the role.
            policy: Optional admin policy.

        Returns:
            None.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def admin_drop_user(self, username: str, policy: dict = ...) -> None:
        """Drop a user.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            username: The name of the user.
            policy: Optional admin policy.

        Returns:
            None.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def admin_get_role(self, role: str, policy: dict = ...) -> dict:
        """Get the privileges for a role.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            role: The name of the role.
            policy: Optional admin policy.

        Returns:
            dict: A dictionary containing role details.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def admin_get_roles(self, policy: dict = ...) -> dict:
        """Get all roles.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            policy: Optional admin policy.

        Returns:
            dict: A dictionary containing all roles.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def admin_grant_privileges(self, role: str, privileges: list, policy: dict = ...) -> None:
        """Grant privileges to a role.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            role: The name of the role.
            privileges: A list of privileges to grant.
            policy: Optional admin policy.

        Returns:
            None.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def admin_grant_roles(self, username: str, roles: list, policy: dict = ...) -> None:
        """Grant roles to a user.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            username: The name of the user.
            roles: A list of roles to grant.
            policy: Optional admin policy.

        Returns:
            None.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def admin_query_role(self, role: str, policy: dict = ...) -> list:
        """Query a role.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            role: The name of the role.
            policy: Optional admin policy.

        Returns:
            list: A list containing role details.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def admin_query_roles(self, policy: dict = ...) -> dict:
        """Query all roles.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            policy: Optional admin policy.

        Returns:
            dict: A dictionary of all roles.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def admin_query_user_info(self, user: str, policy: dict = ...) -> dict:
        """Query user information.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            user: The name of the user.
            policy: Optional admin policy.

        Returns:
            dict: A dictionary containing user information.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def admin_query_users_info(self, policy: dict = ...) -> dict:
        """Query all users information.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            policy: Optional admin policy.

        Returns:
            dict: A dictionary containing information for all users.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def admin_revoke_privileges(self, role: str, privileges: list, policy: dict = ...) -> None:
        """Revoke privileges from a role.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            role: The name of the role.
            privileges: A list of privileges to revoke.
            policy: Optional admin policy.

        Returns:
            None.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def admin_revoke_roles(self, username: str, roles: list, policy: dict = ...) -> None:
        """Revoke roles from a user.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            username: The name of the user.
            roles: A list of roles to revoke.
            policy: Optional admin policy.

        Returns:
            None.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def admin_set_password(self, username: str, password: str, policy: dict = ...) -> None:
        """Set the password for a user.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            username: The name of the user.
            password: The new password.
            policy: Optional admin policy.

        Returns:
            None.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def admin_set_quotas(self, role: str, read_quota: int = ..., write_quota: int = ..., policy: dict = ...) -> None:
        """Set the quotas for a role.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            role: The name of the role.
            read_quota: Max read operations per second.
            write_quota: Max write operations per second.
            policy: Optional admin policy.

        Returns:
            None.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def admin_set_whitelist(self, role: str, whitelist: list, policy: dict = ...) -> None:
        """Set the whitelist for a role.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            role: The name of the role.
            whitelist: A list of allowed IP addresses.
            policy: Optional admin policy.

        Returns:
            None.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def append(self, key: tuple, bin: str, val: str, meta: dict = ..., policy: dict = ...) -> None:
        """Append a string to a bin.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            key: The record key tuple.
            bin: The bin name.
            val: The string to append.
            meta: Optional record metadata.
            policy: Optional write policy.

        Returns:
            None.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def apply(self, key: tuple, module: str, function: str, args: list, policy: dict = ...) -> Union[str, int, float, bytearray, list, dict]:
        """Apply a UDF to a record.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            key: The record key tuple.
            module: The Lua module name.
            function: The Lua function name.
            args: The function arguments.
            policy: Optional apply policy.

        Returns:
            Union[str, int, float, bytearray, list, dict]: The result of the UDF.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def batch_apply(self, keys: list, module: str, function: str, args: list, policy_batch: dict = ..., policy_batch_apply: dict = ...) -> BatchRecords:
        """Apply a UDF to multiple records.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            keys: A list of key tuples.
            module: The Lua module name.
            function: The Lua function name.
            args: The function arguments.
            policy_batch: Optional batch policy.
            policy_batch_apply: Optional batch apply policy.

        Returns:
            BatchRecords: A BatchRecords instance.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def batch_operate(self, keys: list, ops: list, policy_batch: dict = ..., policy_batch_write: dict = ..., ttl: int = ...) -> BatchRecords:
        """Perform multiple operations on multiple records.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            keys: A list of key tuples.
            ops: A list of operations.
            policy_batch: Optional batch policy.
            policy_batch_write: Optional batch write policy.
            ttl: Record time-to-live.

        Returns:
            BatchRecords: A BatchRecords instance.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def batch_remove(self, keys: list, policy_batch: dict = ..., policy_batch_remove: dict = ...) -> BatchRecords:
        """Remove multiple records.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            keys: A list of key tuples.
            policy_batch: Optional batch policy.
            policy_batch_remove: Optional batch remove policy.

        Returns:
            BatchRecords: A BatchRecords instance.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def batch_read(self, keys: list, bins: list[str] = ..., policy: dict = ...) -> BatchRecords:
        """Read multiple records.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            keys: A list of key tuples.
            bins: Optional list of bin names to read.
            policy: Optional batch policy.

        Returns:
            BatchRecords: A BatchRecords instance.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def batch_write(self, batch_records: BatchRecords, policy_batch: dict = ...) -> BatchRecords:
        """Perform writes on multiple records.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            batch_records: A BatchRecords instance containing keys and operations.
            policy_batch: Optional batch policy.

        Returns:
            BatchRecords: A BatchRecords instance.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def close(self) -> None:
        """Close all connections to the cluster.

        It is recommended to explicitly call this method when the program
        is done communicating with the cluster.

        Example:
            [MISSING_EXAMPLE]

        Args:
            [MISSING_ARGS]
        Returns:
            None.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def connect(self, username: str = ..., password: str = ...) -> Client:
        """If there is currently no connection to the cluster, connect to it.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            username: A defined user with roles in the cluster.
            password: The password for the user.

        Returns:
            Client: The Client instance.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def exists(self, key: tuple, policy: dict = ...) -> tuple:
        """Check if a record exists.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            key: The record key tuple.
            policy: Optional read policy.

        Returns:
            tuple: A tuple (key, meta).

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def get(self, key: tuple, policy: dict = ...) -> tuple:
        """Read a record.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            key: The record key tuple.
            policy: Optional read policy.

        Returns:
            tuple: A tuple (key, meta, bins).

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def get_stats(self) -> ClusterStats:
        """Get cluster statistics.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            [MISSING_ARGS]
        Returns:
            ClusterStats: A ClusterStats instance.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def get_cdtctx_base64(self, ctx: list) -> str:
        """Get a base64 encoded representation of a CDT context.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            ctx: A list of CDT context operations.

        Returns:
            str: A base64 encoded string.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    # We cannot use aerospike_helpers's TypeExpression type because mypy's stubtest will complain
    def get_expression_base64(self, expression) -> str:
        """Get a base64 encoded representation of an expression.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            expression: A compiled expression.

        Returns:
            str: A base64 encoded string.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def get_key_partition_id(self, ns: str, set: str, key: Any) -> int:
        """Get the partition ID for a key.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            ns: The namespace.
            set: The set name.
            key: The key.

        Returns:
            int: The partition ID.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def get_node_names(self) -> list:
        """Get the names of the nodes in the cluster.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            [MISSING_ARGS]
        Returns:
            list: A list of node names.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def get_nodes(self) -> list:
        """Get the nodes in the cluster.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            [MISSING_ARGS]
        Returns:
            list: A list of nodes.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def increment(self, key: tuple, bin: str, offset: int, meta: dict = ..., policy: dict = ...) -> None:
        """Increment the value of a bin.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            key: The record key tuple.
            bin: The bin name.
            offset: The amount to increment by.
            meta: Optional record metadata.
            policy: Optional write policy.

        Returns:
            None.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """

    # Index creation for root-level bin values
    def index_geo2dsphere_create(self, ns: str, set: str, bin: str, name: str, policy: dict = ...) -> None:
        """Create a Geo2DSphere index.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            ns: The namespace.
            set: The set name.
            bin: The bin name.
            name: The index name.
            policy: Optional info policy.

        Returns:
            None.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def index_integer_create(self, ns: str, set: str, bin: str, name: str, policy: dict = ...) -> None:
        """Create an integer index.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            ns: The namespace.
            set: The set name.
            bin: The bin name.
            name: The index name.
            policy: Optional info policy.

        Returns:
            None.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def index_string_create(self, ns: str, set: str, bin: str, name: str, policy: dict = ...) -> None:
        """Create a string index.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            ns: The namespace.
            set: The set name.
            bin: The bin name.
            name: The index name.
            policy: Optional info policy.

        Returns:
            None.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def index_blob_create(self, ns: str, set: str, bin: str, name: str, policy: dict = ...) -> None:
        """Create a blob index.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            ns: The namespace.
            set: The set name.
            bin: The bin name.
            name: The index name.
            policy: Optional info policy.

        Returns:
            None.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """

    # We cannot use aerospike_helpers's TypeExpression type because mypy's stubtest will complain
    def index_single_value_create(self, ns: str, set: str, bin: str, index_datatype: int, name: str, policy: dict = ..., ctx: Optional[list] = ...) -> None:
        """Create a single-value index.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            ns: The namespace.
            set: The set name.
            bin: The bin name.
            index_datatype: The index data type.
            name: The index name.
            policy: Optional info policy.
            ctx: Optional CDT context.

        Returns:
            None.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def index_list_create(self, ns: str, set: str, bin: str, index_datatype: int, name: str, policy: dict = ..., ctx: Optional[list] = ...) -> None:
        """Create a list index.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            ns: The namespace.
            set: The set name.
            bin: The bin name.
            index_datatype: The index data type.
            name: The index name.
            policy: Optional info policy.
            ctx: Optional CDT context.

        Returns:
            None.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def index_map_keys_create(self, ns: str, set: str, bin: str, index_datatype: int, name: str, policy: dict = ..., ctx: Optional[list] = ...) -> None:
        """Create a map keys index.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            ns: The namespace.
            set: The set name.
            bin: The bin name.
            index_datatype: The index data type.
            name: The index name.
            policy: Optional info policy.
            ctx: Optional CDT context.

        Returns:
            None.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def index_map_values_create(self, ns: str, set: str, bin: str, index_datatype: int, name: str, policy: dict = ..., ctx: Optional[list] = ...) -> None:
        """Create a map values index.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            ns: The namespace.
            set: The set name.
            bin: The bin name.
            index_datatype: The index data type.
            name: The index name.
            policy: Optional info policy.
            ctx: Optional CDT context.

        Returns:
            None.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """

    def index_cdt_create(self, ns: str, set: str, bin: str, index_type: int, index_datatype: int, name: str, ctx: list, policy: dict = ...) -> int:
        """Create a CDT index.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            ns: The namespace.
            set: The set name.
            bin: The bin name.
            index_type: The index type.
            index_datatype: The index data type.
            name: The index name.
            ctx: The CDT context.
            policy: Optional info policy.

        Returns:
            int: The index ID.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def index_expr_create(self, ns: str, set: str, index_type: int, index_datatype: int, expressions: list, name: str, policy: dict = ...) -> None:
        """Create an expression-based index.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            ns: The namespace.
            set: The set name.
            index_type: The index type.
            index_datatype: The index data type.
            expressions: A list of expressions.
            name: The index name.
            policy: Optional info policy.

        Returns:
            None.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """

    def index_remove(self, ns: str, name: str, policy: dict = ...) -> None:
        """Remove an index.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            ns: The namespace.
            name: The index name.
            policy: Optional info policy.

        Returns:
            None.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """

    def info_all(self, command: str, policy: dict = ...) -> dict:
        """Send an info command to all nodes in the cluster.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            command: The info command.
            policy: Optional info policy.

        Returns:
            dict: A dictionary of responses from all nodes.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def info_random_node(self, command: str, policy: dict = ...) -> str:
        """Send an info command to a random node in the cluster.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            command: The info command.
            policy: Optional info policy.

        Returns:
            str: The response from the node.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def info_single_node(self, command: str, host: str, policy: dict = ...) -> str:
        """Send an info command to a single node in the cluster.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            command: The info command.
            host: The node's host address.
            policy: Optional info policy.

        Returns:
            str: The response from the node.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def is_connected(self) -> bool:
        """Test the connections between the client and the nodes of the cluster.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            [MISSING_ARGS]
        Returns:
            bool: True if connected, False otherwise.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def job_info(self, job_id: int, module: int, policy: dict = ...) -> dict:
        """Get the status of a background job.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            job_id: The job ID.
            module: The job module (e.g., aerospike.JOB_SCAN).
            policy: Optional info policy.

        Returns:
            dict: A dictionary containing job information.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def enable_metrics(self, policy: Optional[MetricsPolicy] = None) -> None:
        """Enable metrics collection.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            policy: Optional metrics policy.

        Returns:
            None.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def disable_metrics(self) -> None:
        """Disable metrics collection.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            [MISSING_ARGS]
        Returns:
            None.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def operate(self, key: tuple, list: list, meta: dict = ..., policy: dict = ...) -> tuple:
        """Perform multiple operations on a single record.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            key: The record key tuple.
            list: A list of operations.
            meta: Optional record metadata.
            policy: Optional write policy.

        Returns:
            tuple: A tuple (key, meta, bins).

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def operate_ordered(self, key: tuple, list: list, meta: dict = ..., policy: dict = ...) -> list:
        """Perform multiple operations on a single record and return results in order.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            key: The record key tuple.
            list: A list of operations.
            meta: Optional record metadata.
            policy: Optional write policy.

        Returns:
            list: A list of results.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def prepend(self, key: tuple, bin: str, val: str, meta: dict = ..., policy: dict = ...) -> None:
        """Prepend a string to a bin.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            key: The record key tuple.
            bin: The bin name.
            val: The string to prepend.
            meta: Optional record metadata.
            policy: Optional write policy.

        Returns:
            None.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def put(self, key: tuple, bins: dict, meta: dict = ..., policy: dict = ..., serializer = ...) -> None:
        """Create or update a record.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            key: The record key tuple.
            bins: A dictionary of bins and their values.
            meta: Optional record metadata.
            policy: Optional write policy.
            serializer: Optional serializer.

        Returns:
            None.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def query(self, namespace: str, set: Optional[str] = None) -> Query:
        """Create a new Query instance.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            namespace: The namespace to query.
            set: The set to query.

        Returns:
            Query: A Query instance.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def query_apply(self, ns: str, set: str, predicate: tuple, module: str, function: str, args: list = ..., policy: dict = ...) -> int:
        """Apply a UDF to records matching a query predicate.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            ns: The namespace.
            set: The set name.
            predicate: The query predicate.
            module: The Lua module name.
            function: The Lua function name.
            args: The function arguments.
            policy: Optional write policy.

        Returns:
            int: The job ID.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def remove(self, key: tuple, meta: dict = ..., policy: dict = ...) -> None:
        """Remove a record.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            key: The record key tuple.
            meta: Optional record metadata.
            policy: Optional write policy.

        Returns:
            None.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def remove_bin(self, key: tuple, list: list, meta: dict = ..., policy: dict = ...) -> None:
        """Remove bins from a record.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            key: The record key tuple.
            list: A list of bin names to remove.
            meta: Optional record metadata.
            policy: Optional write policy.

        Returns:
            None.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def scan(self, namespace: str, set: Optional[str] = None) -> Scan:
        """Create a new Scan instance.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            namespace: The namespace to scan.
            set: The set to scan.

        Returns:
            Scan: A Scan instance.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def scan_apply(self, ns: str, set: str, module: str, function: str, args: list = ..., policy: dict = ..., options: dict = ...) -> int:
        """Apply a UDF to all records in a namespace/set.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            ns: The namespace.
            set: The set name.
            module: The Lua module name.
            function: The Lua function name.
            args: The function arguments.
            policy: Optional write policy.
            options: Optional scan options.

        Returns:
            int: The job ID.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def select(self, *args, **kwargs) -> tuple:
        """Read specific bins from a record.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            key: The record key tuple (in *args).
            bins: A list of bin names to read (in *args).
            policy: Optional read policy (in **kwargs).

        Returns:
            tuple: A tuple (key, meta, bins).

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    # We cannot use aerospike_helpers's TypeExpression type because mypy's stubtest will complain
    def set_xdr_filter(self, data_center: str, namespace: str, expression_filter, policy: dict = ...) -> str:
        """Set an XDR filter.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            data_center: The data center name.
            namespace: The namespace name.
            expression_filter: The expression filter.
            policy: Optional info policy.

        Returns:
            str: The filter result.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def shm_key(self) -> Union[int, None]:
        """Get the shared memory key.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            [MISSING_ARGS]
        Returns:
            Union[int, None]: The shared memory key or None.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def touch(self, key: tuple, val: int = ..., meta: dict = ..., policy: dict = ...) -> None:
        """Update the time-to-live of a record.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            key: The record key tuple.
            val: The new TTL.
            meta: Optional record metadata.
            policy: Optional write policy.

        Returns:
            None.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def truncate(self, namespace: str, set: str, nanos: int, policy: dict = ...) -> int:
        """Truncate a set.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            namespace: The namespace.
            set: The set name.
            nanos: The threshold time in nanoseconds.
            policy: Optional info policy.

        Returns:
            int: The result of the truncate command.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def udf_get(self, module: str, language: int = ..., policy: dict = ...) -> str:
        """Get the content of a UDF module.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            module: The name of the module.
            language: The language of the module.
            policy: Optional info policy.

        Returns:
            str: The content of the module.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def udf_list(self, policy: dict = ...) -> list:
        """List all UDF modules.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            policy: Optional info policy.

        Returns:
            list: A list of modules.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def udf_put(self, filename: str, udf_type = ..., policy: dict = ...) -> None:
        """Upload a UDF module.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            filename: The path to the Lua file.
            udf_type: The language of the module.
            policy: Optional info policy.

        Returns:
            None.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def udf_remove(self, module: str, policy: dict = ...) -> None:
        """Remove a UDF module.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            module: The name of the module to remove.
            policy: Optional info policy.

        Returns:
            None.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def commit(self, transaction: Transaction) -> int:
        """Commit a transaction.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            transaction: The Transaction instance.

        Returns:
            int: The result of the commit.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def abort(self, transaction: Transaction) -> int:
        """Abort a transaction.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            transaction: The Transaction instance.

        Returns:
            int: The result of the abort.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """

class GeoJSON:
    """The GeoJSON class enables serialization of geospatial data.

    Starting with version 3.7.0, the Aerospike server supports storing GeoJSON data.
    Wrapping geospatial data in an instance of the GeoJSON class enables
    serialization of the data into the correct type during a write operation.
    When reading a record from the server, bins with geospatial data will be
    deserialized into a GeoJSON instance.

    [MISSING_DESCRIPTION]

    Example:
        >>> import aerospike
        >>> from aerospike import GeoJSON
        >>> # Create GeoJSON point using WGS84 coordinates.
        >>> latitude = 28.608389
        >>> longitude = -80.604333
        >>> loc = GeoJSON({'type': "Point",
        ...                 'coordinates': [longitude, latitude]})
        >>> print(loc)
        {"type": "Point", "coordinates": [-80.604333, 28.608389]}

    Args:
        [MISSING_ARGS]
    
    Returns:
        [MISSING_RETURNS]
    
    Raises:
        [MISSING_RAISES]
    See Also:
        `Geospatial Index and Query <https://aerospike.com/docs/develop/data-types/geospatial/>`_
    """
    geo_data: Any
    def __init__(self, geo_data: Union[str, dict] = ...) -> None:
        """Initialize a GeoJSON object with a str or a dict of geospatial data.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            geo_data: A GeoJSON str or a dict of geospatial data.

        Returns:
            None.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def dumps(self) -> str:
        """Get the geospatial data as a GeoJSON string.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            [MISSING_ARGS]
        Returns:
            str: A GeoJSON str representing the geospatial data.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def loads(self, raw_geo: str) -> None:
        """Set the geospatial data from a GeoJSON string.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            raw_geo: A GeoJSON string representation.

        Returns:
            None.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def unwrap(self) -> dict:
        """Get the geospatial data as a dict.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            [MISSING_ARGS]
        Returns:
            dict: A dict representing the geospatial data.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def wrap(self, geo_data: dict) -> None:
        """Set the geospatial data from a dict.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            geo_data: A dict representing the geospatial data.

        Returns:
            None.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """

class KeyOrderedDict(dict):
    """A dictionary that directly maps to a key ordered map on the Aerospike server.

    KeyOrderedDict inherits from dict and has no extra functionality.
    The only difference is its mapping to a key ordered map.

    [MISSING_DESCRIPTION]

    Example:
        >>> import aerospike
        >>> from aerospike import KeyOrderedDict
        >>> client = aerospike.client({'hosts': [('127.0.0.1', 3000)]})
        >>> key = ('test', 'demo', 'keyordereddict')
        >>> # create a key ordered map
        >>> client.put(key, {'map': KeyOrderedDict({'b': 2, 'a': 1})})

    Args:
        [MISSING_ARGS]
    
    Returns:
        [MISSING_RETURNS]
    
    Raises:
        [MISSING_RAISES]
    See Also:
        [MISSING_SEE_ALSO]
    """
    def __init__(self, *args, **kwargs) -> None:
        """Initialize a KeyOrderedDict.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.

        Returns:
            None.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """

class Query:
    """The query object is used for executing queries over a secondary index of a specified set.

    [MISSING_DESCRIPTION]

    Example:
        [MISSING_EXAMPLE]
    Attributes:
        max_records: Approximate number of records to return to client.
        records_per_second: Limit the scan to process records at records_per_second.
        ttl: The time-to-live (expiration) of the record in seconds.

    Args:
        [MISSING_ARGS]
    Returns:
        [MISSING_RETURNS]

    Raises:
        [MISSING_RAISES]

    See Also:
        `Queries <https://aerospike.com/docs/develop/learn/queries/>`_
        [MISSING_SEE_ALSO]
    """
    max_records: int
    records_per_second: int
    ttl: int
    def __init__(self, *args, **kwargs) -> None:
        """Initialize the Query class.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            *args: Arbitrary positional arguments.
            **kwargs: Arbitrary keyword arguments.

        Returns:
            None.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def add_ops(self, ops: list) -> None:
        """Add a list of write ops to the query.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            ops: A list of write operations generated by the aerospike_helpers.

        Returns:
            None.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def apply(self, module: str, function: str, arguments: list = ...) -> Any:
        """Aggregate the results using a stream UDF.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            module: The name of the Lua module.
            function: The name of the Lua function within the module.
            arguments: Optional arguments to pass to the function.

        Returns:
            Any: The result of the aggregation.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def execute_background(self, policy: dict = ...) -> int:
        """Execute a record UDF or write operations on records found by the query in the background.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            policy: Optional write policies.

        Returns:
            int: A job ID that can be used with Client.job_info to track the status.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def foreach(self, callback: Callable, policy: dict = ..., options: dict = ...) -> None:
        """Invoke the callback function for each of the records streaming back from the query.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            callback: The function to invoke for each record.
            policy: Optional query policies.
            options: Optional query options.

        Returns:
            None.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def get_partitions_status(self) -> tuple:
        """Get this query instance's partition status.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            [MISSING_ARGS]

        Returns:
            tuple: A dict containing partition status.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def is_done(self) -> bool:
        """Did the previous paginated or partition_filter query return all records?

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            [MISSING_ARGS]

        Returns:
            bool: True if all records have been returned.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def paginate(self) -> None:
        """Makes a query instance a paginated query.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            [MISSING_ARGS]

        Returns:
            None.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def results(self, policy: dict = ..., options: dict = ...) -> list:
        """Buffer the records resulting from the query, and return them as a list of records.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            policy: Optional query policies.
            options: Optional query options.

        Returns:
            list: A list of (key, meta, bins) tuples.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    # TODO: this isn't an infinite list of bins
    def select(self, *args, **kwargs) -> None:
        """Set a filter on the record bins resulting from results or foreach.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            *args: Bin names to select.
            **kwargs: Arbitrary keyword arguments.

        Returns:
            None.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def where(self, predicate: tuple, ctx: list = ...) -> None:
        """Set a where predicate for the query.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            predicate: The tuple produced by either aerospike.predicates.equals or between.
            ctx: Optional list produced by one of the aerospike_helpers.cdt_ctx methods.

        Returns:
            None.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    # We cannot use aerospike_helpers's TypeExpression type because mypy's stubtest will complain
    def where_with_expr(self, expr, predicate: tuple) -> Query:
        """Add an expression predicate to the query.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            expr: Compiled aerospike expressions or base64 encoded string.
            predicate: The tuple produced from aerospike.predicates.

        Returns:
            Query: The query instance.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def where_with_index_name(self, index_name: str, predicate: tuple) -> Query:
        """Add an index name predicate to the query.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            index_name: The name of the index.
            predicate: The tuple produced from aerospike.predicates.

        Returns:
            Query: The query instance.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """

class Scan:
    """The Scan object is used to return all the records in a specified set.

    [MISSING_DESCRIPTION]

    Example:
        [MISSING_EXAMPLE]
    Attributes:
        ttl: The time-to-live (expiration) of the record in seconds.

    Args:
        [MISSING_ARGS]
    
    Returns:
        [MISSING_RETURNS]
    
    Raises:
        [MISSING_RAISES]
    See Also:
        :class:`Query`: Query class.
    """
    ttl: int
    def __init__(self, *args, **kwargs) -> None:
        """Initialize the Scan class.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            *args: Arbitrary positional arguments.
            **kwargs: Arbitrary keyword arguments.

        Returns:
            None.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def add_ops(self, ops: list) -> None:
        """Add a list of write ops to the scan.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            ops: A list of write operations generated by the aerospike_helpers.

        Returns:
            None.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def apply(self, module: str, function: str, arguments: list = ...) -> Any:
        """Apply a record UDF to each record found by the scan.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            module: The name of the Lua module.
            function: The name of the Lua function within the module.
            arguments: Optional arguments to pass to the function.

        Returns:
            Any: The result of the UDF.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def foreach(self, callback: Callable, policy: dict = ..., options: dict = ..., nodename: str = ...) -> None:
        """Invoke the callback function for each of the records streaming back from the scan.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            callback: The function to invoke for each record.
            policy: Optional scan policies.
            options: Optional scan options.
            nodename: Optional Node ID to limit scan to a single node.

        Returns:
            None.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def execute_background(self, policy: dict = ...) -> int:
        """Execute a record UDF on records found by the scan in the background.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            policy: Optional write policies.

        Returns:
            int: A job ID that can be used with Client.job_info to track the status.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def get_partitions_status(self) -> tuple:
        """Get this scan instance's partition status.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            [MISSING_ARGS]
        Returns:
            tuple: A dict containing partition status.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def is_done(self) -> bool:
        """Did the previous paginated or partition_filter scan return all records?

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            [MISSING_ARGS]
        Returns:
            bool: True if all records have been returned.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def paginate(self) -> None:
        """Makes a scan instance a paginated scan.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            [MISSING_ARGS]
        Returns:
            None.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    def results(self, policy: dict = ..., nodename: str = ...) -> list:
        """Buffer the records resulting from the scan, and return them as a list of records.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            policy: Optional scan policies.
            nodename: Optional Node ID to limit scan to a single node.

        Returns:
            list: A list of (key, meta, bins) tuples.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """
    # TODO: this isn't an infinite list of bins
    def select(self, *args, **kwargs) -> None:
        """Set a filter on the record bins resulting from results or foreach.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            *args: Bin names to select.
            **kwargs: Arbitrary keyword arguments.

        Returns:
            None.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """

@final
class null:
    """A type for distinguishing a server-side null from a Python None.

    Replaces the constant aerospike.null.

    [MISSING_DESCRIPTION]

    Example:
        [MISSING_EXAMPLE]
    
    Args:
        [MISSING_ARGS]
    
    Returns:
        [MISSING_RETURNS]
    
    Raises:
        [MISSING_RAISES]
    See Also:
        [MISSING_SEE_ALSO]
    """
    def __init__(self) -> None:
        """Initialize a null type representing the server-side type as_null.

        [MISSING_DESCRIPTION]

        Example:
            [MISSING_EXAMPLE]

        Args:
            [MISSING_ARGS]
        Returns:
            None.

        Raises:
            [MISSING_RAISES]

        See Also:
            [MISSING_SEE_ALSO]
        """

def calc_digest(ns: str, set: str, key: Union[str, int, bytearray]) -> bytearray:
    """Calculate the digest of a particular key.

    [MISSING_DESCRIPTION]

    Example:
        >>> import aerospike
        >>> digest = aerospike.calc_digest("test", "demo", 1)

    Args:
        ns: The namespace in the aerospike cluster.
        set: The set name.
        key: The primary key identifier of the record within the set.

    Returns:
        bytearray: A RIPEMD-160 digest of the input tuple.

    Raises:
        [MISSING_RAISES]

    See Also:
        `Aerospike Key Tuple <https://aerospike.com/docs/develop/client/python/usage/keys/>`_
    """
def client(config: dict) -> Client:
    """Create a new instance of the Client class and immediately connect to the cluster.

    Internally, this is a wrapper function which calls the constructor for the Client class.
    However, the client may also be constructed by calling the constructor directly.
    The client takes on many configuration parameters passed in through a dictionary.

    [MISSING_DESCRIPTION]

    Example:
        >>> import aerospike
        >>> # Configure the client to first connect to a cluster node at 127.0.0.1
        >>> # The client will learn about the other nodes in the cluster from the seed node.
        >>> # Also sets a top level policy for read commands
        >>> config = {
        ...     'hosts':    [ ('127.0.0.1', 3000) ],
        ...     'policies': {'read': {'total_timeout': 1000}},
        ... }
        >>> client = aerospike.client(config)

    Args:
        config: A dictionary of configuration parameters.

    Returns:
        Client: An instance of the Client class.

    Raises:
        [MISSING_RAISES]

    See Also:
        :class:`Client`
    """
def geodata(geo_data: dict) -> GeoJSON:
    """Helper for creating an instance of the GeoJSON class.

    Used to wrap a geospatial object, such as a point, polygon or circle.

    [MISSING_DESCRIPTION]

    Example:
        >>> import aerospike
        >>> # Create GeoJSON point using WGS84 coordinates.
        >>> latitude = 28.608389
        >>> longitude = -80.604333
        >>> loc = aerospike.geodata({'type': "Point",
        ...                         'coordinates': [longitude, latitude]})

    Args:
        geo_data: A dict representing the geospatial data.

    Returns:
        GeoJSON: An instance of the GeoJSON class.

    Raises:
        [MISSING_RAISES]

    See Also:
        [MISSING_SEE_ALSO]
    """
def geojson(geojson_str: str) -> GeoJSON:
    """Helper for creating an instance of the GeoJSON class from a raw GeoJSON str.

    [MISSING_DESCRIPTION]

    Example:
        >>> import aerospike
        >>> # Create GeoJSON point using WGS84 coordinates.
        >>> loc = aerospike.geojson('{"type": "Point", "coordinates": [-80.604333, 28.608389]}')

    Args:
        geojson_str: A string of raw GeoJSON.

    Returns:
        GeoJSON: An instance of the GeoJSON class.

    Raises:
        [MISSING_RAISES]

    See Also:
        [MISSING_SEE_ALSO]
    """
def get_partition_id(*args, **kwargs) -> Any: ...
def set_deserializer(callback: Callable) -> None:
    """Register a user-defined deserializer available to all Client instances.

    Once registered, all read methods (such as Client.get) will run bins containing 'Generic' as_bytes
    of type AS_BYTES_BLOB through this deserializer.

    [MISSING_DESCRIPTION]

    Example:
        [MISSING_EXAMPLE]

    Args:
        callback: The function to invoke for deserialization.

    Returns:
        None.

    Raises:
        [MISSING_RAISES]

    See Also:
        [MISSING_SEE_ALSO]
    """
def set_log_handler(callback: Callable = ...) -> None:
    """Set logging callback globally across all clients.

    When no argument is passed, the default log handler is used.
    When callback is None, the saved log handler is cleared.

    [MISSING_DESCRIPTION]

    Example:
        [MISSING_EXAMPLE]

    Args:
        callback: The function to invoke for logging. It must have five parameters:
            (level, func, file, line, msg).

    Returns:
        None.

    Raises:
        [MISSING_RAISES]

    See Also:
        [MISSING_SEE_ALSO]
    """
def set_log_level(log_level: int) -> None:
    """Declare the logging level threshold for the log handler.

    If setting log level to aerospike.LOG_LEVEL_OFF, the current log handler does not get reset.

    [MISSING_DESCRIPTION]

    Example:
        [MISSING_EXAMPLE]

    Args:
        log_level: One of the aerospike.LOG_LEVEL_* constant values.

    Returns:
        None.

    Raises:
        [MISSING_RAISES]

    See Also:
        [MISSING_SEE_ALSO]
    """
def set_serializer(callback: Callable) -> None:
    """Register a user-defined serializer available to all Client instances.

    [MISSING_DESCRIPTION]

    Example:
        >>> def my_serializer(val):
        ...     return json.dumps(val)
        >>> aerospike.set_serializer(my_serializer)

    Args:
        callback: The function to invoke for serialization.

    Returns:
        None.

    Raises:
        [MISSING_RAISES]

    See Also:
        To use this function with :meth:`Client.put`, the argument to the serializer
        parameter should be :const:`aerospike.SERIALIZER_USER`.
    """
def unset_serializers() -> None:
    """Deregister the user-defined deserializer/serializer available from Client instances.

    [MISSING_DESCRIPTION]

    Example:
        [MISSING_EXAMPLE]

    Args:
        [MISSING_ARGS]
    Returns:
        None.

    Raises:
        [MISSING_RAISES]

    See Also:
        [MISSING_SEE_ALSO]
    """
