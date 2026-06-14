"""Aerospike client for Python (``aerospike`` package).

This package provides a Python client for Aerospike database clusters. The client
manages connections to the cluster and handles commands performed against it.

Full reference: https://aerospike-python-client.readthedocs.io/
"""

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
    """
    Sentinel representing positive infinity for CDT compare operations.

    May only be used as a comparison value in list/map operations; it cannot be stored
    in the database. Requires Aerospike Server 4.3.1.3 or greater.
    """
    def __init__(self) -> None:
        """Initialize the CDTInfinite class.
        Example:
        Args:
        Returns:
            None.

        Raises:
        See Also:
        """
        ...

@final
class CDTWildcard:
    """
    Sentinel wildcard for CDT compare operations (matches any sub-value at that position).

    May only be used as a comparison value; it cannot be stored in the database.
    Requires Aerospike Server 4.3.1.3 or greater.
    """
    def __init__(self) -> None:
        """Initialize the CDTWildcard class.
        Example:
        Args:
        Returns:
            None.

        Raises:
        See Also:
        """
        ...

@final
class Transaction:
    """
    The Transaction class represents a Multi-Record Transaction.

    Initialize transaction, assign random transaction id and initialize reads/writes hashmaps with default capacities. For ``reads_capacity`` and ``writes_capacity``, pass unsigned 32-bit integers (minimum 16).

    Attributes:
        id: Random transaction id (unsigned 64-bit), read-only.
        in_doubt: Whether the transaction status is in doubt, read-only.
        state: One of the ``aerospike.TXN_STATE_*`` constants, read-only.
        timeout: Transaction timeout in seconds (see server MRT documentation).
    """
    def __init__(self, reads_capacity: int = 128, writes_capacity: int = 128) -> None:
        """
        Initialize transaction, assign random transaction id and initialize reads/writes hashmaps with default capacities.

        For both parameters, pass an unsigned 32-bit integer; the minimum value should be 16.

        Args:
            reads_capacity: Expected number of record reads in the transaction (default 128).
            writes_capacity: Expected number of record writes in the transaction (default 128).
        """
    id: int
    in_doubt: bool
    state: int
    timeout: int

@final
class ConfigProvider:
    """
    Dynamic configuration provider. Determines how to retrieve cluster policies.

        An instance of this class is immutable.

        For the ``interval`` parameter, an unsigned 32-bit integer must be passed.

        :param path: Dynamic configuration file path. Cluster policies will be read from the yaml file at cluster initialization and whenever the file changes. The policies fields in the file override all command policies.
        :type path: str

        :param interval: Interval in milliseconds between dynamic configuration check for file modifications.
            The value must be greater than or equal to the tend interval. Defaults to ``5000``.
        :type interval: int | None

    Attributes:
        path: Dynamic configuration file path, read-only.
        interval: Interval in milliseconds between configuration checks, read-only.
    """
    def __new__(cls, path: str, interval: int = 60) -> ConfigProvider:
        """Create a new ConfigProvider instance.
        Example:
        Args:
            path: Dynamic configuration file path.
            interval: Interval in milliseconds between checks for file modifications.

        Returns:
            ConfigProvider: A new ConfigProvider instance.

        Raises:
        See Also:
        """
    path: str
    interval: int

class Client:
    """
    The Client class enables you to build an application in Python with an Aerospike cluster as its database.

    The client connects through a seed node (the address of a single node) to an
    Aerospike database cluster. From the seed node, the client learns of the other
    nodes and establishes connections to them. It also gets the partition map of
    the cluster, which is how it knows where every record actually lives.

    The client handles the connections, including re-establishing them ahead of
    executing an command. It keeps track of changes to the cluster through
    a cluster-tending thread.
    """
    def __init__(self, *args, **kwargs) -> None:
        """
        Instantiate ``Client`` with the same ``config`` dict as the ``aerospike.client(config)`` factory. Prefer the factory unless subclassing.
        """
    def admin_change_password(self, username: str, password: str, policy: dict = ...) -> None:
        """
        Change the password of a user.

        This operation can only be performed by that same user.

        :param str user: the username of the user.
        :param str password: the password associated with the given username.
        :param dict policy: optional the Aerospike Python client documentation.

        :raises: one of the AdminError subclasses.
        """
    def admin_create_role(self, role: str, privileges: list, policy: dict = ..., whitelist: list = ..., read_quota: int = ..., write_quota: int = ...) -> None:
        """
        Create a custom role containing a list of privileges, as well as an optional whitelist and quotas.

        :param str role: The name of the role.
        :param list privileges: A list of the Aerospike Python client documentation.
        :param dict policy: See the Aerospike Python client documentation.
        :param list whitelist: A list of whitelist IP addresses that can contain wildcards, for example ``10.1.2.0/24``.
        :param int read_quota: Maximum reads per second limit. Pass in ``0`` for no limit.
        :param int write_quota: Maximum write per second limit, Pass in ``0`` for no limit.

        :raises: One of the AdminError subclasses.
        """
    def admin_create_pki_user(self, username: str, roles: list, policy: dict = ...) -> None:
        """
        Create a user and grant it roles. PKI users are authenticated via TLS and a certificate instead of a password.

        Warning: This function should only be called for server versions 8.1+. If this function is called for older server versions,
            an error will be returned.

        :param str user: the username to be added to the Aerospike cluster.
        :param list roles: the list of role names assigned to the user.
        :param dict policy: optional the Aerospike Python client documentation.

        :raises: one of the AdminError subclasses.
        """
    def admin_create_user(self, username: str, password: str, roles: list, policy: dict = ...) -> None:
        """
        Create a user and grant it roles.

        :param str user: the username to be added to the Aerospike cluster.
        :param str password: the password associated with the given username.
        :param list roles: the list of role names assigned to the user.
        :param dict policy: optional the Aerospike Python client documentation.

        :raises: one of the AdminError subclasses.
        """
    def admin_drop_role(self, role: str, policy: dict = ...) -> None:
        """
        Drop a custom role.

        :param str role: the name of the role.
        :param dict policy: See the Aerospike Python client documentation.

        :raises: one of the AdminError subclasses.
        """
    def admin_drop_user(self, username: str, policy: dict = ...) -> None:
        """
        Drop the user with a specified username from the cluster.

        :param str user: the username to be dropped from the aerospike cluster.

        :param dict policy: optional the Aerospike Python client documentation.

        :raises: one of the AdminError subclasses.
        """
    def admin_get_role(self, role: str, policy: dict = ...) -> dict:
        """
        Get a dict of privileges, whitelist, and quotas associated with a role.

        :param str role: the name of the role.
        :param dict policy: See the Aerospike Python client documentation.

        :return: a the Aerospike Python client documentation.

        :raises: one of the AdminError subclasses.
        """
    def admin_get_roles(self, policy: dict = ...) -> dict:
        """
        Get the names of all roles and their attributes.

        :param dict policy: See the Aerospike Python client documentation.

        :return: a dict of the Aerospike Python client documentation keyed by role names.

        :raises: one of the AdminError subclasses.
        """
    def admin_grant_privileges(self, role: str, privileges: list, policy: dict = ...) -> None:
        """
        Add privileges to a role.

        :param str role: the name of the role.
        :param list privileges: a list of the Aerospike Python client documentation.
        :param dict policy: See the Aerospike Python client documentation.

        :raises: one of the AdminError subclasses.
        """
    def admin_grant_roles(self, username: str, roles: list, policy: dict = ...) -> None:
        """
        Add roles to a user.

        :param str user: the username of the user.
        :param list roles: a list of role names.
        :param dict policy: optional the Aerospike Python client documentation.

        :raises: one of the AdminError subclasses.
        """
    def admin_query_role(self, role: str, policy: dict = ...) -> list:
        """
        Get the list of privileges associated with a role.

        :param str role: the name of the role.
        :param dict policy: See the Aerospike Python client documentation.

        :return: a list of the Aerospike Python client documentation.

        :raises: one of the AdminError subclasses.
        """
    def admin_query_roles(self, policy: dict = ...) -> dict:
        """
        Get all named roles and their privileges.

        :param dict policy: optional the Aerospike Python client documentation.

        :return: a dict of the Aerospike Python client documentation keyed by role name.

        :raises: one of the AdminError subclasses.
        """
    def admin_query_user_info(self, user: str, policy: dict = ...) -> dict:
        """
        Retrieve roles and other info for a given user.

        :param str user: the username of the user.
        :param dict policy: optional the Aerospike Python client documentation.

        :return: a dict of user data. See the Aerospike Python client documentation.
        """
    def admin_query_users_info(self, policy: dict = ...) -> dict:
        """
        Retrieve roles and other info for all users.

        :param dict policy: optional the Aerospike Python client documentation.

        :return: a dict mapping usernames to user dictionaries. See the Aerospike Python client documentation.

        Metrics
        -------
        """
    def admin_revoke_privileges(self, role: str, privileges: list, policy: dict = ...) -> None:
        """
        Remove privileges from a role.

        :param str role: the name of the role.
        :param list privileges: a list of the Aerospike Python client documentation.
        :param dict policy: See the Aerospike Python client documentation.

        :raises: one of the AdminError subclasses.
        """
    def admin_revoke_roles(self, username: str, roles: list, policy: dict = ...) -> None:
        """
        Remove roles from a user.

        :param str user: the username to have the roles revoked.
        :param list roles: a list of role names.
        :param dict policy: optional the Aerospike Python client documentation.

        :raises: one of the AdminError subclasses.
        """
    def admin_set_password(self, username: str, password: str, policy: dict = ...) -> None:
        """
        Set the password of a user by a user administrator.

        :param str user: the username to be added to the aerospike cluster.
        :param str password: the password associated with the given username.
        :param dict policy: optional the Aerospike Python client documentation.

        :raises: one of the AdminError subclasses.
        """
    def admin_set_quotas(self, role: str, read_quota: int = ..., write_quota: int = ..., policy: dict = ...) -> None:
        """
        Add quotas to a role.

        :param str role: the name of the role.
        :param int read_quota: Maximum reads per second limit. Pass in ``0`` for no limit.
        :param int write_quota: Maximum write per second limit. Pass in ``0`` for no limit.
        :param dict policy: See the Aerospike Python client documentation.

        :raises: one of the AdminError subclasses.
        """
    def admin_set_whitelist(self, role: str, whitelist: list, policy: dict = ...) -> None:
        """
        Add a whitelist to a role.

        :param str role: The name of the role.
        :param list whitelist: List of IP strings the role is allowed to connect to.
            Setting this to None will clear the whitelist for that role.
        :param dict policy: See the Aerospike Python client documentation.

        :raises: One of the AdminError subclasses.
        """
    def append(self, key: tuple, bin: str, val: str, meta: dict = ..., policy: dict = ...) -> None:
        """
        Append a string to the string value in bin.

        :param tuple key: a the Aerospike Python client documentation tuple associated with the record.
        :param str bin: the name of the bin.
        :param str val: the string to append to the bin value.
        :param dict meta: record metadata to be set. See the Aerospike Python client documentation.
        :param dict policy: optional the Aerospike Python client documentation.

        :raises: a subclass of AerospikeError.
        """
    def apply(self, key: tuple, module: str, function: str, args: list, policy: dict = ...) -> Union[str, int, float, bytearray, list, dict]:
        """
        Apply a registered (see udf_put()) record UDF to a particular record.

        :param tuple key: a the Aerospike Python client documentation associated with the record.
        :param str module: the name of the UDF module.
        :param str function: the name of the UDF to apply to the record identified by *key*.
        :param list args: the arguments to the UDF.
        :param dict policy: optional the Aerospike Python client documentation.
        :return: the value optionally returned by the UDF, one of str,\
                 int, float, bytearray, list, dict.
        :raises: a subclass of AerospikeError.

          and `Developing Record UDFs <https://aerospike.com/docs/database/advanced/udf/modules/record/develop>`_.
        """
    def batch_apply(self, keys: list, module: str, function: str, args: list, policy_batch: dict = ..., policy_batch_apply: dict = ...) -> BatchRecords:
        """
        Apply UDF (user defined function) on multiple keys.

        :param list keys: The keys to operate on.
        :param str module: the name of the UDF module.
        :param str function: the name of the UDF to apply to the record identified by *key*.
        :param list args: the arguments to the UDF.
        :param dict policy_batch: See the Aerospike Python client documentation.
        :param dict policy_batch_apply: See the Aerospike Python client documentation.

        :return: an instance of BatchRecords <aerospike_helpers.batch.records>.
        :raises: A subclass of AerospikeError. See note above batch_write() for details.
        """
    def batch_operate(self, keys: list, ops: list, policy_batch: dict = ..., policy_batch_write: dict = ..., ttl: int = ...) -> BatchRecords:
        """
        Perform the same read/write operations on multiple keys.

            This bug was fixed in version 14.0.0.

        :param list keys: The keys to operate on.
        :param list ops: List of operations to apply.
        :param dict policy_batch: See the Aerospike Python client documentation.
        :param dict policy_batch_write: See the Aerospike Python client documentation.
        :param int ttl: The time-to-live (expiration) of each record in seconds.

        :return: an instance of BatchRecords <aerospike_helpers.batch.records>.

        :raises: A subclass of AerospikeError. See note above batch_write() for details.
        """
    def batch_remove(self, keys: list, policy_batch: dict = ..., policy_batch_remove: dict = ...) -> BatchRecords:
        """
        Remove multiple records by key.

        :param list keys: The keys to remove.
        :param dict policy_batch: Optional aerospike batch policy the Aerospike Python client documentation.
        :param dict policy_batch_remove: Optional aerospike batch remove policy the Aerospike Python client documentation.
        :return: an instance of BatchRecords <aerospike_helpers.batch.records>.
        :raises: A subclass of AerospikeError. See note above batch_write() for details.
        """
    def batch_read(self, keys: list, bins: list[str] = ..., policy: dict = ...) -> BatchRecords:
        """
        Read multiple records.

        If a list of bin names is not provided, return all the bins for each record.

        If a list of bin names is provided, return only these bins for the given list of records.

        If an empty list of bin names is provided, only the metadata of each record will be returned.
        Each ``BatchRecord.record`` in ``BatchRecords.batch_records`` will only be a 2-tuple ``(key, meta)``.

        :param list keys: The key tuples of the records to fetch.
        :param bins: List of bin names to fetch for each record.
        :type bins: list[str] or None
        :param dict policy: See the Aerospike Python client documentation.

        :return: an instance of BatchRecords <aerospike_helpers.batch.records>.

        :raises: A subclass of AerospikeError. See note above batch_write() for details.
        """
    def batch_write(self, batch_records: BatchRecords, policy_batch: dict = ...) -> BatchRecords:
        """
        Write/read multiple records for specified batch keys in one batch call.

        This method allows different sub-commands for each key in the batch.
        The resulting status and operated bins are set in ``batch_records.results`` and ``batch_records.record``.

        :param BatchRecords batch_records: A aerospike_helpers.batch.records.BatchRecords object used to specify the operations to carry out.
        :param dict policy_batch: aerospike batch policy the Aerospike Python client documentation.

        :return: A reference to the batch_records argument of type BatchRecords <aerospike_helpers.batch.records>.

        :raises: A subclass of AerospikeError. See note above batch_write() for details.

            batch helpers the Aerospike Python client documentation
        """
    def close(self) -> None:
        """
        Close all connections to the cluster. It is recommended to explicitly \
        call this method when the program is done communicating with the cluster.

        You may call Client.connect() again after closing the connection.

        Record Commands
        ---------------
        """
    def connect(self, username: str = ..., password: str = ...) -> Client:
        """
        If there is currently no connection to the cluster, connect to it. The optional *username* and *password* only
        apply when connecting to the Enterprise Edition of Aerospike.

        :param str username: a defined user with roles in the cluster. See admin_create_user().
        :param str password: the password will be hashed by the client using bcrypt.
        :raises: ClientError, for example when a connection cannot be \
                 established to a seed node (any single node in the cluster from which the client \
                 learns of the other nodes).

            Python client 5.0.0 and up will fail to connect to Aerospike server 4.8.x or older.
            If you see the error "-10, ‘Failed to connect’", please make sure you are using server 4.9 or later.
        """
    def exists(self, key: tuple, policy: dict = ...) -> tuple:
        """
        Check if a record with a given key exists in the cluster.

        Returns the record's key and metadata in a tuple.

        If the record does not exist, the tuple's metadata will be None.

        :param tuple key: a the Aerospike Python client documentation associated with the record.
        :param dict policy: see the Aerospike Python client documentation.

        :raises: a subclass of AerospikeError.
        """
    def get(self, key: tuple, policy: dict = ...) -> tuple:
        """
        Returns a record with a given key.

        :param tuple key: a the Aerospike Python client documentation associated with the record.
        :param dict policy: see the Aerospike Python client documentation.

        :return: a the Aerospike Python client documentation.

        :raises: RecordNotFound.
        """
    def get_stats(self) -> ClusterStats:
        """
        Retrieve aerospike client instance statistics.

        :return: an instance of aerospike_helpers.metrics.ClusterStats
        :raises: AerospikeError or one of its subclasses.
        """
    def get_cdtctx_base64(self, ctx: list) -> str:
        """
        Get the base64 representation of aerospike CDT ctx.

        See the Aerospike Python client documentation for more details on CDT context.

        :param list ctx: Aerospike CDT context: generated by aerospike CDT ctx helper aerospike_helpers.
        :raises: a subclass of AerospikeError.
        """
    # We cannot use aerospike_helpers's TypeExpression type because mypy's stubtest will complain
    def get_expression_base64(self, expression) -> str:
        """
        Get the base64 representation of a compiled aerospike expression.

        See the Aerospike Python client documentation for more details on expressions.

        :param TypeExpression expression: the compiled expression. See ``aerospike_helpers.expressions``.
        :raises: a subclass of AerospikeError.
        """
    def get_key_partition_id(self, ns: str, set: str, key: Any) -> int:
        """Get the partition ID for a key.
        Example:
        Args:
            ns: The namespace.
            set: The set name.
            key: The key.

        Returns:
            int: The partition ID.

        Raises:
        See Also:
        """
    def get_node_names(self) -> list:
        """
        Return the list of hosts and node names present in a connected cluster.

        :return: a list of node info dictionaries.
        :raises: a subclass of AerospikeError.
        """
    def get_nodes(self) -> list:
        """
        Return the list of hosts present in a connected cluster.

        :return: a list of node address tuples.
        :raises: a subclass of AerospikeError.
        """
    def increment(self, key: tuple, bin: str, offset: int, meta: dict = ..., policy: dict = ...) -> None:
        """
        Increment the integer value in *bin* by the integer *val*.

        :param tuple key: a the Aerospike Python client documentation tuple associated with the record.
        :param str bin: the name of the bin.
        :param int offset: the value by which to increment the value in *bin*.
        :type offset: int or float
        :param dict meta: record metadata to be set. See the Aerospike Python client documentation.
        :param dict policy: optional the Aerospike Python client documentation. Note: the ``exists`` policy option may not be: ``aerospike.POLICY_EXISTS_CREATE_OR_REPLACE`` nor ``aerospike.POLICY_EXISTS_REPLACE``
        :raises: a subclass of AerospikeError.
        """

    # Index creation for root-level bin values
    def index_geo2dsphere_create(self, ns: str, set: str, bin: str, name: str, policy: dict = ...) -> None:
        """
        Deprecated: 19.1.0 index_single_value_create() should be used instead.

        Create a geospatial 2D spherical index with *name* on the *bin* \
        in the specified *ns*, *set*.

        :param str ns: the namespace in the aerospike cluster.
        :param str set: the set name.
        :param str bin: the name of bin the secondary index is built on.
        :param str name: the name of the index.
        :param dict policy: optional the Aerospike Python client documentation.
        :raises: a subclass of AerospikeError.
        """
    def index_integer_create(self, ns: str, set: str, bin: str, name: str, policy: dict = ...) -> None:
        """
        Deprecated: 19.1.0 index_single_value_create() should be used instead.

        Create an integer index with *name* on the *bin* in the specified \
        *ns*, *set*.

        :param str ns: the namespace in the aerospike cluster.
        :param str set: the set name.
        :param str bin: the name of bin the secondary index is built on.
        :param str name: the name of the index.
        :param dict policy: optional the Aerospike Python client documentation.
        :raises: a subclass of AerospikeError.
        """
    def index_string_create(self, ns: str, set: str, bin: str, name: str, policy: dict = ...) -> None:
        """
        Deprecated: 19.1.0 index_single_value_create() should be used instead.

        Create a string index with *index_name* on the *bin* in the specified \
        *ns*, *set*.

        :param str ns: the namespace in the aerospike cluster.
        :param str set: the set name.
        :param str bin: the name of bin the secondary index is built on.
        :param str name: the name of the index.
        :param dict policy: optional the Aerospike Python client documentation.
        :raises: a subclass of AerospikeError.
        """
    def index_blob_create(self, ns: str, set: str, bin: str, name: str, policy: dict = ...) -> None:
        """
        Deprecated: 19.1.0 index_single_value_create() should be used instead.

        Create a blob index with *name* on the *bin* in the specified \
        *ns*, *set*.

        :param str ns: the namespace in the aerospike cluster.
        :param str set: the set name.
        :param str bin: the name of bin the secondary index is built on.
        :param str name: the name of the index.
        :param dict policy: optional the Aerospike Python client documentation.
        :raises: a subclass of AerospikeError.
        """

    # We cannot use aerospike_helpers's TypeExpression type because mypy's stubtest will complain
    def index_single_value_create(self, ns: str, set: str, bin: str, index_datatype: int, name: str, policy: dict = ..., ctx: Optional[list] = ...) -> None:
    def index_single_value_create(self, ns: str, set: str, bin: str, index_datatype: int, name: str, policy: dict = ..., ctx: Optional[list] = ...) -> None: ...
    def index_list_create(self, ns: str, set: str, bin: str, index_datatype: int, name: str, policy: dict = ..., ctx: Optional[list] = ...) -> None: ...
    def index_map_keys_create(self, ns: str, set: str, bin: str, index_datatype: int, name: str, policy: dict = ..., ctx: Optional[list] = ...) -> None: ...
    def index_map_values_create(self, ns: str, set: str, bin: str, index_datatype: int, name: str, policy: dict = ..., ctx: Optional[list] = ...) -> None: ...
    def index_set_create(self, ns: str, set: str, name: str, policy: dict = ...) -> None: ...

    """Create a single-value index.
        Example:
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
        See Also:
        """
    def index_list_create(self, ns: str, set: str, bin: str, index_datatype: int, name: str, policy: dict = ..., ctx: Optional[list] = ...) -> None:
        """
        Create a secondary index for all of a list's values, where all the values are the same type.

        :param str ns: the namespace in the aerospike cluster.
        :param str set: the set name.
        :param str bin: the name of bin the secondary index is built on.
        :param int index_datatype: the type of the values being indexed. See the Aerospike Python client documentation.
        :param str name: the name of the index.
        :param dict policy: optional the Aerospike Python client documentation.
        :param dict | None ctx: an optional list of contexts produced by aerospike_helpers.cdt_ctx methods. Defaults to None.
        :raises: a subclass of AerospikeError.
        """
    def index_map_keys_create(self, ns: str, set: str, bin: str, index_datatype: int, name: str, policy: dict = ..., ctx: Optional[list] = ...) -> None:
        """
        Create a secondary index on all of a map's keys, where all of the keys are the same type.

        :param str ns: the namespace in the aerospike cluster.
        :param str set: the set name.
        :param str bin: the name of bin the secondary index is built on.
        :param int index_datatype: the type of the values being indexed. See the Aerospike Python client documentation.
        :param str name: the name of the index.
        :param dict policy: optional the Aerospike Python client documentation.
        :param dict | None ctx: an optional list of contexts produced by aerospike_helpers.cdt_ctx methods. Defaults to None.
        :raises: a subclass of AerospikeError.
        """
    def index_map_values_create(self, ns: str, set: str, bin: str, index_datatype: int, name: str, policy: dict = ..., ctx: Optional[list] = ...) -> None:
        """
        Create a secondary index on all of a map's values, where all of the values are the same type.

        :param str ns: the namespace in the aerospike cluster.
        :param str set: the set name.
        :param str bin: the name of bin the secondary index is built on.
        :param int index_datatype: the type of the values being indexed. See the Aerospike Python client documentation.
        :param str name: the name of the index.
        :param dict policy: optional the Aerospike Python client documentation.
        :param dict | None ctx: an optional list of contexts produced by aerospike_helpers.cdt_ctx methods. Defaults to None.
        :raises: a subclass of AerospikeError.
        """

    def index_cdt_create(self, ns: str, set: str, bin: str, index_type: int, index_datatype: int, name: str, ctx: list, policy: dict = ...) -> int:
        """
        Deprecated: 19.1.0 Use the other non-deprecated index methods to create an index with a list of contexts.

        Create an collection data type (CDT) index named *index_name* for a scalar, list values, map keys, or map values (as defined by *index_type*) and for
        numeric, string, or GeoJSON values (as defined by *index_datatype*)
        on records of the specified *ns*, *set* whose bin is a list or map.

        :param str ns: the namespace in the aerospike cluster.
        :param str set: the set name.
        :param str bin: the name of bin the secondary index is built on.
        :param index_type: whether we are querying a single scalar value or specific values of a CDT type. See the Aerospike Python client documentation.
        :param index_datatype: the type of value being queried on. See the Aerospike Python client documentation.
        :param str index_name: the name of the index.
        :param dict ctx: a list of contexts produced by aerospike_helpers.cdt_ctx methods.
        :param dict policy: optional the Aerospike Python client documentation.
        :raises: a subclass of AerospikeError.
        """
    def index_expr_create(self, ns: str, set: str, index_type: int, index_datatype: int, expressions: list, name: str, policy: dict = ...) -> None:
        """
        Create secondary index on an expression.

        :param str ns: The namespace to be indexed.
        :param str set: The set to be indexed.
        :param index_type: See the Aerospike Python client documentation for possible values.
        :param index_datatype: See the Aerospike Python client documentation for possible values.
        :param list expressions: The compiled expression to be indexed. Produced from the Aerospike Python client documentation.
        :param str name: the name of the index.
        :param dict policy: optional the Aerospike Python client documentation.
        :raises: a subclass of AerospikeError.
        """

    def index_remove(self, ns: str, name: str, policy: dict = ...) -> None:
        """
        Remove the index with *name* from the namespace.

        :param str ns: the namespace in the aerospike cluster.
        :param str name: the name of the index.
        :param dict policy: optional the Aerospike Python client documentation.
        :raises: a subclass of AerospikeError.
        """

    def info_all(self, command: str, policy: dict = ...) -> dict:
        """
        Send an info command to all nodes in the cluster to which the client is connected.

        If any of the individual requests fail, this will raise an exception.

        :param str command: see `Info Command Reference <https://aerospike.com/docs/database/reference/info>`_.
        :param dict policy: optional the Aerospike Python client documentation.

        :raises: a subclass of AerospikeError.
        """
    def info_random_node(self, command: str, policy: dict = ...) -> str:
        """
        Send an info *command* to a single random node.

        :param str command: the info command. See `Info Command Reference <https://aerospike.com/docs/database/reference/info>`_.
        :param dict policy: optional the Aerospike Python client documentation.

        :raises: a subclass of AerospikeError.
        """
    def info_single_node(self, command: str, host: str, policy: dict = ...) -> str:
        """
        Send an info *command* to a single node specified by *host name*.

        :param str command: the info command. See `Info Command Reference <https://aerospike.com/docs/database/reference/info>`_.
        :param str host: a node name. Example: 'BCER199932C'
        :param dict policy: optional the Aerospike Python client documentation.

        :raises: a subclass of AerospikeError.
        """
    def is_connected(self) -> bool:
        """
        Tests the connections between the client and the nodes of the cluster.
        If the result is ``False``, the client will require another call to
        Client.connect().
        """
    def job_info(self, job_id: int, module: int, policy: dict = ...) -> dict:
        """
        Return the status of a job running in the background.

        The returned dict contains these keys:

            * ``"status"``: see the Aerospike Python client documentation for possible values.
            * ``"records_read"``: number of scanned records.
            * ``"progress_pct"``: progress percentage of the job

        :param int job_id: the job ID returned by scan_apply() or query_apply().
        :param module: one of the Aerospike Python client documentation.
        :param policy: optional the Aerospike Python client documentation.
        :returns: dict
        :raises: a subclass of AerospikeError.
        """
    def enable_metrics(self, policy: Optional[MetricsPolicy] = None) -> None:
        """
        Enable extended periodic cluster and node latency metrics.

        :param MetricsPolicy policy: Optional metrics policy

        :raises: AerospikeError or one of its subclasses.
        """
    def disable_metrics(self) -> None:
        """
        Disable extended periodic cluster and node latency metrics.

        :raises: AerospikeError or one of its subclasses.

        Scan and Query Constructors
        ---------------------------
        """
    def operate(self, key: tuple, list: list, meta: dict = ..., policy: dict = ...) -> tuple:
        """
        Lookup a record by key, then perform specified operations.

        Starting with Aerospike server version 3.6.0, non-existent bins are not present in the returned the Aerospike Python client documentation. \
        The returned record tuple will only contain one element per bin, even if multiple operations were performed on the bin. \
        (In Aerospike server versions prior to 3.6.0, non-existent bins being read will have a \
        None value. )

        :param tuple key: a the Aerospike Python client documentation associated with the record.
        :param list list: See the Aerospike Python client documentation.
        :param dict meta: record metadata to be set. See the Aerospike Python client documentation.
        :param dict policy: optional the Aerospike Python client documentation.
        :return: a the Aerospike Python client documentation.
        :raises: a subclass of AerospikeError.

            operate() can now have multiple write operations on a single
            bin.
        """
    def operate_ordered(self, key: tuple, list: list, meta: dict = ..., policy: dict = ...) -> list:
        """
        Lookup a record by key, then perform specified operations. \
        The results will be returned as a list of (bin-name, result) tuples. The order of the \
        elements in the list will correspond to the order of the operations \
        from the input parameters.

        Write operations or read operations that fail will not return a ``(bin-name, result)`` tuple.

        :param tuple key: a the Aerospike Python client documentation associated with the record.
        :param list list: See the Aerospike Python client documentation.
        :param dict meta: record metadata to be set. See the Aerospike Python client documentation.
        :param dict policy: optional the Aerospike Python client documentation.

        :return: a the Aerospike Python client documentation.
        :raises: a subclass of AerospikeError.
        """
    def prepend(self, key: tuple, bin: str, val: str, meta: dict = ..., policy: dict = ...) -> None:
        """
        Prepend the string value in *bin* with the string *val*.

        :param tuple key: a the Aerospike Python client documentation tuple associated with the record.
        :param str bin: the name of the bin.
        :param str val: the string to prepend to the bin value.
        :param dict meta: record metadata to be set. See the Aerospike Python client documentation.
        :param dict policy: optional the Aerospike Python client documentation.

        :raises: a subclass of AerospikeError.
        """
    def put(self, key: tuple, bins: dict, meta: dict = ..., policy: dict = ..., serializer = ...) -> None:
        """
        Create a new record, or remove / add bins to a record.

        :param tuple key: a the Aerospike Python client documentation associated with the record.
        :param dict bins: contains bin name-value pairs of the record.
        :param dict meta: record metadata to be set. see the Aerospike Python client documentation.
        :param dict policy: see the Aerospike Python client documentation.

        :param serializer: override the serialization mode of the client \
            with one of the the Aerospike Python client documentation.
            To use a class-level, user-defined serialization function registered with aerospike.set_serializer(), \
            use aerospike.SERIALIZER_USER.

        :raises: a subclass of AerospikeError.

        Example:
        """
    def query(self, namespace: str, set: Optional[str] = None) -> Query:
        """
        Return a aerospike.Query object to be used for executing queries
        over a specified set in a namespace.

        See the Aerospike Python client documentation for more details.

        :param str namespace: the namespace in the aerospike cluster.
        :param str set: optional specified set name. Otherwise, all records in the specified namespace will be queried.
        :return: an aerospike.Query class.
        """
    def query_apply(self, ns: str, set: str, predicate: tuple, module: str, function: str, args: list = ..., policy: dict = ...) -> int:
        """
        Initiate a query and apply a record UDF to each record matched by the query.

        This method blocks until the query is complete.

        :param str ns: the namespace in the aerospike cluster.
        :param str set: the set name. Should be None if you want to query records in the *ns* which are in no set.
        :param tuple predicate: the `tuple` produced by one of the aerospike.predicates methods.
        :param str module: the name of the UDF module.
        :param str function: the name of the UDF to apply to the records matched by the query.
        :param list args: the arguments to the UDF.
        :param dict policy: optional dictionary that takes in both the Aerospike Python client documentation and the Aerospike Python client documentation.

        :return: a job ID that can be used with job_info() to check the status of the ``aerospike.JOB_QUERY``.
        :raises: a subclass of AerospikeError.
        """
    def remove(self, key: tuple, meta: dict = ..., policy: dict = ...) -> None:
        """
        Remove a record matching the *key* from the cluster.

            Deprecated the ``meta`` parameter. Use the policy parameter to set ``gen`` instead.

        :param tuple key: a the Aerospike Python client documentation associated with the record.
        :param dict meta: contains the expected generation of the record in a key called ``"gen"``.
        :param dict policy: see the Aerospike Python client documentation. May be passed as a keyword argument.

        :raises: a subclass of AerospikeError.
        """
    def remove_bin(self, key: tuple, list: list, meta: dict = ..., policy: dict = ...) -> None:
        """
        Remove a list of bins from a record with a given *key*. Equivalent to \
        setting those bins to aerospike.null() with a Client.put().

        :param tuple key: a the Aerospike Python client documentation associated with the record.
        :param list list: the bins names to be removed from the record.
        :param dict meta: record metadata to be set. See the Aerospike Python client documentation.
        :param dict policy: optional the Aerospike Python client documentation.

        :raises: a subclass of AerospikeError.
        """
    def scan(self, namespace: str, set: Optional[str] = None) -> Scan:
        """
        Deprecated: 7.0.0 aerospike.Query should be used instead.

        Returns a aerospike.Scan object to scan all records in a namespace / set.

        If set is omitted or set to None, the object returns all records in the namespace.

        :param str namespace: the namespace in the aerospike cluster.
        :param str set: optional specified set name, otherwise the entire \
            *namespace* will be scanned.

        :return: an aerospike.Scan class.
        """
    def scan_apply(self, ns: str, set: str, module: str, function: str, args: list = ..., policy: dict = ..., options: dict = ...) -> int:
        """
        Deprecated: 7.0.0 aerospike.Query should be used instead.

        Initiate a scan and apply a record UDF to each record matched by the scan.

        This method blocks until the scan is complete.

        :param str ns: the namespace in the aerospike cluster.
        :param str set: the set name. Should be None if the entire namespace is to be scanned.
        :param str module: the name of the UDF module.
        :param str function: the name of the UDF to apply to the records matched by the scan.
        :param list args: the arguments to the UDF.
        :param dict policy: optional dictionary that takes in both the Aerospike Python client documentation and the Aerospike Python client documentation.
        :param dict options: the the Aerospike Python client documentation that will apply to the scan.

        :return: a job ID that can be used with job_info() to check the status of the ``aerospike.JOB_SCAN``.
        :raises: a subclass of AerospikeError.
        """
    def select(self, *args, **kwargs) -> tuple:
        """
        Returns specific bins of a record.

        If a bin does not exist, it will not show up in the returned the Aerospike Python client documentation.

        :param tuple key: a the Aerospike Python client documentation associated with the record.
        :param list bins: a list of bin names to select from the record.
        :param dict policy: optional the Aerospike Python client documentation.

        :return: a the Aerospike Python client documentation.

        :raises: RecordNotFound.
        """
    # We cannot use aerospike_helpers's TypeExpression type because mypy's stubtest will complain
    def set_xdr_filter(self, data_center: str, namespace: str, expression_filter, policy: dict = ...) -> str:
        """
        Set the cluster's xdr filter using an Aerospike expression.

        The cluster's current filter can be removed by setting expression_filter to None.

        :param str data_center: The data center to apply the filter to.
        :param str namespace: The namespace to apply the filter to.
        :param TypeExpression expression_filter: The compiled expression filter to set. See ``aerospike_helpers.expressions``.
        :param dict policy: optional the Aerospike Python client documentation.
        :raises: a subclass of AerospikeError.

        Warning: Requires Aerospike server version >= 5.3.
        """
    def shm_key(self) -> Union[int, None]:
        """
        Expose the value of the shm_key for this client if shared-memory cluster tending is enabled,
        """
    def touch(self, key: tuple, val: int = ..., meta: dict = ..., policy: dict = ...) -> None:
        """
        Touch the given record, setting its time-to-live and incrementing its generation.

            Deprecated the ``meta["ttl"]`` parameter. Use the ``val`` parameter instead.

        :param tuple key: a the Aerospike Python client documentation associated with the record.
        :param int val: ttl in seconds, with ``0`` resolving to the default value in the server config.
        :param dict meta: record metadata to be set. see the Aerospike Python client documentation
        :param dict policy: see the Aerospike Python client documentation.

        :raises: a subclass of AerospikeError.
        """
    def truncate(self, namespace: str, set: str, nanos: int, policy: dict = ...) -> int:
        """
        Remove all records in the namespace / set whose last updated time is older than the given time.

        This method is many orders of magnitude faster than deleting records one at a time.
        See `Truncate command reference <https://aerospike.com/docs/database/reference/info#truncate>`_.

        This asynchronous server call may return before the truncation is complete.  The user can still
        write new records after the server returns because new records will have last update times
        greater than the truncate cutoff (set at the time of truncate call)

        :param str namespace: The namespace to truncate.
        :param str set: The set to truncate. Pass in None to truncate a namespace instead.
        :param int nanos:  A cutoff threshold where records last updated before the threshold will be removed.
            Units are in nanoseconds since the UNIX epoch ``(1970-01-01)``.
            A value of ``0`` indicates that all records in the set should be truncated regardless of update time.
            The value must not be in the future.
        :param dict policy: See the Aerospike Python client documentation.
        :return: Status indicating the success of the operation.

        :raises: a subclass of AerospikeError.
        """
    def udf_get(self, module: str, language: int = ..., policy: dict = ...) -> str:
        """
        Return the content of a UDF module which is registered with the cluster.

        :param str module: the UDF module to read from the cluster.
        :param int language: aerospike.UDF_TYPE_LUA
        :param dict policy: currently **timeout** in milliseconds is the available policy.

        :raises: a subclass of AerospikeError.
        """
    def udf_list(self, policy: dict = ...) -> list:
        """
        Return the list of UDF modules registered with the cluster.

        :param dict policy: currently **timeout** in milliseconds is the available policy.

        :raises: a subclass of AerospikeError.
        """
    def udf_put(self, filename: str, udf_type = ..., policy: dict = ...) -> None:
        """
        Register a UDF module with the cluster.

        This waits for the UDF to be added to all nodes in the server before returning.

        :param str filename: the path to the UDF module to be registered with the cluster.
        :param int udf_type: aerospike.UDF_TYPE_LUA.
        :param dict policy: currently **timeout** in milliseconds is the available policy.
        :raises: a subclass of AerospikeError.

        To run this example, do not run the boilerplate code.
        """
    def udf_remove(self, module: str, policy: dict = ...) -> None:
        """
        Remove a previously registered UDF module from the cluster.

        This waits for the UDF to be removed from the server completely before returning.

        :param str module: the UDF module to be deregistered from the cluster.
        :param dict policy: currently **timeout** in milliseconds is the available policy.
        :raises: a subclass of AerospikeError.
        """
    def commit(self, transaction: Transaction) -> int:
        """
        Attempt to commit the given transaction. First, the expected record versions are
        sent to the server nodes for verification. If all nodes return success, the transaction is
        committed. Otherwise, the transaction is aborted.

        Requires server version 8.0+

        :param transaction: Transaction.
        :type transaction: aerospike.Transaction
        :return: The status of the commit. One of the Aerospike Python client documentation.
        """
    def abort(self, transaction: Transaction) -> int:
        """
        Abort and rollback the given transaction.

        Requires server version 8.0+

        :param transaction: Transaction.
        :type transaction: aerospike.Transaction
        :return: The status of the abort. One of the Aerospike Python client documentation.
        """

class GeoJSON:
    """
    Starting with version ``3.7.0``, the Aerospike server supports storing GeoJSON data.
    A Geo2DSphere index can be built on a bin which contains GeoJSON data,
    which allows queries for points inside any given shapes using:

    * aerospike.predicates.geo_within_geojson_region()
    * aerospike.predicates.geo_within_radius()

    It also enables queries for regions that contain a given point using:

    * aerospike.predicates.geo_contains_geojson_point()
    * aerospike.predicates.geo_contains_point()

    On the client side, wrapping geospatial data in an instance of the
    aerospike.GeoJSON class enables serialization of the data into the
    correct type during a write operation, such as in Client.put().

    When reading a record from the server, bins with geospatial data will be
    deserialized into a aerospike.GeoJSON instance.

        `Geospatial Index and Query
        <https://aerospike.com/docs/develop/data-types/geospatial/>`_.

    See also: https://aerospike.com/docs/develop/data-types/geospatial/
    """
    geo_data: Any
    def __init__(self, geo_data: Union[str, dict] = ...) -> None:
        """Initialize a GeoJSON object with a str or a dict of geospatial data.
        Example:
        Args:
            geo_data: A GeoJSON str or a dict of geospatial data.

        Returns:
            None.

        Raises:
        See Also:
        """
    def dumps(self) -> str:
        """
        Gets the geospatial data contained in the aerospike.GeoJSON class as a GeoJSON string.

        :return: a GeoJSON str representing the geospatial data.
        """
    def loads(self, raw_geo: str) -> None:
        """
        Sets the geospatial data of the aerospike.GeoJSON wrapper class from a GeoJSON string.

        :param str raw_geo: a GeoJSON string representation.
        """
    def unwrap(self) -> dict:
        """
        Gets the geospatial data contained in the aerospike.GeoJSON class.

        :return: a dict representing the geospatial data.
        """
    def wrap(self, geo_data: dict) -> None:
        """
        Sets the geospatial data of the aerospike.GeoJSON wrapper class.

        :param dict geo_data: a dict representing the geospatial data.
        """

class KeyOrderedDict(dict):
    """
    The KeyOrderedDict class is a dictionary that directly maps to a key ordered map on the Aerospike server.
    This assists in matching key ordered maps through various read operations. See the example snippet below.
    """
    def __init__(self, *args, **kwargs) -> None:
        """Initialize a KeyOrderedDict.
        Example:
        Args:
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.

        Returns:
            None.

        Raises:
        See Also:
        """

class Query:
    """The query object is used for executing queries over a secondary index of a specified set.
    Example:
    Attributes:
        max_records: Approximate number of records to return to client.
        records_per_second: Limit the scan to process records at records_per_second.
        ttl: The time-to-live (expiration) of the record in seconds.

    Args:
    Returns:
    Raises:
    See Also:
        `Queries <https://aerospike.com/docs/develop/learn/queries/>`_
    """
    max_records: int
    records_per_second: int
    ttl: int
    def __init__(self, *args, **kwargs) -> None:
        """Initialize the Query class.
        Example:
        Args:
            *args: Arbitrary positional arguments.
            **kwargs: Arbitrary keyword arguments.

        Returns:
            None.

        Raises:
        See Also:
        """
    def add_ops(self, ops: list) -> None:
        """
        Warning: In the next major client release, if this is called after ~Query.select() was called on the same object, an ParamError will be raised.

        Add a list of operations to the query.

        For background queries, only write operations are allowed.
        For foreground queries, only read operations are allowed.

        For server versions < 8.1.2, basic read operations are allowed in foreground queries. Otherwise with this
        server version, using a non-basic read operation will raise a ParamError.

        If no predicate is attached to the Query it will apply ops to all the records in the specified set.

        If there are selected bins in this Query object via ~Query.select(), those selected bins will be ignored
        during the query.

        :param ops: `list` A list of operations generated from the Aerospike Python client documentation.

            Requires server version >= 4.7.0.
        """
    def apply(self, module: str, function: str, arguments: list = ...) -> Any:
        """
        Aggregate the results() using a stream \
        `UDF <https://aerospike.com/docs/database/learn/architecture/udf/#stream-udfs>`_. If no \
        predicate is attached to the  aerospike.Query the stream UDF \
        will aggregate over all the records in the specified set.

        This function can also be used to apply a record UDF.

        :param str module: the name of the Lua module.
        :param str function: the name of the Lua function within the *module*.
        :param list arguments: optional arguments to pass to the *function*. NOTE: these arguments must be types supported by Aerospike See: `supported data types <https://aerospike.com/docs/develop/client/python/data-types/>`_.
            If you need to use an unsupported type, (e.g. set or tuple) you must use your own serializer.

        Example: find the first name distribution of users who are 21 or older using \
        a query aggregation:

        Assume the example code above is in a file called "example.lua", and is the same folder as the following script.

        With stream UDFs, the final reduce steps (which ties
        the results from the reducers of the cluster nodes) executes on the
        client-side. Explicitly setting the Lua ``user_path`` in the
        config helps the client find the local copy of the module
        containing the stream UDF. The ``system_path`` is constructed when
        the Python package is installed, and contains system modules such
        as ``aerospike.lua``, ``as.lua``, and ``stream_ops.lua``.
        The default value for the Lua ``system_path`` is
        ``/usr/local/aerospike/lua``.
        """
    def execute_background(self, policy: dict = ...) -> int:
        """
        Execute a record UDF or write operations on records found by the query in the background. This method returns before the query has completed.
        A UDF or a list of write operations must have been added to the query with Query.apply() or Query.add_ops() respectively.

        :param dict policy: optional the Aerospike Python client documentation.

        :return: a job ID that can be used with Client.job_info() to track the status of the ``aerospike.JOB_QUERY`` , as it runs in the background.

                operations.increment("score", 100)
            ]
            query.add_ops(ops)
            id = query.execute_background()

            # Allow time for query to complete
            import time
            time.sleep(3)

            for key in keyTuples:
                _, _, bins = client.get(key)
                print(bins)
            # {"score": 200, "elo": 1400}
            # {"score": 120, "elo": 1500}
            # {"score": 110, "elo": 1100}
            # {"score": 300, "elo": 900}

            # EXAMPLE 2: Increase score by 100 again for those with elos > 1000
            # Use write policy to select players by elo
            import aerospike_helpers.expressions as expr
            eloGreaterOrEqualTo1000 = expr.GE(expr.IntBin("elo"), 1000).compile()
            writePolicy = {
                "expressions": eloGreaterOrEqualTo1000
            }
            id = query.execute_background(policy=writePolicy)

            time.sleep(3)

            for i, key in enumerate(keyTuples):
                _, _, bins = client.get(key)
                print(bins)
            # {"score": 300, "elo": 1400} <--
            # {"score": 220, "elo": 1500} <--
            # {"score": 210, "elo": 1100} <--
            # {"score": 300, "elo": 900}

            # Cleanup and close the connection to the Aerospike cluster.
            for key in keyTuples:
                client.remove(key)
            client.close()
        """
    def foreach(self, callback: Callable, policy: dict = ..., options: dict = ...) -> None:
        """
        Invoke the *callback* function for each of the records streaming back from the query.

        A the Aerospike Python client documentation is passed as the argument to the callback function.
        If the query is using the "partition_filter" query policy the callback will receive two arguments
        The first is a int representing partition id, the second is the same the Aerospike Python client documentation
        as a normal callback.

        :param typing.Callable callback: the function to invoke for each record.
        :param dict policy: optional the Aerospike Python client documentation.
        :param dict options: optional the Aerospike Python client documentation.

         "partition_filter" see the Aerospike Python client documentation can be used to specify which partitions/records
         foreach will query. See the example below.

                print(part_id)
                partitions.append(part_id)

            query = client.query("test", "demo")

            policy = {
                "partition_filter": {
                    "begin": 1000,
                    "count": 4
                },
            }

            query.foreach(callback, policy)

            # NOTE that these will only be non 0 if there are records in partitions 1000 - 1003
            # should be 4
            print(len(partitions))

            # should be [1000, 1001, 1002, 1003]
            print(partitions)
        """
    def get_partitions_status(self) -> tuple:
        """
        Get this query instance's partition status. That is which partitions have been queried and which have not.
        If the query instance is not tracking its partitions, the returned dict will be empty.

            A query instance must have had .paginate() called on it, or been used with a partition filter, in order retrieve its
            partition status. If .paginate() was not called, or partition_filter was not used, the query instance will not save partition status.

        :return: See the Aerospike Python client documentation for a description of the partition status return value.

                global recordCount
                if recordCount == 2:
                    return False
                recordCount += 1

                print(record)

            # Query is set to read ALL records
            query = client.query("test", "demo")
            query.paginate()
            query.foreach(callback)
            # (('test', 'demo', None, bytearray(b'...')), {'ttl': 2591996, 'gen': 1}, {'score': 10, 'elo': 1100})
            # (('test', 'demo', None, bytearray(b'...')), {'ttl': 2591996, 'gen': 1}, {'score': 20, 'elo': 1500})

            # Use this to resume query where we left off
            partition_status = query.get_partitions_status()

            # Callback must include partition_id parameter
            # if partition_filter is included in policy
            def resume_callback(partition_id, record):
                print(partition_id, "->", record)

            policy = {
                "partition_filter": {
                    "partition_status": partition_status
                },
            }

            query.foreach(resume_callback, policy)
            # 1096 -> (('test', 'demo', None, bytearray(b'...')), {'ttl': 2591996, 'gen': 1}, {'score': 100, 'elo': 1400})
            # 3690 -> (('test', 'demo', None, bytearray(b'...')), {'ttl': 2591996, 'gen': 1}, {'score': 200, 'elo': 900})
        """
    def is_done(self) -> bool:
        """
        If using query pagination, did the previous paginated or partition_filter query using this query instance return all records?

        :return: A bool signifying whether this paginated query instance has returned all records.
        """
    def paginate(self) -> None:
        """
        Makes a query instance a paginated query.
        Call this if you are using the max_records and you need to query data in pages.

            Calling .paginate() on a query instance causes it to save its partition state.
            This can be retrieved later using .get_partitions_status(). This can also been done by
            using the partition_filter policy.

                records = query.results()
                print("got page: " + str(page))

                # Print records in each page
                for record in records:
                    print(record)

                if query.is_done():
                    print("all done")
                    break
            # got page: 0
            # (('test', 'demo', None, bytearray(b'HD\xd1\xfa$L\xa0\xf5\xa2~\xd6\x1dv\x91\x9f\xd6\xfa\xad\x18\x00')), {'ttl': 2591996, 'gen': 1}, {'score': 20, 'elo': 1500})
            # (('test', 'demo', None, bytearray(b'f\xa4\t"\xa9uc\xf5\xce\x97\xf0\x16\x9eI\xab\x89Q\xb8\xef\x0b')), {'ttl': 2591996, 'gen': 1}, {'score': 10, 'elo': 1100})
            # got page: 1
            # (('test', 'demo', None, bytearray(b'\xb6\x9f\xf5\x7f\xfarb.IeaVc\x17n\xf4\x9b\xad\xa7T')), {'ttl': 2591996, 'gen': 1}, {'score': 200, 'elo': 900})
            # (('test', 'demo', None, bytearray(b'j>@\xfe\xe0\x94\xd5?\n\xd7\xc3\xf2\xd7\x045\xbc*\x07 \x1a')), {'ttl': 2591996, 'gen': 1}, {'score': 100, 'elo': 1400})
            # got page: 2
            # all done
        """
    def results(self, policy: dict = ..., options: dict = ...) -> list:
        """
        Buffer the records resulting from the query, and return them as a \
        list of records.

        :param dict policy: optional the Aerospike Python client documentation.
        :param dict options: optional the Aerospike Python client documentation.
        :return: a list of the Aerospike Python client documentation.

            "partition_filter" see the Aerospike Python client documentation can be used to specify which partitions/records
            results will query. See the example below.

                # This is an example of querying partitions 1000 - 1003.
                import aerospike

                query = client.query("test", "demo")

                policy = {
                    "partition_filter": {
                        "begin": 1000,
                        "count": 4
                    },
                }

            # NOTE that these will only be non 0 if there are records in partitions 1000 - 1003
            # results will be the records in partitions 1000 - 1003
            results = query.results(policy=policy)
        """
    # TODO: this isn't an infinite list of bins
    def select(self, *args, **kwargs) -> None:
        """
        Warning: In the next major client release, calling this method after Query.add_ops() was called on the same Query object will raise a :pyParamError exception.

        Set a filter on the record bins resulting from results() or foreach().

        If this method is called more than once on the same query instance, a :pyClientError exception will be raised.

        If a selected bin does not exist in a record, it will not appear in the *bins* portion of that record tuple.

        If this method is called after Query.add_ops() was called on the same Query object, the selected bins in
        this call will be ignored during the query.
        """
    def where(self, predicate: tuple, ctx: list = ...) -> None:
        """
        Set a where *predicate* for the query.

        You can only assign at most one predicate to the query.
        If this method is called more than once on the same query instance, a :pyClientError exception will be raised.

        If this function isn't called, the query will behave similar to aerospike.Scan.

        :param tuple predicate: the tuple produced by either aerospike.predicates.equals() or aerospike.predicates.between().
        :param list ctx: the list produced by one of the aerospike_helpers.cdt_ctx methods.
        """
    # We cannot use aerospike_helpers's TypeExpression type because mypy's stubtest will complain
    def where_with_expr(self, expr, predicate: tuple) -> Query:
        """
        Add an expression *predicate* to the query.

        Predicate must have the bin name set to None.

        You can only assign at most one predicate to the query.

        :param TypeExpression | str expr:
            Compiled aerospike expressions produced from the Aerospike Python client documentation.
            Alternatively, you can pass in a base64 encoded string of an expression returned from asinfo when printing
            a list of secondary indexes based on expressions in the server.

        :param tuple predicate: the tuple produced from aerospike.predicates
        """
    def where_with_index_name(self, index_name: str, predicate: tuple) -> Query:
        """
        Add an index name *predicate* to the query.

        Predicate must have the bin name set to None.

        You can only assign at most one predicate to the query.

        :param str index_name: The name of the index.
        :param tuple predicate: the tuple produced from aerospike.predicates
        """

class Scan:
    """The Scan object is used to return all the records in a specified set.
    Example:
    Attributes:
        ttl: The time-to-live (expiration) of the record in seconds.

    Args:
    Returns:
    Raises:
    See Also:
        :class:`Query`: Query class.
    """
    ttl: int
    def __init__(self, *args, **kwargs) -> None:
        """Initialize the Scan class.
        Example:
        Args:
            *args: Arbitrary positional arguments.
            **kwargs: Arbitrary keyword arguments.

        Returns:
            None.

        Raises:
        See Also:
        """
    def add_ops(self, ops: list) -> None:
        """
        Add a list of write ops to the scan.
        When used with Scan.execute_background() the scan will perform the write ops on any records found.
        If no predicate is attached to the scan it will apply ops to all the records in the specified set. See aerospike_helpers for available ops.

        :param ops: `list` A list of write operations generated by the aerospike_helpers e.g. list_operations, map_operations, etc.

            Requires server version >= 4.7.0.

                operations.append(test_bin, 'val_to_append'),
                list_operations.list_remove_by_index(test_bin, list_index_to_remove, aerospike.LIST_RETURN_NONE)
            ]
            scan.add_ops(ops)

            id = scan.execute_background()
            client.close()

        For a more comprehensive example, see using a list of write ops with Query.execute_background() .
        """
    def apply(self, module: str, function: str, arguments: list = ...) -> Any:
        """
        Apply a record UDF to each record found by the scan \
        `User-defined functions (UDFs) <https://aerospike.com/docs/database/learn/architecture/udf/>`_.

        :param str module: the name of the Lua module.
        :param str function: the name of the Lua function within the *module*.
        :param list arguments: optional arguments to pass to the *function*. NOTE: these arguments must be types supported by Aerospike See: `supported data types <https://aerospike.com/docs/develop/data-types/scalar>`_.
            If you need to use an unsupported type, (e.g. set or tuple) you must use your own serializer.
        :return: one of the supported types, int, str, float (double), list, dict (map), bytearray (bytes), bool.
        """
    def foreach(self, callback: Callable, policy: dict = ..., options: dict = ..., nodename: str = ...) -> None:
        """
        Invoke the *callback* function for each of the records streaming back \
        from the scan.

        :param typing.Callable callback: the function to invoke for each record.
        :param dict policy: optional the Aerospike Python client documentation.
        :param dict options: the the Aerospike Python client documentation that will apply to the scan.
        :param str nodename: optional Node ID of node used to limit the scan to a single node.

            A the Aerospike Python client documentation is passed as the argument to the callback function.
            If the scan is using the "partition_filter" scan policy the callback will receive two arguments
            The first is a int representing partition id, the second is the same the Aerospike Python client documentation
            as a normal callback.

                policy={'key':aerospike.POLICY_KEY_SEND})
            client.put(('test','test','key2'), {'id':2,'b':2},
                policy={'key':aerospike.POLICY_KEY_SEND})

            def show_key(record):
                key, meta, bins = record
                print(key)

            scan = client.scan('test', 'test')
            scan_opts = {
              'concurrent': True,
              'nobins': True
            }
            scan.foreach(show_key, options=scan_opts)
            client.close()

            We expect to see:

                ('test', 'test', u'key2', bytearray(b'\xb2\x18\n\xd4\xce\xd8\xba:\x96s\xf5\x9ba\xf1j\xa7t\xeem\x01'))
                ('test', 'test', u'key1', bytearray(b'\x1cJ\xce\xa7\xd4Vj\xef+\xdf@W\xa5\xd8o\x8d:\xc9\xf4\xde'))

                import aerospike

                config = { 'hosts': [ ('127.0.0.1',3000)]}
                client = aerospike.client(config)

                def limit(lim, result):
                    c = [0] # integers are immutable so a list (mutable) is used for the counter
                    def key_add(record):
                        key, metadata, bins = record
                        if c[0] < lim:
                            result.append(key)
                            c[0] = c[0] + 1
                        else:
                            return False
                    return key_add

                scan = client.scan('test','user')
                keys = []
                scan.foreach(limit(100, keys))
                print(len(keys)) # this will be 100 if the number of matching records > 100
                client.close()

         "partition_filter" see the Aerospike Python client documentation can be used to specify which partitions/records
         foreach will scan. See the example below.

                print(part_id)
                partitions.append(part_id)

            scan = client.scan("test", "demo")

            policy = {
                "partition_filter": {
                    "begin": 1000,
                    "count": 4
                },
            }

            scan.foreach(callback, policy)

            # NOTE that these will only be non 0 if there are records in partitions 1000 - 1003
            # should be 4
            print(len(partitions))

            # should be [1000, 1001, 1002, 1003]
            print(partitions)
        """
    def execute_background(self, policy: dict = ...) -> int:
        """
        Execute a record UDF on records found by the scan in the background. This method returns before the scan has completed.
        A UDF can be added to the scan with Scan.apply().

        :param dict policy: optional the Aerospike Python client documentation.

        :return: a job ID that can be used with Client.job_info() to track the status of the ``aerospike.JOB_SCAN``, as it runs in the background.

            Python client version 3.10.0 implemented scan execute_background.
        """
    def get_partitions_status(self) -> tuple:
        """
        Get this scan instance's partition status. That is which partitions have been queried and which have not.
        The returned value is a dict with partition id, int, as keys and tuple as values.
        If the scan instance is not tracking its partitions, the returned dict will be empty.

            A scan instance must have had .paginate() called on it in order retrieve its
            partition status. If .paginate() was not called, the scan instance will not save partition status.

        :return: a tuple of form (id: int, init: class`bool`, done: class`bool`, digest: bytearray).
            See the Aerospike Python client documentation for more information.

                key = ("test", "demo", i)
                bins = {"id": i}
                client.put(key, bins)

            records = []
            resumed_records = []

            def callback(input_tuple):
                record, _, _ = input_tuple

                if len(records) == 5:
                    return False

                records.append(record)

            scan = client.scan("test", "demo")
            scan.paginate()

            scan.foreach(callback)

            # The first scan should stop after 5 records.
            assert len(records) == 5

            partition_status = scan.get_partitions_status()

            def resume_callback(part_id, input_tuple):
                record, _, _ = input_tuple
                resumed_records.append(record)

            scan_resume = client.scan("test", "demo")

            policy = {
                "partition_filter": {
                    "partition_status": partition_status
                },
            }

            scan_resume.foreach(resume_callback, policy)

            # should be 15
            total_records = len(records) + len(resumed_records)
            print(total_records)

            # cleanup
            for i in range(15):
                key = ("test", "demo", i)
                client.remove(key)
        """
    def is_done(self) -> bool:
        """
        If using scan pagination, did the previous paginated or partition_filter scan using this scan instance return all records?

        :return: A bool signifying whether this paginated scan instance has returned all records.

                print("all done")

            # This id can be used to monitor the progress of a paginated scan.
        """
    def paginate(self) -> None:
        """
        Makes a scan instance a paginated scan.
        Call this if you are using the "max_records" scan policy and you need to scan data in pages.

            Calling .paginate() on a scan instance causes it to save its partition state.
            This can be retrieved later using .get_partitions_status(). This can also be done using the
            partition_filter policy.

                records = scan.results(policy=policy)

                print("got page: " + str(page))

                if scan.is_done():
                    print("all done")
                    break

            # This id can be used to paginate queries.
        """
    def results(self, policy: dict = ..., nodename: str = ...) -> list:
        """
        Buffer the records resulting from the scan, and return them as a \
        list of records.

        :param dict policy: optional the Aerospike Python client documentation.
        :param str nodename: optional Node ID of node used to limit the scan to a single node.

        :return: a list of the Aerospike Python client documentation.

                policy={'key':aerospike.POLICY_KEY_SEND})
            client.put(('test','test','key2'), {'id':2,'b':2},
                policy={'key':aerospike.POLICY_KEY_SEND})

            scan = client.scan('test', 'test')
            scan.select('id','a','zzz')
            res = scan.results()
            pp.pprint(res)
            client.close()

            We expect to see:

                [ ( ( 'test',
                      'test',
                      u'key2',
                      bytearray(b'\xb2\x18\n\xd4\xce\xd8\xba:\x96s\xf5\x9ba\xf1j\xa7t\xeem\x01')),
                    { 'gen': 52, 'ttl': 2592000},
                    { 'id': 2}),
                  ( ( 'test',
                      'test',
                      u'key1',
                      bytearray(b'\x1cJ\xce\xa7\xd4Vj\xef+\xdf@W\xa5\xd8o\x8d:\xc9\xf4\xde')),
                    { 'gen': 52, 'ttl': 2592000},
                    { 'a': 1, 'id': 1})]

         "partition_filter" see the Aerospike Python client documentation can be used to specify which partitions/records
         results will scan. See the example below.

                "partition_filter": {
                    "begin": 1000,
                    "count": 4
                },
            }

            # NOTE that these will only be non 0 if there are records in partitions 1000 - 1003
            # results will be the records in partitions 1000 - 1003
            results = scan.results(policy=policy)
        """
    # TODO: this isn't an infinite list of bins
    def select(self, *args, **kwargs) -> None:
        """
        Set a filter on the record bins resulting from results() or \
        foreach(). If a selected bin does not exist in a record it will \
        not appear in the *bins* portion of that record tuple.
        """

@final
class null:
    """A type for distinguishing a server-side null from a Python None.

    Replaces the constant aerospike.null.
    Example:
    Args:
    Returns:
    Raises:
    See Also:
    """
    def __init__(self) -> None:
        """Initialize a null type representing the server-side type as_null.
        Example:
        Args:
        Returns:
            None.

        Raises:
        See Also:
        """

def calc_digest(ns: str, set: str, key: Union[str, int, bytearray]) -> bytearray:
    """
    Calculate the digest of a particular key. See: the Aerospike Python client documentation.

    :param str ns: the namespace in the aerospike cluster.
    :param str set: the set name.
    :param key: the primary key identifier of the record within the set.
    :type key: str, int or bytearray
    :return: a RIPEMD-160 digest of the input tuple.

        import aerospike
        import pprint

        digest = aerospike.calc_digest("test", "demo", 1 )
        pp.pprint(digest)
    """
def client(config: dict) -> Client:
    """
    Creates a new instance of the Client class and immediately connects to the cluster.

    See the Aerospike Python client documentation for more details.

    Internally, this is a wrapper function which calls the constructor for the Client class.
    However, the client may also be constructed by calling the constructor directly.

    The client takes on many configuration parameters passed in through a dictionary.

    :param dict config: See the Aerospike Python client documentation.

    :return: an instance of the Client class.

    Simple example:

        import aerospike

        # Configure the client to first connect to a cluster node at 127.0.0.1
        # The client will learn about the other nodes in the cluster from the seed node.
        # Also sets a top level policy for read commands
        config = {
            'hosts':    [ ('127.0.0.1', 3000) ],
            'policies': {'read': {'total_timeout': 1000}},
        }
        client = aerospike.client(config)

    Connecting using TLS example:

        import aerospike
        import sys

        # NOTE: Use of TLS requires Aerospike Enterprise version >= 3.11
        # and client version 2.1.0 or greater
        tls_name = "some-server-tls-name"
        tls_ip = "127.0.0.1"
        tls_port = 4333

        # If tls-name is specified,
        # it must match the tls-name in the node’s server configuration file
        # and match the server’s CA certificate.
        tls_host_tuple = (tls_ip, tls_port, tls_name)
        hosts = [tls_host_tuple]

        # Example configuration which will use TLS with the specified cafile
        tls_config = {
            "cafile": "/path/to/cacert.pem",
            "enable": True
        }
        try:
            client = aerospike.client({
                "hosts": hosts,
                "tls": tls_config
            })
        except Exception as e:
            print(e)
            print("Failed to connect")
            sys.exit()
    """
def geodata(geo_data: dict) -> GeoJSON:
    """
    Helper for creating an instance of the aerospike.GeoJSON class. \
    Used to wrap a geospatial object, such as a point, polygon or circle.

    :param dict geo_data: a dict representing the geospatial data.
    :return: an instance of the aerospike.GeoJSON class.

        import aerospike

        # Create GeoJSON point using WGS84 coordinates.
        latitude = 45.920278
        longitude = 63.342222
        loc = aerospike.geodata({'type': 'Point',
                                 'coordinates': [longitude, latitude]})
    """
def geojson(geojson_str: str) -> GeoJSON:
    """
    Helper for creating an instance of the aerospike.GeoJSON class \
    from a raw GeoJSON str.

    :param dict geojson_str: a str of raw GeoJSON.
    :return: an instance of the aerospike.GeoJSON class.

        import aerospike

        # Create GeoJSON point using WGS84 coordinates.
        loc = aerospike.geojson('{"type": "Point", "coordinates": [-80.604333, 28.608389]}')
    """
def get_partition_id(*args, **kwargs) -> Any: ...
def set_deserializer(callback: Callable) -> None:
    """
    Register a user-defined deserializer available to all Client
    instances.

    Once registered, all read methods (such as Client.get()) will run bins containing 'Generic' *as_bytes* \
    of type `AS_BYTES_BLOB <http://www.aerospike.com/apidocs/c/d0/dd4/as__bytes_8h.html#a0cf2a6a1f39668f606b19711b3a98bf3>`_
    through this deserializer.

    :param typing.Callable callback: the function to invoke for deserialization.
    """
def set_log_handler(callback: Callable = ...) -> None:
    """
    Set logging callback globally across all clients.

    When no argument is passed, the default log handler is used. See the Aerospike Python client documentation for more details.

    When callback is None, the saved log handler is cleared.

    When a callable is passed, it must have these five parameters in this order:

        def callback(level: int, function: str, path: str, line: int, message: str):
            pass

    :param typing.Callable | None log_handler: the function used as the logging handler.
    """
def set_log_level(log_level: int) -> None:
    """
    Declare the logging level threshold for the log handler. If setting log level to aerospike.LOG_LEVEL_OFF,
    the current log handler does not get reset.

    :param int loglevel: one of the the Aerospike Python client documentation constant values.
    """
def set_serializer(callback: Callable) -> None:
    """
    Register a user-defined serializer available to all `Client`
    instances.

    :param typing.Callable callback: the function to invoke for serialization.

        the argument to the serializer parameter should be aerospike.SERIALIZER_USER.

        def my_serializer(val):
            return json.dumps(val)

        aerospike.set_serializer(my_serializer)
    """
def unset_serializers() -> None:
    """
    Deregister the user-defined deserializer/serializer available from Client
    instances.
    """
