import aerospike_helpers.metrics
from _typeshed import Incomplete
from typing import Any, overload
from typing import Literal

AS_BOOL: Literal[1]
AS_BYTES_BLOB: Literal[4]
AS_BYTES_BOOL: Literal[17]
AS_BYTES_CSHARP: Literal[8]
AS_BYTES_DOUBLE: Literal[2]
AS_BYTES_ERLANG: Literal[12]
AS_BYTES_GEOJSON: Literal[23]
AS_BYTES_HLL: Literal[18]
AS_BYTES_INTEGER: Literal[1]
AS_BYTES_JAVA: Literal[7]
AS_BYTES_LIST: Literal[20]
AS_BYTES_MAP: Literal[19]
AS_BYTES_PHP: Literal[11]
AS_BYTES_PYTHON: Literal[9]
AS_BYTES_RUBY: Literal[10]
AS_BYTES_STRING: Literal[3]
AS_BYTES_TYPE_MAX: Literal[24]
AS_BYTES_UNDEF: Literal[0]
AUTH_EXTERNAL: Literal[1]
AUTH_EXTERNAL_INSECURE: Literal[2]
AUTH_INTERNAL: Literal[0]
AUTH_PKI: Literal[3]
BIT_OVERFLOW_FAIL: Literal[0]
BIT_OVERFLOW_SATURATE: Literal[2]
BIT_OVERFLOW_WRAP: Literal[4]
BIT_RESIZE_DEFAULT: Literal[0]
BIT_RESIZE_FROM_FRONT: Literal[1]
BIT_RESIZE_GROW_ONLY: Literal[2]
BIT_RESIZE_SHRINK_ONLY: Literal[4]
BIT_WRITE_CREATE_ONLY: Literal[1]
BIT_WRITE_DEFAULT: Literal[0]
BIT_WRITE_NO_FAIL: Literal[4]
BIT_WRITE_PARTIAL: Literal[8]
BIT_WRITE_UPDATE_ONLY: Literal[2]
CDT_CTX_LIST_INDEX: Literal[0x10]
CDT_CTX_LIST_INDEX_CREATE: Literal[0x14]
CDT_CTX_LIST_RANK: Literal[0x11]
CDT_CTX_LIST_VALUE: Literal[0x13]
CDT_CTX_MAP_INDEX: Literal[0x20]
CDT_CTX_MAP_KEY: Literal[0x22]
CDT_CTX_MAP_KEY_CREATE: Literal[0x24]
CDT_CTX_MAP_RANK: Literal[0x21]
CDT_CTX_MAP_VALUE: Literal[0x23]
EXP_READ_DEFAULT: Literal[0]
EXP_READ_EVAL_NO_FAIL: Literal[16]
EXP_WRITE_ALLOW_DELETE: Literal[4]
EXP_WRITE_CREATE_ONLY: Literal[1]
EXP_WRITE_DEFAULT: Literal[0]
EXP_WRITE_EVAL_NO_FAIL: Literal[16]
EXP_WRITE_POLICY_NO_FAIL: Literal[8]
EXP_WRITE_UPDATE_ONLY: Literal[2]
HLL_WRITE_ALLOW_FOLD: Literal[8]
HLL_WRITE_CREATE_ONLY: Literal[1]
HLL_WRITE_DEFAULT: Literal[0]
HLL_WRITE_NO_FAIL: Literal[4]
HLL_WRITE_UPDATE_ONLY: Literal[2]
INDEX_BLOB: Literal[3]
INDEX_GEO2DSPHERE: Literal[2]
INDEX_NUMERIC: Literal[1]
INDEX_STRING: Literal[0]
INDEX_TYPE_DEFAULT: Literal[0]
INDEX_TYPE_LIST: Literal[1]
INDEX_TYPE_MAPKEYS: Literal[2]
INDEX_TYPE_MAPVALUES: Literal[3]
INTEGER: Literal[0]
JOB_QUERY: Literal["query"]
JOB_SCAN: Literal["scan"]
JOB_STATUS_COMPLETED: Literal[2]
JOB_STATUS_INPROGRESS: Literal[1]
JOB_STATUS_UNDEF: Literal[0]
LIST_ORDERED: Literal[1]
LIST_RETURN_COUNT: Literal[5]
LIST_RETURN_EXISTS: Literal[13]
LIST_RETURN_INDEX: Literal[1]
LIST_RETURN_NONE: Literal[0]
LIST_RETURN_RANK: Literal[3]
LIST_RETURN_REVERSE_INDEX: Literal[2]
LIST_RETURN_REVERSE_RANK: Literal[4]
LIST_RETURN_VALUE: Literal[7]
LIST_SORT_DEFAULT: Literal[0]
LIST_SORT_DROP_DUPLICATES: Literal[2]
LIST_UNORDERED: Literal[0]
LIST_WRITE_DEFAULT: Literal[0]
LIST_WRITE_ADD_UNIQUE: Literal[1]
LIST_WRITE_INSERT_BOUNDED: Literal[2]
LIST_WRITE_NO_FAIL: Literal[4]
LIST_WRITE_PARTIAL: Literal[8]
LOG_LEVEL_DEBUG: Literal[3]
LOG_LEVEL_ERROR: Literal[0]
LOG_LEVEL_INFO: Literal[2]
LOG_LEVEL_OFF: Literal[-1]
LOG_LEVEL_TRACE: Literal[4]
LOG_LEVEL_WARN: Literal[1]
MAP_KEY_ORDERED: Literal[1]
MAP_KEY_VALUE_ORDERED: Literal[3]
MAP_RETURN_COUNT: Literal[5]
MAP_RETURN_EXISTS: Literal[13]
MAP_RETURN_INDEX: Literal[1]
MAP_RETURN_KEY: Literal[6]
MAP_RETURN_KEY_VALUE: Literal[8]
MAP_RETURN_NONE: Literal[0]
MAP_RETURN_RANK: Literal[3]
MAP_RETURN_REVERSE_INDEX: Literal[2]
MAP_RETURN_REVERSE_RANK: Literal[4]
MAP_RETURN_VALUE: Literal[7]
MAP_RETURN_ORDERED_MAP: Literal[17]
MAP_RETURN_UNORDERED_MAP: Literal[16]
MAP_UNORDERED: Literal[0]
MAP_WRITE_FLAGS_CREATE_ONLY: Literal[1]
MAP_WRITE_FLAGS_DEFAULT: Literal[0]
MAP_WRITE_FLAGS_NO_FAIL: Literal[4]
MAP_WRITE_FLAGS_PARTIAL: Literal[8]
MAP_WRITE_FLAGS_UPDATE_ONLY: Literal[2]
MAP_WRITE_NO_FAIL: Literal[4]
MAP_WRITE_PARTIAL: Literal[8]
OPERATOR_APPEND: Literal[9]
OPERATOR_DELETE: Literal[14]
OPERATOR_INCR: Literal[6]
OPERATOR_PREPEND: Literal[10]
OPERATOR_READ: Literal[0]
OPERATOR_TOUCH: Literal[11]
OPERATOR_WRITE: Literal[1]
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
    """A type used to match anything when used in a Map or list comparison."""
    @classmethod
    def __init__(cls, *args, **kwargs) -> None:
        """Create and return a new object.  See help(type) for accurate signature."""

class CDTWildcard:
    """A type used to match anything when used in a Map or list comparison."""
    @classmethod
    def __init__(cls, *args, **kwargs) -> None:
        """Create and return a new object.  See help(type) for accurate signature."""

class Client:
    """The Client class manages the connections and trasactions against
    an Aerospike cluster.
    """
    def __init__(self, *args, **kwargs) -> None:
        """Initialize self.  See help(type(self)) for accurate signature."""
    def abort(self, transaction: Transaction): ...
    def admin_change_password(self, user, password, policy: dict = ...):
        """Change password."""
    def admin_create_pki_user(self, user: str, roles: list, policy: dict = ...):
        """Create a new pki user."""
    def admin_create_role(self, role, privileges, policy: dict = ..., whitelist=..., read_quota=..., write_quota=...):
        """Create a new role."""
    def admin_create_user(self, user, password, roles, policy: dict = ...):
        """Create a new user."""
    def admin_drop_role(self, role, policy: dict = ...):
        """Drop a new role."""
    def admin_drop_user(self, user, policy: dict = ...):
        """Drop a user."""
    def admin_get_role(self, role, policy: dict = ...):
        """Get a user defined role."""
    def admin_get_roles(self, policy: dict = ...):
        """Get all user defined roles."""
    def admin_grant_privileges(self, role, privileges, policy: dict = ...):
        """Grant privileges to a user defined role."""
    def admin_grant_roles(self, user, roles, policy: dict = ...):
        """Grant Roles."""
    def admin_query_role(self, role, policy: dict = ...):
        """DEPRECATED Query a user defined role."""
    def admin_query_roles(self, policy: dict = ...):
        """DEPRECATED Query all user defined roles."""
    def admin_query_user_info(self, *args, **kwargs):
        """Query a user for read/write info, connections-in-use and roles."""
    def admin_query_users_info(self, *args, **kwargs):
        """Query all users for read/write info, connections-in-use and roles."""
    def admin_revoke_privileges(self, role, privileges, policy: dict = ...):
        """Revoke privileges from a user defined role."""
    def admin_revoke_roles(self, user, roles, policy: dict = ...):
        """Revoke roles"""
    def admin_set_password(self, user, password, policy: dict = ...):
        """Set password"""
    def admin_set_quotas(self, role, read_quota=..., write_quota=..., policy: dict = ...):
        """Set read and write quotas for a user defined role."""
    def admin_set_whitelist(self, role, whitelist, policy: dict = ...):
        """Set IP whitelist for a user defined role."""
    def append(self, key, bin, val, meta: dict = ..., policy: dict = ...):
        """append(key, bin, val[, meta[, policy]])

        Append the string val to the string value in bin."""
    def apply(self, *args, **kwargs):
        """apply(key, module, function, args[, policy])

        Apply a registered (see udf_put()) record UDF to a particular record."""
    def batch_apply(self, keys: list, module: str, function: str, args: list, policy_batch: dict = ..., policy_batch_apply: dict = ...):
        """batch_apply([keys], module, function, [args], policy_batch, policy_batch_apply) -> BatchRecords

        Apply a user defined function (UDF) to multiple keys. Requires server version 6.0+"""
    def batch_operate(self, keys: list, ops: list, policy_batch: dict = ..., policy_batch_write: dict = ..., ttl: int = ...):
        """batch_operate([keys], [ops], policy_batch, policy_batch_write) -> BatchRecords

        Perform read/write operations on multiple keys. Requires server version 6.0+"""
    def batch_read(self, keys: list, bins: list = ..., policy: dict = ...):
        """Read multiple keys."""
    def batch_remove(self, keys: list, policy_batch: dict = ..., policy_batch_remove: dict = ...):
        """batch_remove([keys], policy_batch, policy_batch_remove) -> BatchRecords

        Remove multiple records by key. Requires server version 6.0+"""
    def batch_write(self, batch_records: BatchRecords, policy_batch: dict = ...):
        """batch_write(batch_records, policy) -> None

        Read/Write multiple records for specified batch keys in one batch call. This method allows different sub-commands for each key in the batch. The returned records are located in the same list. Requires server version 6.0+"""
    def close(self):
        """Close the connection(s) to the cluster."""
    def commit(self, transaction: Transaction): ...
    def connect(self, username=..., password=...):
        """connect([username, password])

        Connect to the cluster. The optional username and password only apply when connecting to the Enterprise Edition of Aerospike."""
    def disable_metrics(self): ...
    def enable_metrics(self, policy: aerospike_helpers.metrics.MetricsPolicy | None = ...): ...
    def exists(self, key, policy: dict = ...):
        """exists(key[, policy]) -> (key, meta)

        Check if a record with a given key exists in the cluster and return the record as a tuple() consisting of key and meta. If the record does not exist the meta data will be None."""
    def get(self, key, policy: dict = ...):
        """get(key[, policy]) -> (key, meta, bins)

        Read a record with a given key, and return the record as a tuple() consisting of key, meta and bins."""
    def get_cdtctx_base64(self, ctx: list):
        """get_cdtctx_base64(compiled_cdtctx: list) -> str

        Get the base64 representation of a compiled aerospike CDT ctx."""
    def get_expression_base64(self, expression):
        """get_expression_base64(compiled_expression: list) -> str

        Get the base64 representation of a compiled aerospike expression."""
    def get_key_partition_id(self, ns, set, key) -> int:
        """get_key_partition_id(ns, set, key) -> int

        Gets the partition ID of given key. See: Key Tuple."""
    def get_node_names(self):
        """get_node_names() -> []

        Return the list of hosts, including node names, present in a connected cluster."""
    def get_nodes(self):
        """get_nodes() -> []

        Return the list of hosts present in a connected cluster."""
    def get_stats(self): ...
    def increment(self, key, bin, offset, meta: dict = ..., policy: dict = ...):
        """increment(key, bin, offset[, meta[, policy]])

        Increment the integer value in bin by the integer val."""
    def index_blob_create(self, ns, set, bin, name, policy=...):
        """index_blob_create(ns, set, bin, index_name[, policy])

        Create a blob index with index_name on the bin in the specified ns, set."""
    def index_cdt_create(self, *args, **kwargs):
        """index_cdt_create(ns, set, bin,  index_type, index_datatype, index_name, ctx, [, policy])

        Create an cdt index named index_name for list, map keys or map values (as defined by index_type) and for numeric, string or GeoJSON values (as defined by index_datatype) on records of the specified ns, set whose bin is a list or map."""
    def index_expr_create(self, ns, set, index_type, index_datatype, expressions, name, policy: dict = ...): ...
    def index_geo2dsphere_create(self, ns, set, bin, name, policy: dict = ...):
        """index_geo2dsphere_create(ns, set, bin, index_name[, policy])

        Create a geospatial 2D spherical index with index_name on the bin in the specified ns, set."""
    def index_integer_create(self, ns, set, bin, name, policy=...):
        """index_integer_create(ns, set, bin, index_name[, policy])

        Create an integer index with index_name on the bin in the specified ns, set."""
    def index_list_create(self, ns, set, bin, index_datatype, name, policy: dict = ...):
        """index_list_create(ns, set, bin, index_datatype, index_name[, policy])

        Create an index named index_name for numeric, string or GeoJSON values (as defined by index_datatype) on records of the specified ns, set whose bin is a list."""
    def index_map_keys_create(self, ns, set, bin, index_datatype, name, policy: dict = ...):
        """index_map_keys_create(ns, set, bin, index_datatype, index_name[, policy])

        Create an index named index_name for numeric, string or GeoJSON values (as defined by index_datatype) on records of the specified ns, set whose bin is a map. The index will include the keys of the map."""
    def index_map_values_create(self, ns, set, bin, index_datatype, name, policy: dict = ...):
        """index_map_values_create(ns, set, bin, index_datatype, index_name[, policy])

        Create an index named index_name for numeric, string or GeoJSON values (as defined by index_datatype) on records of the specified ns, set whose bin is a map. The index will include the values of the map."""
    def index_remove(self, ns: str, name: str, policy: dict = ...):
        """index_remove(ns, index_name[, policy])

        Remove the index with index_name from the namespace."""
    def index_string_create(self, ns, set, bin, name, policy: dict = ...):
        """index_string_create(ns, set, bin, index_name[, policy])

        Create a string index with index_name on the bin in the specified ns, set."""
    def info_all(self, command, policy: dict = ...):
        """info_all(command[, policy]]) -> {}

        Send an info *command* to all nodes in the cluster to which the client is connected.
        If any of the individual requests fail, this will raise an exception."""
    def info_random_node(self, command, policy: dict = ...):
        """info_random_node(command, [policy]) -> str

        Send an info command to a single random node."""
    def info_single_node(self, command, host, policy: dict = ...):
        """info_single_node(command, host[, policy]) -> str

        Send an info command to a single node specified by host."""
    def is_connected(self):
        """Checks current connection state."""
    def job_info(self, job_id, module, policy: dict = ...):
        """job_info(job_id, module[, policy]) -> dict

        Return the status of a job running in the background."""
    def operate(self, key, list: list, meta: dict = ..., policy: dict = ...):
        """operate(key, list[, meta[, policy]]) -> (key, meta, bins)

        Perform multiple bin operations on a record with a given key, In Aerospike server versions prior to 3.6.0, non-existent bins being read will have a None value. Starting with 3.6.0 non-existent bins will not be present in the returned Record Tuple. The returned record tuple will only contain one entry per bin, even if multiple operations were performed on the bin."""
    def operate_ordered(self, key, list: list, meta: dict = ..., policy: dict = ...):
        """operate_ordered(key, list[, meta[, policy]]) -> (key, meta, bins)

        Perform multiple bin operations on a record with the results being returned as a list of (bin-name, result) tuples. The order of the elements in the list will correspond to the order of the operations from the input parameters."""
    def prepend(self, key, bin, val, meta: dict = ..., policy: dict = ...):
        """prepend(key, bin, val[, meta[, policy]])

        Prepend the string value in bin with the string val."""
    def put(self, key, bins: dict, meta: dict = ..., policy: dict = ..., serializer=...):
        """put(key, bins[, meta[, policy[, serializer]]])

        Write a record with a given key to the cluster."""
    def query(self, namespace, set=...):
        """query(namespace[, set]) -> Query

        Return a `aerospike.Query` object to be used for executing queries over a specified set (which can be omitted or None) in a namespace. A query with a None set returns records which are not in any named set. This is different than the meaning of a None set in a scan."""
    def query_apply(self, ns, set, predicate, module, function, args=..., policy: dict = ...):
        """query_apply(ns, set, predicate, module, function[, args[, policy]]) -> int

        Initiate a background query and apply a record UDF to each record matched by the query."""
    def remove(self):
        """remove(key[, policy])

        Remove a record matching the key from the cluster."""
    def remove_bin(self, key, list, meta: dict = ..., policy: dict = ...):
        """remove_bin(key, list[, meta[, policy]])

        Remove a list of bins from a record with a given key. Equivalent to setting those bins to aerospike.null() with a put()."""
    def scan(self, namespace, set=...):
        """scan(namespace[, set]) -> Scan

        Return a `aerospike.Scan` object to be used for executing scans over a specified set (which can be omitted or None) in a namespace. A scan with a None set returns all the records in the namespace."""
    def scan_apply(self, ns, set, module, function, args=..., policy: dict = ..., options=...):
        """scan_apply(ns, set, module, function[, args[, policy[, options,[ block]]]]) -> int

        Initiate a background scan and apply a record UDF to each record matched by the scan."""
    def select(self, *args, **kwargs):
        """select(key, bins[, policy]) -> (key, meta, bins)

        Read a record with a given key, and return the record as a tuple() consisting of key, meta and bins, with the specified bins projected. Prior to Aerospike server 3.6.0, if a selected bin does not exist its value will be None. Starting with 3.6.0, if a bin does not exist it will not be present in the returned Record Tuple."""
    def set_xdr_filter(self, data_center, namespace, expression_filter, policy: dict = ...):
        """set_xdr_filter(data_center, namespace, expression_filter[, policy]) -> {}

        Set cluster xdr filter."""
    def shm_key(self):
        """Get the shm key of the cluster"""
    def touch(self, key, val=..., meta: dict = ..., policy: dict = ...):
        """touch(key[, val=0[, meta[, policy]]])

        Touch the given record, resetting its time-to-live and incrementing its generation."""
    def truncate(self, namespace, set, nanos, policy: dict = ...):
        """truncate(namespace, set, nanos[, policy])

        Remove records in specified namespace/set efficiently. This method is many orders of magnitude faster than deleting records one at a time. Works with Aerospike Server versions >= 3.12.

        This asynchronous server call may return before the truncation is complete. The user can still write new records after the server returns because new records will have last update times greater than the truncate cutoff (set at the time of truncate call)"""
    def udf_get(self, module: str, language: int = ..., policy: dict = ...):
        """udf_get(module[, language[, policy]]) -> str

        Return the content of a UDF module which is registered with the cluster."""
    def udf_list(self, policy: dict = ...):
        """udf_list([policy]) -> []

        Return the list of UDF modules registered with the cluster."""
    def udf_put(self, filename, udf_type=..., policy: dict = ...):
        """udf_put(filename[, udf_type[, policy]])

        Register a UDF module with the cluster."""
    def udf_remove(self, module, policy: dict = ...):
        """udf_remove(module[, policy])

        Remove a  previously registered UDF module from the cluster."""

class ConfigProvider:
    interval: Incomplete
    path: Incomplete
    @classmethod
    def __init__(cls, *args, **kwargs) -> None:
        """Create and return a new object.  See help(type) for accurate signature."""

class GeoJSON:
    """The GeoJSON class casts geospatial data to and from the server's
    as_geojson type.
    """
    geo_data: Incomplete
    def __init__(self, geo_data=...) -> None:
        """Initialize self.  See help(type(self)) for accurate signature."""
    def dumps(self):
        """Get the geospatial data in form of a GeoJSON string."""
    def loads(self, raw_geo):
        """Set the geospatial data from a raw GeoJSON string"""
    def unwrap(self):
        """Returns the geospatial data contained in the aerospike.GeoJSON object"""
    def wrap(self, geo_data):
        """Sets the geospatial data in the aerospike.GeoJSON object"""

class KeyOrderedDict(dict):
    """The KeyOrderedDict class is a dictionary that directly maps
    to a key ordered map on the Aerospike server.
    This assists in matching key ordered maps
    through various read operations.
    """
    def __init__(self, *args, **kwargs) -> None:
        """Initialize self.  See help(type(self)) for accurate signature."""

class Query:
    """The Query class assists in populating the parameters of a query
    operation. To create a new instance of the Query class, call the
    query() method on an instance of a Client class.
    """
    max_records: Incomplete
    records_per_second: Incomplete
    ttl: Incomplete
    def __init__(self, *args, **kwargs) -> None:
        """Initialize self.  See help(type(self)) for accurate signature."""
    def add_ops(self, ops):
        """add_ops(ops)

        Add a list of write ops to the query. When used with :meth:`Query.execute_background` the query will perform the write ops on any records found. If no predicate is attached to the Query it will apply ops to all the records in the specified set."""
    def apply(self, *args, **kwargs):
        """apply(module, function[, arguments])

        Aggregate the results() using a stream UDF. If no predicate is attached to the Query the stream UDF will aggregate over all the records in the specified set."""
    def execute_background(self, policy=...):
        """execute_background([policy]) -> list of (key, meta, bins)

        Buffer the records resulting from the query, and return them as a list of records."""
    def foreach(self, *args, **kwargs):
        """foreach(callback[, policy])

        Invoke the callback function for each of the records streaming back from the query."""
    def get_partitions_status(self):
        """get_parts() -> {int: (int, bool, bool, bytearray[20]), ...}

        Gets the complete partition status of the query. Returns a dictionary of the form {id:(id, init, done, digest), ...}."""
    def is_done(self):
        """is_done() -> bool

        If using query pagination, did the previous paginated query with this query instance return all records?"""
    def paginate(self):
        """paginate()

        Set pagination filter to receive records in bunch (max_records or page_size)."""
    def results(self, *args, **kwargs):
        """results([policy]) -> list of (key, meta, bins)

        Buffer the records resulting from the query, and return them as a list of records."""
    def select(self, *args, **kwargs):
        """select(bin1[, bin2[, bin3..]])

        Set a filter on the record bins resulting from results() or foreach(). If a selected bin does not exist in a record it will not appear in the bins portion of that record tuple."""
    def where(self, predicate, ctx=...):
        """where(predicate[, cdt_ctx])

        Set a where predicate for the query, without which the query will behave similar to aerospike.Scan. The predicate is produced by one of the aerospike.predicates methods equals() and between(). The list cdt_ctx is produced by one of the aerospike_helpers.cdt_ctx methods"""
    def where_with_expr(self, expr, predicate):
        """where(predicate[, cdt_ctx])

        Set a where predicate for the query, without which the query will behave similar to aerospike.Scan. The predicate is produced by one of the aerospike.predicates methods equals() and between(). The list cdt_ctx is produced by one of the aerospike_helpers.cdt_ctx methods"""
    def where_with_index_name(self, index_name, predicate):
        """where(predicate[, cdt_ctx])

        Set a where predicate for the query, without which the query will behave similar to aerospike.Scan. The predicate is produced by one of the aerospike.predicates methods equals() and between(). The list cdt_ctx is produced by one of the aerospike_helpers.cdt_ctx methods"""

class Scan:
    """The Scan class assists in populating the parameters of a scan
    operation. To create a new instance of the Scan class, call the
    scan() method on an instance of a Client class.
    """
    ttl: Incomplete
    def __init__(self, *args, **kwargs) -> None:
        """Initialize self.  See help(type(self)) for accurate signature."""
    def add_ops(self, ops):
        """results([policy [, nodename]) -> list of (key, meta, bins)

        Buffer the records resulting from the scan, and return them as a list of records.If provided nodename should be the Node ID of a node to limit the scan to."""
    def apply(self, *args, **kwargs):
        """results([policy [, nodename]) -> list of (key, meta, bins)

        Buffer the records resulting from the scan, and return them as a list of records.If provided nodename should be the Node ID of a node to limit the scan to."""
    def execute_background(self, policy=...):
        """results([policy [, nodename]) -> list of (key, meta, bins)

        Buffer the records resulting from the scan, and return them as a list of records.If provided nodename should be the Node ID of a node to limit the scan to."""
    def foreach(self, *args, **kwargs):
        """foreach(callback[, policy[, options [, nodename]])

        Invoke the callback function for each of the records streaming back from the scan. If provided nodename should be the Node ID of a node to limit the scan to."""
    def get_partitions_status(self):
        """get_parts() -> {int: (int, bool, bool, bytearray[20]), ...}

        Gets the complete partition status of the scan. Returns a dictionary of the form {id:(id, init, done, digest), ...}."""
    def is_done(self):
        """is_done() -> bool

        Gets the status of scan"""
    def paginate(self):
        """paginate()

        Set pagination filter to receive records in bunch (max_records or page_size)."""
    def results(self, *args, **kwargs):
        """results([policy [, nodename]) -> list of (key, meta, bins)

        Buffer the records resulting from the scan, and return them as a list of records.If provided nodename should be the Node ID of a node to limit the scan to."""
    def select(self, *args, **kwargs):
        """select(bin1[, bin2[, bin3..]])

        Set a filter on the record bins resulting from results() or foreach(). If a selected bin does not exist in a record it will not appear in the bins portion of that record tuple."""

class Transaction:
    id: Incomplete
    in_doubt: Incomplete
    state: Incomplete
    timeout: Incomplete
    def __init__(self, *args, **kwargs) -> None:
        """Initialize self.  See help(type(self)) for accurate signature."""

class null:
    """The nullobject when used with put() works as a removebin()"""
    @classmethod
    def __init__(cls, *args, **kwargs) -> None:
        """Create and return a new object.  See help(type) for accurate signature."""

def calc_digest(*args, **kwargs):
    """Calculate the digest of a key"""
@overload
def client(config) -> clientobject:
    """client(config) -> client object

    Creates a new instance of the Client class.
    This client can connect() to the cluster and perform operations against it, such as put() and get() records.

    config = {
        'hosts':    [ ('127.0.0.1', 3000) ],
        'policies': {'timeout': 1000},
    }
    client = aerospike.client(config)"""
@overload
def client(config) -> Any:
    """client(config) -> client object

    Creates a new instance of the Client class.
    This client can connect() to the cluster and perform operations against it, such as put() and get() records.

    config = {
        'hosts':    [ ('127.0.0.1', 3000) ],
        'policies': {'timeout': 1000},
    }
    client = aerospike.client(config)"""
def geodata(*args, **kwargs):
    """Creates a GeoJSON object from geospatial data."""
def geojson(*args, **kwargs):
    """Creates a GeoJSON object from a raw GeoJSON string."""
def get_partition_id(*args, **kwargs):
    """Get partition ID for given digest"""
def set_deserializer(*args, **kwargs):
    """Sets the deserializer"""
def set_log_handler(*args, **kwargs):
    """Enables the log handler"""
def set_log_level(*args, **kwargs):
    """Sets the log level"""
def set_serializer(*args, **kwargs):
    """Sets the serializer"""
def unset_serializers(*args, **kwargs):
    """Unsets the serializer and deserializer"""
