import aerospike_helpers.metrics
from _typeshed import Incomplete
from typing import Any, overload

ABORT_ALREADY_ABORTED: int
ABORT_CLOSE_ABANDONED: int
ABORT_OK: int
ABORT_ROLL_BACK_ABANDONED: int
AS_BOOL: int
AS_BYTES_BLOB: int
AS_BYTES_BOOL: int
AS_BYTES_CSHARP: int
AS_BYTES_DOUBLE: int
AS_BYTES_ERLANG: int
AS_BYTES_GEOJSON: int
AS_BYTES_HLL: int
AS_BYTES_INTEGER: int
AS_BYTES_JAVA: int
AS_BYTES_LIST: int
AS_BYTES_MAP: int
AS_BYTES_PHP: int
AS_BYTES_PYTHON: int
AS_BYTES_RUBY: int
AS_BYTES_STRING: int
AS_BYTES_TYPE_MAX: int
AS_BYTES_UNDEF: int
AUTH_EXTERNAL: int
AUTH_EXTERNAL_INSECURE: int
AUTH_INTERNAL: int
AUTH_PKI: int
BIT_OVERFLOW_FAIL: int
BIT_OVERFLOW_SATURATE: int
BIT_OVERFLOW_WRAP: int
BIT_RESIZE_DEFAULT: int
BIT_RESIZE_FROM_FRONT: int
BIT_RESIZE_GROW_ONLY: int
BIT_RESIZE_SHRINK_ONLY: int
BIT_WRITE_CREATE_ONLY: int
BIT_WRITE_DEFAULT: int
BIT_WRITE_NO_FAIL: int
BIT_WRITE_PARTIAL: int
BIT_WRITE_UPDATE_ONLY: int
CDT_CTX_LIST_INDEX: int
CDT_CTX_LIST_INDEX_CREATE: int
CDT_CTX_LIST_RANK: int
CDT_CTX_LIST_VALUE: int
CDT_CTX_MAP_INDEX: int
CDT_CTX_MAP_KEY: int
CDT_CTX_MAP_KEY_CREATE: int
CDT_CTX_MAP_RANK: int
CDT_CTX_MAP_VALUE: int
COMMIT_ALREADY_COMMITTED: int
COMMIT_CLOSE_ABANDONED: int
COMMIT_OK: int
COMMIT_ROLL_FORWARD_ABANDONED: int
EXP_READ_DEFAULT: int
EXP_READ_EVAL_NO_FAIL: int
EXP_WRITE_ALLOW_DELETE: int
EXP_WRITE_CREATE_ONLY: int
EXP_WRITE_DEFAULT: int
EXP_WRITE_EVAL_NO_FAIL: int
EXP_WRITE_POLICY_NO_FAIL: int
EXP_WRITE_UPDATE_ONLY: int
HLL_WRITE_ALLOW_FOLD: int
HLL_WRITE_CREATE_ONLY: int
HLL_WRITE_DEFAULT: int
HLL_WRITE_NO_FAIL: int
HLL_WRITE_UPDATE_ONLY: int
INDEX_BLOB: int
INDEX_GEO2DSPHERE: int
INDEX_NUMERIC: int
INDEX_STRING: int
INDEX_TYPE_DEFAULT: int
INDEX_TYPE_LIST: int
INDEX_TYPE_MAPKEYS: int
INDEX_TYPE_MAPVALUES: int
INTEGER: int
JOB_QUERY: str
JOB_SCAN: str
JOB_STATUS_COMPLETED: int
JOB_STATUS_INPROGRESS: int
JOB_STATUS_UNDEF: int
LIST_ORDERED: int
LIST_RETURN_COUNT: int
LIST_RETURN_EXISTS: int
LIST_RETURN_INDEX: int
LIST_RETURN_NONE: int
LIST_RETURN_RANK: int
LIST_RETURN_REVERSE_INDEX: int
LIST_RETURN_REVERSE_RANK: int
LIST_RETURN_VALUE: int
LIST_SORT_DEFAULT: int
LIST_SORT_DROP_DUPLICATES: int
LIST_UNORDERED: int
LIST_WRITE_ADD_UNIQUE: int
LIST_WRITE_DEFAULT: int
LIST_WRITE_INSERT_BOUNDED: int
LIST_WRITE_NO_FAIL: int
LIST_WRITE_PARTIAL: int
LOG_LEVEL_DEBUG: int
LOG_LEVEL_ERROR: int
LOG_LEVEL_INFO: int
LOG_LEVEL_OFF: int
LOG_LEVEL_TRACE: int
LOG_LEVEL_WARN: int
MAP_KEY_ORDERED: int
MAP_KEY_VALUE_ORDERED: int
MAP_RETURN_COUNT: int
MAP_RETURN_EXISTS: int
MAP_RETURN_INDEX: int
MAP_RETURN_KEY: int
MAP_RETURN_KEY_VALUE: int
MAP_RETURN_NONE: int
MAP_RETURN_ORDERED_MAP: int
MAP_RETURN_RANK: int
MAP_RETURN_REVERSE_INDEX: int
MAP_RETURN_REVERSE_RANK: int
MAP_RETURN_UNORDERED_MAP: int
MAP_RETURN_VALUE: int
MAP_UNORDERED: int
MAP_WRITE_FLAGS_CREATE_ONLY: int
MAP_WRITE_FLAGS_DEFAULT: int
MAP_WRITE_FLAGS_NO_FAIL: int
MAP_WRITE_FLAGS_PARTIAL: int
MAP_WRITE_FLAGS_UPDATE_ONLY: int
MAP_WRITE_NO_FAIL: int
MAP_WRITE_PARTIAL: int
OPERATOR_APPEND: int
OPERATOR_DELETE: int
OPERATOR_INCR: int
OPERATOR_PREPEND: int
OPERATOR_READ: int
OPERATOR_TOUCH: int
OPERATOR_WRITE: int
OP_BIT_ADD: int
OP_BIT_AND: int
OP_BIT_COUNT: int
OP_BIT_GET: int
OP_BIT_GET_INT: int
OP_BIT_INSERT: int
OP_BIT_LSCAN: int
OP_BIT_LSHIFT: int
OP_BIT_NOT: int
OP_BIT_OR: int
OP_BIT_REMOVE: int
OP_BIT_RESIZE: int
OP_BIT_RSCAN: int
OP_BIT_RSHIFT: int
OP_BIT_SET: int
OP_BIT_SET_INT: int
OP_BIT_SUBTRACT: int
OP_BIT_XOR: int
OP_EXPR_READ: int
OP_EXPR_WRITE: int
OP_HLL_ADD: int
OP_HLL_DESCRIBE: int
OP_HLL_FOLD: int
OP_HLL_GET_COUNT: int
OP_HLL_GET_INTERSECT_COUNT: int
OP_HLL_GET_SIMILARITY: int
OP_HLL_GET_UNION: int
OP_HLL_GET_UNION_COUNT: int
OP_HLL_INIT: int
OP_HLL_MAY_CONTAIN: int
OP_HLL_REFRESH_COUNT: int
OP_HLL_SET_UNION: int
OP_LIST_APPEND: int
OP_LIST_APPEND_ITEMS: int
OP_LIST_CLEAR: int
OP_LIST_CREATE: int
OP_LIST_GET: int
OP_LIST_GET_BY_INDEX: int
OP_LIST_GET_BY_INDEX_RANGE: int
OP_LIST_GET_BY_INDEX_RANGE_TO_END: int
OP_LIST_GET_BY_RANK: int
OP_LIST_GET_BY_RANK_RANGE: int
OP_LIST_GET_BY_RANK_RANGE_TO_END: int
OP_LIST_GET_BY_VALUE: int
OP_LIST_GET_BY_VALUE_LIST: int
OP_LIST_GET_BY_VALUE_RANGE: int
OP_LIST_GET_BY_VALUE_RANK_RANGE_REL: int
OP_LIST_GET_BY_VALUE_RANK_RANGE_REL_TO_END: int
OP_LIST_GET_RANGE: int
OP_LIST_INCREMENT: int
OP_LIST_INSERT: int
OP_LIST_INSERT_ITEMS: int
OP_LIST_POP: int
OP_LIST_POP_RANGE: int
OP_LIST_REMOVE: int
OP_LIST_REMOVE_BY_INDEX: int
OP_LIST_REMOVE_BY_INDEX_RANGE: int
OP_LIST_REMOVE_BY_INDEX_RANGE_TO_END: int
OP_LIST_REMOVE_BY_RANK: int
OP_LIST_REMOVE_BY_RANK_RANGE: int
OP_LIST_REMOVE_BY_RANK_RANGE_TO_END: int
OP_LIST_REMOVE_BY_REL_RANK_RANGE: int
OP_LIST_REMOVE_BY_REL_RANK_RANGE_TO_END: int
OP_LIST_REMOVE_BY_VALUE: int
OP_LIST_REMOVE_BY_VALUE_LIST: int
OP_LIST_REMOVE_BY_VALUE_RANGE: int
OP_LIST_REMOVE_BY_VALUE_RANK_RANGE_REL: int
OP_LIST_REMOVE_RANGE: int
OP_LIST_SET: int
OP_LIST_SET_ORDER: int
OP_LIST_SIZE: int
OP_LIST_SORT: int
OP_LIST_TRIM: int
OP_MAP_CLEAR: int
OP_MAP_CREATE: int
OP_MAP_DECREMENT: int
OP_MAP_GET_BY_INDEX: int
OP_MAP_GET_BY_INDEX_RANGE: int
OP_MAP_GET_BY_INDEX_RANGE_TO_END: int
OP_MAP_GET_BY_KEY: int
OP_MAP_GET_BY_KEY_INDEX_RANGE_REL: int
OP_MAP_GET_BY_KEY_LIST: int
OP_MAP_GET_BY_KEY_RANGE: int
OP_MAP_GET_BY_KEY_REL_INDEX_RANGE: int
OP_MAP_GET_BY_KEY_REL_INDEX_RANGE_TO_END: int
OP_MAP_GET_BY_RANK: int
OP_MAP_GET_BY_RANK_RANGE: int
OP_MAP_GET_BY_RANK_RANGE_TO_END: int
OP_MAP_GET_BY_VALUE: int
OP_MAP_GET_BY_VALUE_LIST: int
OP_MAP_GET_BY_VALUE_RANGE: int
OP_MAP_GET_BY_VALUE_RANK_RANGE_REL: int
OP_MAP_GET_BY_VALUE_RANK_RANGE_REL_TO_END: int
OP_MAP_INCREMENT: int
OP_MAP_PUT: int
OP_MAP_PUT_ITEMS: int
OP_MAP_REMOVE_BY_INDEX: int
OP_MAP_REMOVE_BY_INDEX_RANGE: int
OP_MAP_REMOVE_BY_INDEX_RANGE_TO_END: int
OP_MAP_REMOVE_BY_KEY: int
OP_MAP_REMOVE_BY_KEY_INDEX_RANGE_REL: int
OP_MAP_REMOVE_BY_KEY_LIST: int
OP_MAP_REMOVE_BY_KEY_RANGE: int
OP_MAP_REMOVE_BY_KEY_REL_INDEX_RANGE: int
OP_MAP_REMOVE_BY_KEY_REL_INDEX_RANGE_TO_END: int
OP_MAP_REMOVE_BY_RANK: int
OP_MAP_REMOVE_BY_RANK_RANGE: int
OP_MAP_REMOVE_BY_RANK_RANGE_TO_END: int
OP_MAP_REMOVE_BY_VALUE: int
OP_MAP_REMOVE_BY_VALUE_LIST: int
OP_MAP_REMOVE_BY_VALUE_RANGE: int
OP_MAP_REMOVE_BY_VALUE_RANK_RANGE_REL: int
OP_MAP_REMOVE_BY_VALUE_REL_INDEX_RANGE: int
OP_MAP_REMOVE_BY_VALUE_REL_RANK_RANGE: int
OP_MAP_REMOVE_BY_VALUE_REL_RANK_RANGE_TO_END: int
OP_MAP_SET_POLICY: int
OP_MAP_SIZE: int
POLICY_COMMIT_LEVEL_ALL: int
POLICY_COMMIT_LEVEL_MASTER: int
POLICY_EXISTS_CREATE: int
POLICY_EXISTS_CREATE_OR_REPLACE: int
POLICY_EXISTS_IGNORE: int
POLICY_EXISTS_REPLACE: int
POLICY_EXISTS_UPDATE: int
POLICY_GEN_EQ: int
POLICY_GEN_GT: int
POLICY_GEN_IGNORE: int
POLICY_KEY_DIGEST: int
POLICY_KEY_SEND: int
POLICY_READ_MODE_AP_ALL: int
POLICY_READ_MODE_AP_ONE: int
POLICY_READ_MODE_SC_ALLOW_REPLICA: int
POLICY_READ_MODE_SC_ALLOW_UNAVAILABLE: int
POLICY_READ_MODE_SC_LINEARIZE: int
POLICY_READ_MODE_SC_SESSION: int
POLICY_REPLICA_ANY: int
POLICY_REPLICA_MASTER: int
POLICY_REPLICA_PREFER_RACK: int
POLICY_REPLICA_RANDOM: int
POLICY_REPLICA_SEQUENCE: int
POLICY_RETRY_NONE: int
POLICY_RETRY_ONCE: int
PRIV_DATA_ADMIN: int
PRIV_READ: int
PRIV_READ_WRITE: int
PRIV_READ_WRITE_UDF: int
PRIV_SINDEX_ADMIN: int
PRIV_SYS_ADMIN: int
PRIV_TRUNCATE: int
PRIV_UDF_ADMIN: int
PRIV_USER_ADMIN: int
PRIV_WRITE: int
QUERY_DURATION_LONG: int
QUERY_DURATION_LONG_RELAX_AP: int
QUERY_DURATION_SHORT: int
REGEX_EXTENDED: int
REGEX_ICASE: int
REGEX_NEWLINE: int
REGEX_NONE: int
REGEX_NOSUB: int
SERIALIZER_JSON: int
SERIALIZER_NONE: int
SERIALIZER_USER: int
TTL_CLIENT_DEFAULT: int
TTL_DONT_UPDATE: int
TTL_NAMESPACE_DEFAULT: int
TTL_NEVER_EXPIRE: int
TXN_STATE_ABORTED: int
TXN_STATE_COMMITTED: int
TXN_STATE_OPEN: int
TXN_STATE_VERIFIED: int
UDF_TYPE_LUA: int

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
