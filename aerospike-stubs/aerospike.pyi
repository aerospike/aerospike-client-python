from typing import Any, Callable, Union, Literal
from typing_extensions import final

from aerospike_helpers.batch.records import BatchRecords
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
INDEX_GEO2DSPHERE: Literal[2]
INDEX_NUMERIC: Literal[1]
INDEX_STRING: Literal[0]
INDEX_TYPE_DEFAULT: Literal[0]
INDEX_TYPE_LIST: Literal[1]
INDEX_TYPE_MAPKEYS: Literal[2]
INDEX_TYPE_MAPVALUES: Literal[3]
INTEGER: Literal[0]
JOB_QUERY: Literal['query']
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
LIST_WRITE_DEFAULT: int
LIST_WRITE_ADD_UNIQUE: int
LIST_WRITE_INSERT_BOUNDED: int
LIST_WRITE_NO_FAIL: int
LIST_WRITE_PARTIAL: int
LOG_LEVEL_DEBUG: int
LOG_LEVEL_ERROR: int
LOG_LEVEL_INFO: int
LOG_LEVEL_OFF: int
LOG_LEVEL_TRACE: int
LOG_LEVEL_WARN: int
MAP_CREATE_ONLY: int
MAP_KEY_ORDERED: int
MAP_KEY_VALUE_ORDERED: int
MAP_RETURN_COUNT: int
MAP_RETURN_EXISTS: int
MAP_RETURN_INDEX: int
MAP_RETURN_KEY: int
MAP_RETURN_KEY_VALUE: int
MAP_RETURN_NONE: int
MAP_RETURN_RANK: int
MAP_RETURN_REVERSE_INDEX: int
MAP_RETURN_REVERSE_RANK: int
MAP_RETURN_VALUE: int
MAP_RETURN_ORDERED_MAP: int
MAP_RETURN_UNORDERED_MAP: int
MAP_UNORDERED: int
MAP_UPDATE: int
MAP_UPDATE_ONLY: int
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
REGEX_EXTENDED: int
REGEX_ICASE: int
REGEX_NEWLINE: int
REGEX_NONE: int
REGEX_NOSUB: int
SERIALIZER_JSON: int
SERIALIZER_NONE: int
SERIALIZER_USER: int
TTL_DONT_UPDATE: int
TTL_NAMESPACE_DEFAULT: int
TTL_NEVER_EXPIRE: int
UDF_TYPE_LUA: int

@final
class CDTInfinite:
    def __init__(self, *args, **kwargs) -> None: ...

@final
class CDTWildcard:
    def __init__(self, *args, **kwargs) -> None: ...

class Client:
    def __init__(self, *args, **kwargs) -> None: ...
    def admin_change_password(self, username: str, password: str, policy: dict = ...) -> None: ...
    def admin_create_role(self, role: str, privileges: list, policy: dict = ..., whitelist: list = ..., read_quota: int = ..., write_quota: int = ...) -> None: ...
    def admin_create_user(self, username: str, password: str, roles: list, policy: dict = ...) -> None: ...
    def admin_drop_role(self, role: str, policy: dict = ...) -> None: ...
    def admin_drop_user(self, username: str, policy: dict = ...) -> None: ...
    def admin_get_role(self, role: str, policy: dict = ...) -> dict: ...
    def admin_get_roles(self, policy: dict = ...) -> dict: ...
    def admin_grant_privileges(self, role: str, privileges: list, policy: dict = ...) -> None: ...
    def admin_grant_roles(self, username: str, roles: list, policy: dict = ...) -> None: ...
    def admin_query_role(self, role: str, policy: dict = ...) -> list: ...
    def admin_query_roles(self, policy: dict = ...) -> dict: ...
    def admin_query_user(self, username: str, policy: dict = ...) -> list: ...
    def admin_query_user_info(self, *args, **kwargs) -> Any: ...
    def admin_query_users(self, policy: dict = ...) -> dict: ...
    def admin_query_users_info(self, *args, **kwargs) -> Any: ...
    def admin_revoke_privileges(self, role: str, privileges: list, policy: dict = ...) -> None: ...
    def admin_revoke_roles(self, username: str, roles: list, policy: dict = ...) -> None: ...
    def admin_set_password(self, username: str, password: str, policy: dict = ...) -> None: ...
    def admin_set_quotas(self, role: str, read_quota: int = ..., write_quota: int = ..., policy: dict = ...) -> None: ...
    def admin_set_whitelist(self, role: str, whitelist: list, policy: dict = ...) -> None: ...
    def append(self, key: tuple, bin: str, val: str, meta: dict = ..., policy: dict = ...) -> None: ...
    def apply(self, key: tuple, module: str, function: str, args: list, policy: dict = ...) -> Union[str, int, float, bytearray, list, dict]: ...
    def batch_apply(self, keys: list, module: str, function: str, args: list, policy_batch: dict = ..., policy_batch_apply: dict = ...) -> BatchRecords: ...
    def batch_get_ops(self, keys: list, ops: list, policy: dict) -> list: ...
    def batch_operate(self, keys: list, ops: list, policy_batch: dict = ..., policy_batch_write: dict = ...) -> BatchRecords: ...
    def batch_remove(self, keys: list, policy_batch: dict = ..., policy_batch_remove: dict = ...) -> BatchRecords: ...
    def batch_read(self, keys: list, bins: list[str] = ..., policy_batch: dict = ...) -> BatchRecords: ...
    def batch_write(self, batch_records: BatchRecords, policy_batch: dict = ...) -> BatchRecords: ...
    def close(self) -> None: ...
    def connect(self, username: str = ..., password: str = ...) -> Client: ...
    def exists(self, key: tuple, policy: dict = ...) -> tuple: ...
    def exists_many(self, keys: list, policy: dict = ...) -> list: ...
    def get(self, key: tuple, policy: dict = ...) -> tuple: ...
    def get_cdtctx_base64(self, ctx: list) -> str: ...
    def get_expression_base64(self, expression) -> str: ...
    def get_key_partition_id(self, ns, set, key) -> int: ...
    def get_many(self, keys: list, policy: dict = ...) -> list: ...
    def get_node_names(self) -> list: ...
    def get_nodes(self) -> list: ...
    def increment(self, key: tuple, bin: str, offset: int, meta: dict = ..., policy: dict = ...) -> None: ...
    def index_cdt_create(self, *args, **kwargs) -> Any: ...
    def index_geo2dsphere_create(self, ns: str, set: str, bin: str, name: str, policy: dict = ...) -> None: ...
    def index_integer_create(self, ns: str, set: str, bin: str, name: str, policy: dict = ...) -> None: ...
    def index_list_create(self, ns: str, set: str, bin: str, index_datatype, name: str, policy: dict = ...) -> None: ...
    def index_map_keys_create(self, ns: str, set: str, bin: str, index_datatype, name: str, policy: dict = ...) -> None: ...
    def index_map_values_create(self, ns: str, set: str, bin: str, index_datatype, name: str, policy: dict = ...) -> None: ...
    def index_remove(self, ns, name: str, policy: dict = ...) -> None: ...
    def index_string_create(self, ns: str, set: str, bin: str, name: str, policy: dict = ...) -> None: ...
    def info_all(self, command: str, policy: dict = ...) -> dict: ...
    def info_random_node(self, command: str, policy: dict = ...) -> str: ...
    def info_single_node(self, command: str, host: str, policy: dict = ...) -> str: ...
    def is_connected(self) -> bool: ...
    def job_info(self, job_id: int, module, policy: dict = ...) -> dict: ...
    # List and map operations in the aerospike module are deprecated and undocumented
    # def list_append(self, *args, **kwargs) -> Any: ...
    # def list_clear(self, *args, **kwargs) -> Any: ...
    # def list_extend(self, *args, **kwargs) -> Any: ...
    # def list_get(self, *args, **kwargs) -> Any: ...
    # def list_get_range(self, *args, **kwargs) -> Any: ...
    # def list_insert(self, *args, **kwargs) -> Any: ...
    # def list_insert_items(self, *args, **kwargs) -> Any: ...
    # def list_pop(self, *args, **kwargs) -> Any: ...
    # def list_pop_range(self, *args, **kwargs) -> Any: ...
    # def list_remove(self, *args, **kwargs) -> Any: ...
    # def list_remove_range(self, *args, **kwargs) -> Any: ...
    # def list_set(self, *args, **kwargs) -> Any: ...
    # def list_size(self, *args, **kwargs) -> Any: ...
    # def list_trim(self, *args, **kwargs) -> Any: ...
    # def map_clear(self, *args, **kwargs) -> Any: ...
    # def map_decrement(self, *args, **kwargs) -> Any: ...
    # def map_get_by_index(self, *args, **kwargs) -> Any: ...
    # def map_get_by_index_range(self, *args, **kwargs) -> Any: ...
    # def map_get_by_key(self, *args, **kwargs) -> Any: ...
    # def map_get_by_key_list(self, *args, **kwargs) -> Any: ...
    # def map_get_by_key_range(self, *args, **kwargs) -> Any: ...
    # def map_get_by_rank(self, *args, **kwargs) -> Any: ...
    # def map_get_by_rank_range(self, *args, **kwargs) -> Any: ...
    # def map_get_by_value(self, *args, **kwargs) -> Any: ...
    # def map_get_by_value_list(self, *args, **kwargs) -> Any: ...
    # def map_get_by_value_range(self, *args, **kwargs) -> Any: ...
    # def map_increment(self, *args, **kwargs) -> Any: ...
    # def map_put(self, *args, **kwargs) -> Any: ...
    # def map_put_items(self, *args, **kwargs) -> Any: ...
    # def map_remove_by_index(self, *args, **kwargs) -> Any: ...
    # def map_remove_by_index_range(self, *args, **kwargs) -> Any: ...
    # def map_remove_by_key(self, *args, **kwargs) -> Any: ...
    # def map_remove_by_key_list(self, *args, **kwargs) -> Any: ...
    # def map_remove_by_key_range(self, *args, **kwargs) -> Any: ...
    # def map_remove_by_rank(self, *args, **kwargs) -> Any: ...
    # def map_remove_by_rank_range(self, *args, **kwargs) -> Any: ...
    # def map_remove_by_value(self, *args, **kwargs) -> Any: ...
    # def map_remove_by_value_list(self, *args, **kwargs) -> Any: ...
    # def map_remove_by_value_range(self, *args, **kwargs) -> Any: ...
    # def map_set_policy(self, key, bin, map_policy) -> Any: ...
    # def map_size(self, *args, **kwargs) -> Any: ...
    def operate(self, key: tuple, list: list, meta: dict = ..., policy: dict = ...) -> tuple: ...
    def operate_ordered(self, key: tuple, list: list, meta: dict = ..., policy: dict = ...) -> list: ...
    def prepend(self, key: tuple, bin: str, val: str, meta: dict = ..., policy: dict = ...) -> None: ...
    def put(self, key: tuple, bins: dict, meta: dict = ..., policy: dict = ..., serializer = ...) -> None: ...
    def query(self, namespace: str, set: str = ...) -> Query: ...
    def query_apply(self, ns: str, set: str, predicate: tuple, module: str, function: str, args: list = ..., policy: dict = ...) -> int: ...
    def remove(self, key: tuple, meta: dict = ..., policy: dict = ...) -> None: ...
    def remove_bin(self, key: tuple, list: list, meta: dict = ..., policy: dict = ...) -> None: ...
    def scan(self, namespace: str, set: str = ...) -> Scan: ...
    def scan_apply(self, ns: str, set: str, module: str, function: str, args: list = ..., policy: dict = ..., options: dict = ...) -> int: ...
    def select(self, *args, **kwargs) -> tuple: ...
    def select_many(self, keys: list, bins: list, policy: dict = ...) -> list: ...
    def set_xdr_filter(self, data_center: str, namespace: str, expression_filter, policy: dict = ...) -> str: ...
    def shm_key(self) -> Union[int, None]: ...
    def touch(self, key: tuple, val: int = ..., meta: dict = ..., policy: dict = ...) -> None: ...
    def truncate(self, namespace: str, set: str, nanos: int, policy: dict = ...) -> int: ...
    def udf_get(self, module: str, language: int = ..., policy: dict = ...) -> str: ...
    def udf_list(self, policy: dict = ...) -> list: ...
    def udf_put(self, filename: str, udf_type = ..., policy: dict = ...) -> None: ...
    def udf_remove(self, module: str, policy: dict = ...) -> None: ...

class GeoJSON:
    geo_data: Any
    def __init__(self, geo_data: Union[str, dict] = ...) -> None: ...
    def dumps(self) -> str: ...
    def loads(self, raw_geo: str) -> None: ...
    def unwrap(self) -> dict: ...
    def wrap(self, geo_data: dict) -> None: ...

class KeyOrderedDict(dict):
    def __init__(self, *args, **kwargs) -> None: ...

class Query:
    max_records: int
    records_per_second: int
    ttl: int
    def __init__(self, *args, **kwargs) -> None: ...
    def add_ops(self, ops: list) -> None: ...
    def apply(self, module: str, function: str, arguments: list = ...) -> Any: ...
    def execute_background(self, policy: dict = ...) -> int: ...
    def foreach(self, callback: Callable, policy: dict = ..., options: dict = ...) -> None: ...
    def get_partitions_status(self) -> tuple: ...
    def is_done(self) -> bool: ...
    def paginate(self) -> None: ...
    def results(self, policy: dict = ..., options: dict = ...) -> list: ...
    # TODO: this isn't an infinite list of bins
    def select(self, *args, **kwargs) -> None: ...
    def where(self, predicate: tuple, ctx: list = ...) -> None: ...

class Scan:
    def __init__(self, *args, **kwargs) -> None: ...
    def add_ops(self, ops: list) -> None: ...
    def apply(self, module: str, function: str, arguments: list = ...) -> Any: ...
    def foreach(self, callback: Callable, policy: dict = ..., options: dict = ..., nodename: str = ...) -> None: ...
    def execute_background(self, policy: dict = ...) -> int: ...
    def get_partitions_status(self) -> tuple: ...
    def is_done(self) -> bool: ...
    def paginate(self) -> None: ...
    def results(self, policy: dict = ..., nodename: str = ...) -> list: ...
    # TODO: this isn't an infinite list of bins
    def select(self, *args, **kwargs) -> None: ...

@final
class null:
    def __init__(self, *args, **kwargs) -> None: ...

def calc_digest(ns: str, set: str, key: Union[str, int, bytearray]) -> bytearray: ...
def client(config: dict) -> Client: ...
def geodata(geo_data: dict) -> GeoJSON: ...
def geojson(geojson_str: str) -> GeoJSON: ...
def get_cdtctx_base64(ctx: list) -> str: ...
def get_expression_base64(expression) -> str: ...
def get_partition_id(*args, **kwargs) -> Any: ...
def set_deserializer(callback: Callable) -> None: ...
def set_log_handler(callback: Callable = ...) -> None: ...
def set_log_level(log_level: int) -> None: ...
def set_serializer(callback: Callable) -> None: ...
def unset_serializers() -> None: ...
