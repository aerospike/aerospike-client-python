from typing import Any, Callable, Union, final, Literal, Optional, Final

from aerospike_helpers.batch.records import BatchRecords
from aerospike_helpers.metrics import MetricsPolicy

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

@final
class CDTInfinite:
    def __init__(self, *args, **kwargs) -> None: ...

@final
class CDTWildcard:
    def __init__(self, *args, **kwargs) -> None: ...

@final
class Transaction:
    def __init__(self, reads_capacity: int = 128, writes_capacity: int = 128) -> None: ...
    id: int
    in_doubt: bool
    state: int
    timeout: int

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
    def batch_operate(self, keys: list, ops: list, policy_batch: dict = ..., policy_batch_write: dict = ..., ttl: int = ...) -> BatchRecords: ...
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
    def index_blob_create(self, ns: str, set: str, bin: str, name: str, policy: dict = ...) -> None: ...
    def info_all(self, command: str, policy: dict = ...) -> dict: ...
    def info_random_node(self, command: str, policy: dict = ...) -> str: ...
    def info_single_node(self, command: str, host: str, policy: dict = ...) -> str: ...
    def is_connected(self) -> bool: ...
    def job_info(self, job_id: int, module, policy: dict = ...) -> dict: ...
    def enable_metrics(self, policy: Optional[MetricsPolicy] = None) -> None: ...
    def disable_metrics(self) -> None: ...
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
    def commit(self, transaction: Transaction) -> int: ...
    def abort(self, transaction: Transaction) -> int: ...

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
    ttl: int
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
