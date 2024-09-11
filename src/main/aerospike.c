/*******************************************************************************
 * Copyright 2013-2021 Aerospike, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

#include <Python.h>
#include <stdbool.h>
#include <stdint.h>
#include <string.h>

#include "client.h"
#include "query.h"
#include "geo.h"
#include "scan.h"
#include "key_ordered_dict.h"
#include "predicates.h"
#include "exceptions.h"
#include "policy.h"
#include "log.h"
#include <aerospike/as_operations.h>
#include "serializer.h"
#include "module_functions.h"
#include "nullobject.h"
#include "cdt_types.h"
#include <aerospike/as_log_macros.h>
#include <aerospike/as_job.h>
#include <aerospike/as_admin.h>
#include <aerospike/as_record.h>

PyObject *py_global_hosts;
int counter = 0xA8000000;
bool user_shm_key = false;

PyDoc_STRVAR(client_doc, "client(config) -> client object\n\
\n\
Creates a new instance of the Client class.\n\
This client can connect() to the cluster and perform operations against it, such as put() and get() records.\n\
\n\
config = {\n\
    'hosts':    [ ('127.0.0.1', 3000) ],\n\
    'policies': {'timeout': 1000},\n\
}\n\
client = aerospike.client(config)");

static PyMethodDef Aerospike_Methods[] = {

    //Serialization
    {"set_serializer", (PyCFunction)AerospikeClient_Set_Serializer,
     METH_VARARGS | METH_KEYWORDS, "Sets the serializer"},
    {"set_deserializer", (PyCFunction)AerospikeClient_Set_Deserializer,
     METH_VARARGS | METH_KEYWORDS, "Sets the deserializer"},
    {"unset_serializers", (PyCFunction)AerospikeClient_Unset_Serializers,
     METH_VARARGS | METH_KEYWORDS, "Unsets the serializer and deserializer"},

    {"client", (PyCFunction)AerospikeClient_New, METH_VARARGS | METH_KEYWORDS,
     client_doc},
    {"set_log_level", (PyCFunction)Aerospike_Set_Log_Level,
     METH_VARARGS | METH_KEYWORDS, "Sets the log level"},
    {"set_log_handler", (PyCFunction)Aerospike_Set_Log_Handler,
     METH_VARARGS | METH_KEYWORDS, "Enables the log handler"},
    {"geodata", (PyCFunction)Aerospike_Set_Geo_Data,
     METH_VARARGS | METH_KEYWORDS,
     "Creates a GeoJSON object from geospatial data."},
    {"geojson", (PyCFunction)Aerospike_Set_Geo_Json,
     METH_VARARGS | METH_KEYWORDS,
     "Creates a GeoJSON object from a raw GeoJSON string."},

    //Calculate the digest of a key
    {"calc_digest", (PyCFunction)Aerospike_Calc_Digest,
     METH_VARARGS | METH_KEYWORDS, "Calculate the digest of a key"},

    //Get partition ID for given digest
    {"get_partition_id", (PyCFunction)Aerospike_Get_Partition_Id, METH_VARARGS,
     "Get partition ID for given digest"},

    {NULL}};

static struct module_member_name_to_as_const_value module_constants[] = {
    {"OPERATOR_READ", AS_OPERATOR_READ},
    {"OPERATOR_WRITE", AS_OPERATOR_WRITE},
    {"OPERATOR_INCR", AS_OPERATOR_INCR},
    {"OPERATOR_APPEND", AS_OPERATOR_APPEND},
    {"OPERATOR_PREPEND", AS_OPERATOR_PREPEND},
    {"OPERATOR_TOUCH", AS_OPERATOR_TOUCH},
    {"OPERATOR_DELETE", AS_OPERATOR_DELETE},

    {"AUTH_INTERNAL", AS_AUTH_INTERNAL},
    {"AUTH_EXTERNAL", AS_AUTH_EXTERNAL},
    {"AUTH_EXTERNAL_INSECURE", AS_AUTH_EXTERNAL_INSECURE},
    {"AUTH_PKI", AS_AUTH_PKI},

    {"POLICY_RETRY_NONE", AS_POLICY_RETRY_NONE},
    {"POLICY_RETRY_ONCE", AS_POLICY_RETRY_ONCE},

    {"POLICY_EXISTS_IGNORE", AS_POLICY_EXISTS_IGNORE},
    {"POLICY_EXISTS_CREATE", AS_POLICY_EXISTS_CREATE},
    {"POLICY_EXISTS_UPDATE", AS_POLICY_EXISTS_UPDATE},
    {"POLICY_EXISTS_REPLACE", AS_POLICY_EXISTS_REPLACE},
    {"POLICY_EXISTS_CREATE_OR_REPLACE", AS_POLICY_EXISTS_CREATE_OR_REPLACE},

    {"UDF_TYPE_LUA", AS_UDF_TYPE_LUA},

    {"POLICY_KEY_DIGEST", AS_POLICY_KEY_DIGEST},
    {"POLICY_KEY_SEND", AS_POLICY_KEY_SEND},
    {"POLICY_GEN_IGNORE", AS_POLICY_GEN_IGNORE},
    {"POLICY_GEN_EQ", AS_POLICY_GEN_EQ},
    {"POLICY_GEN_GT", AS_POLICY_GEN_GT},

    {"JOB_STATUS_COMPLETED", AS_JOB_STATUS_COMPLETED},
    {"JOB_STATUS_UNDEF", AS_JOB_STATUS_UNDEF},
    {"JOB_STATUS_INPROGRESS", AS_JOB_STATUS_INPROGRESS},

    {"POLICY_REPLICA_MASTER", AS_POLICY_REPLICA_MASTER},
    {"POLICY_REPLICA_ANY", AS_POLICY_REPLICA_ANY},
    {"POLICY_REPLICA_SEQUENCE", AS_POLICY_REPLICA_SEQUENCE},
    {"POLICY_REPLICA_PREFER_RACK", AS_POLICY_REPLICA_PREFER_RACK},

    {"POLICY_COMMIT_LEVEL_ALL", AS_POLICY_COMMIT_LEVEL_ALL},
    {"POLICY_COMMIT_LEVEL_MASTER", AS_POLICY_COMMIT_LEVEL_MASTER},

    {"SERIALIZER_USER", SERIALIZER_USER},
    {"SERIALIZER_JSON", SERIALIZER_JSON},
    {"SERIALIZER_NONE", SERIALIZER_NONE},

    {"INTEGER", SEND_BOOL_AS_INTEGER},
    {"AS_BOOL", SEND_BOOL_AS_AS_BOOL},

    {"INDEX_STRING", AS_INDEX_STRING},
    {"INDEX_NUMERIC", AS_INDEX_NUMERIC},
    {"INDEX_GEO2DSPHERE", AS_INDEX_GEO2DSPHERE},
    {"INDEX_BLOB", AS_INDEX_BLOB},
    {"INDEX_TYPE_DEFAULT", AS_INDEX_TYPE_DEFAULT},
    {"INDEX_TYPE_LIST", AS_INDEX_TYPE_LIST},
    {"INDEX_TYPE_MAPKEYS", AS_INDEX_TYPE_MAPKEYS},
    {"INDEX_TYPE_MAPVALUES", AS_INDEX_TYPE_MAPVALUES},

    {"PRIV_USER_ADMIN", AS_PRIVILEGE_USER_ADMIN},
    {"PRIV_SYS_ADMIN", AS_PRIVILEGE_SYS_ADMIN},
    {"PRIV_DATA_ADMIN", AS_PRIVILEGE_DATA_ADMIN},
    {"PRIV_READ", AS_PRIVILEGE_READ},
    {"PRIV_WRITE", AS_PRIVILEGE_WRITE},
    {"PRIV_READ_WRITE", AS_PRIVILEGE_READ_WRITE},
    {"PRIV_READ_WRITE_UDF", AS_PRIVILEGE_READ_WRITE_UDF},
    {"PRIV_TRUNCATE", AS_PRIVILEGE_TRUNCATE},
    {"PRIV_UDF_ADMIN", AS_PRIVILEGE_UDF_ADMIN},
    {"PRIV_SINDEX_ADMIN", AS_PRIVILEGE_SINDEX_ADMIN},

    {"OP_LIST_APPEND", OP_LIST_APPEND},
    {"OP_LIST_APPEND_ITEMS", OP_LIST_APPEND_ITEMS},
    {"OP_LIST_INSERT", OP_LIST_INSERT},
    {"OP_LIST_INSERT_ITEMS", OP_LIST_INSERT_ITEMS},
    {"OP_LIST_POP", OP_LIST_POP},
    {"OP_LIST_POP_RANGE", OP_LIST_POP_RANGE},
    {"OP_LIST_REMOVE", OP_LIST_REMOVE},
    {"OP_LIST_REMOVE_RANGE", OP_LIST_REMOVE_RANGE},
    {"OP_LIST_CLEAR", OP_LIST_CLEAR},
    {"OP_LIST_SET", OP_LIST_SET},
    {"OP_LIST_GET", OP_LIST_GET},
    {"OP_LIST_GET_RANGE", OP_LIST_GET_RANGE},
    {"OP_LIST_TRIM", OP_LIST_TRIM},
    {"OP_LIST_SIZE", OP_LIST_SIZE},
    {"OP_LIST_INCREMENT", OP_LIST_INCREMENT},

    {"OP_MAP_SET_POLICY", OP_MAP_SET_POLICY},
    {"OP_MAP_CREATE", OP_MAP_CREATE},
    {"OP_MAP_PUT", OP_MAP_PUT},
    {"OP_MAP_PUT_ITEMS", OP_MAP_PUT_ITEMS},
    {"OP_MAP_INCREMENT", OP_MAP_INCREMENT},
    {"OP_MAP_DECREMENT", OP_MAP_DECREMENT},
    {"OP_MAP_SIZE", OP_MAP_SIZE},
    {"OP_MAP_CLEAR", OP_MAP_CLEAR},
    {"OP_MAP_REMOVE_BY_KEY", OP_MAP_REMOVE_BY_KEY},
    {"OP_MAP_REMOVE_BY_KEY_LIST", OP_MAP_REMOVE_BY_KEY_LIST},
    {"OP_MAP_REMOVE_BY_KEY_RANGE", OP_MAP_REMOVE_BY_KEY_RANGE},
    {"OP_MAP_REMOVE_BY_VALUE", OP_MAP_REMOVE_BY_VALUE},
    {"OP_MAP_REMOVE_BY_VALUE_LIST", OP_MAP_REMOVE_BY_VALUE_LIST},
    {"OP_MAP_REMOVE_BY_VALUE_RANGE", OP_MAP_REMOVE_BY_VALUE_RANGE},
    {"OP_MAP_REMOVE_BY_INDEX", OP_MAP_REMOVE_BY_INDEX},
    {"OP_MAP_REMOVE_BY_INDEX_RANGE", OP_MAP_REMOVE_BY_INDEX_RANGE},
    {"OP_MAP_REMOVE_BY_RANK", OP_MAP_REMOVE_BY_RANK},
    {"OP_MAP_REMOVE_BY_RANK_RANGE", OP_MAP_REMOVE_BY_RANK_RANGE},
    {"OP_MAP_GET_BY_KEY", OP_MAP_GET_BY_KEY},
    {"OP_MAP_GET_BY_KEY_RANGE", OP_MAP_GET_BY_KEY_RANGE},
    {"OP_MAP_GET_BY_VALUE", OP_MAP_GET_BY_VALUE},
    {"OP_MAP_GET_BY_VALUE_RANGE", OP_MAP_GET_BY_VALUE_RANGE},
    {"OP_MAP_GET_BY_INDEX", OP_MAP_GET_BY_INDEX},
    {"OP_MAP_GET_BY_INDEX_RANGE", OP_MAP_GET_BY_INDEX_RANGE},
    {"OP_MAP_GET_BY_RANK", OP_MAP_GET_BY_RANK},
    {"OP_MAP_GET_BY_RANK_RANGE", OP_MAP_GET_BY_RANK_RANGE},
    {"OP_MAP_GET_BY_VALUE_LIST", OP_MAP_GET_BY_VALUE_LIST},
    {"OP_MAP_GET_BY_KEY_LIST", OP_MAP_GET_BY_KEY_LIST},

    {"MAP_UNORDERED", AS_MAP_UNORDERED},
    {"MAP_KEY_ORDERED", AS_MAP_KEY_ORDERED},
    {"MAP_KEY_VALUE_ORDERED", AS_MAP_KEY_VALUE_ORDERED},

    {"MAP_RETURN_NONE", AS_MAP_RETURN_NONE},
    {"MAP_RETURN_INDEX", AS_MAP_RETURN_INDEX},
    {"MAP_RETURN_REVERSE_INDEX", AS_MAP_RETURN_REVERSE_INDEX},
    {"MAP_RETURN_RANK", AS_MAP_RETURN_RANK},
    {"MAP_RETURN_REVERSE_RANK", AS_MAP_RETURN_REVERSE_RANK},
    {"MAP_RETURN_COUNT", AS_MAP_RETURN_COUNT},
    {"MAP_RETURN_KEY", AS_MAP_RETURN_KEY},
    {"MAP_RETURN_VALUE", AS_MAP_RETURN_VALUE},
    {"MAP_RETURN_KEY_VALUE", AS_MAP_RETURN_KEY_VALUE},
    {"MAP_RETURN_EXISTS", AS_MAP_RETURN_EXISTS},
    {"MAP_RETURN_ORDERED_MAP", AS_MAP_RETURN_ORDERED_MAP},
    {"MAP_RETURN_UNORDERED_MAP", AS_MAP_RETURN_UNORDERED_MAP},

    {"TTL_NAMESPACE_DEFAULT", AS_RECORD_DEFAULT_TTL},
    {"TTL_NEVER_EXPIRE", AS_RECORD_NO_EXPIRE_TTL},
    {"TTL_DONT_UPDATE", AS_RECORD_NO_CHANGE_TTL},
    {"TTL_CLIENT_DEFAULT", AS_RECORD_CLIENT_DEFAULT_TTL},

    /* New CDT Operations, post 3.16.0.1 */
    {"OP_LIST_GET_BY_INDEX", OP_LIST_GET_BY_INDEX},
    {"OP_LIST_GET_BY_INDEX_RANGE", OP_LIST_GET_BY_INDEX_RANGE},
    {"OP_LIST_GET_BY_RANK", OP_LIST_GET_BY_RANK},
    {"OP_LIST_GET_BY_RANK_RANGE", OP_LIST_GET_BY_RANK_RANGE},
    {"OP_LIST_GET_BY_VALUE", OP_LIST_GET_BY_VALUE},
    {"OP_LIST_GET_BY_VALUE_LIST", OP_LIST_GET_BY_VALUE_LIST},
    {"OP_LIST_GET_BY_VALUE_RANGE", OP_LIST_GET_BY_VALUE_RANGE},
    {"OP_LIST_REMOVE_BY_INDEX", OP_LIST_REMOVE_BY_INDEX},
    {"OP_LIST_REMOVE_BY_INDEX_RANGE", OP_LIST_REMOVE_BY_INDEX_RANGE},
    {"OP_LIST_REMOVE_BY_RANK", OP_LIST_REMOVE_BY_RANK},
    {"OP_LIST_REMOVE_BY_RANK_RANGE", OP_LIST_REMOVE_BY_RANK_RANGE},
    {"OP_LIST_REMOVE_BY_VALUE", OP_LIST_REMOVE_BY_VALUE},
    {"OP_LIST_REMOVE_BY_VALUE_LIST", OP_LIST_REMOVE_BY_VALUE_LIST},
    {"OP_LIST_REMOVE_BY_VALUE_RANGE", OP_LIST_REMOVE_BY_VALUE_RANGE},
    {"OP_LIST_SET_ORDER", OP_LIST_SET_ORDER},
    {"OP_LIST_SORT", OP_LIST_SORT},
    {OP_LIST_REMOVE_BY_VALUE_RANK_RANGE_REL,
     "OP_LIST_REMOVE_BY_VALUE_RANK_RANGE_REL"},
    {OP_LIST_GET_BY_VALUE_RANK_RANGE_REL,
     "OP_LIST_GET_BY_VALUE_RANK_RANGE_REL"},
    {"OP_LIST_CREATE", OP_LIST_CREATE},

    {"LIST_RETURN_NONE", AS_LIST_RETURN_NONE},
    {"LIST_RETURN_INDEX", AS_LIST_RETURN_INDEX},
    {"LIST_RETURN_REVERSE_INDEX", AS_LIST_RETURN_REVERSE_INDEX},
    {"LIST_RETURN_RANK", AS_LIST_RETURN_RANK},
    {"LIST_RETURN_REVERSE_RANK", AS_LIST_RETURN_REVERSE_RANK},
    {"LIST_RETURN_COUNT", AS_LIST_RETURN_COUNT},
    {"LIST_RETURN_VALUE", AS_LIST_RETURN_VALUE},
    {"LIST_RETURN_EXISTS", AS_LIST_RETURN_EXISTS},

    {"LIST_SORT_DROP_DUPLICATES", AS_LIST_SORT_DROP_DUPLICATES},
    {"LIST_SORT_DEFAULT", AS_LIST_SORT_DEFAULT},

    {"LIST_WRITE_DEFAULT", AS_LIST_WRITE_DEFAULT},
    {"LIST_WRITE_ADD_UNIQUE", AS_LIST_WRITE_ADD_UNIQUE},
    {"LIST_WRITE_INSERT_BOUNDED", AS_LIST_WRITE_INSERT_BOUNDED},

    {"LIST_ORDERED", AS_LIST_ORDERED},
    {"LIST_UNORDERED", AS_LIST_UNORDERED},

    /* CDT operations for use with expressions, new in 5.0 */
    {OP_MAP_REMOVE_BY_VALUE_RANK_RANGE_REL,
     "OP_MAP_REMOVE_BY_VALUE_RANK_RANGE_REL"},
    {OP_MAP_REMOVE_BY_KEY_INDEX_RANGE_REL,
     "OP_MAP_REMOVE_BY_KEY_INDEX_RANGE_REL"},
    {"OP_MAP_GET_BY_VALUE_RANK_RANGE_REL", OP_MAP_GET_BY_VALUE_RANK_RANGE_REL},
    {"OP_MAP_GET_BY_KEY_INDEX_RANGE_REL", OP_MAP_GET_BY_KEY_INDEX_RANGE_REL},

    {OP_LIST_GET_BY_VALUE_RANK_RANGE_REL_TO_END,
     "OP_LIST_GET_BY_VALUE_RANK_RANGE_REL_TO_END"},
    {"OP_LIST_GET_BY_INDEX_RANGE_TO_END", OP_LIST_GET_BY_INDEX_RANGE_TO_END},
    {"OP_LIST_GET_BY_RANK_RANGE_TO_END", OP_LIST_GET_BY_RANK_RANGE_TO_END},
    {OP_LIST_REMOVE_BY_REL_RANK_RANGE_TO_END,
     "OP_LIST_REMOVE_BY_REL_RANK_RANGE_TO_END"},
    {"OP_LIST_REMOVE_BY_REL_RANK_RANGE", OP_LIST_REMOVE_BY_REL_RANK_RANGE},
    {OP_LIST_REMOVE_BY_INDEX_RANGE_TO_END,
     "OP_LIST_REMOVE_BY_INDEX_RANGE_TO_END"},
    {OP_LIST_REMOVE_BY_RANK_RANGE_TO_END,
     "OP_LIST_REMOVE_BY_RANK_RANGE_TO_END"},

    {"MAP_WRITE_NO_FAIL", AS_MAP_WRITE_NO_FAIL},
    {"MAP_WRITE_PARTIAL", AS_MAP_WRITE_PARTIAL},

    {"LIST_WRITE_NO_FAIL", AS_LIST_WRITE_NO_FAIL},
    {"LIST_WRITE_PARTIAL", AS_LIST_WRITE_PARTIAL},

    /* Map write flags post 3.5.0 */
    {"MAP_WRITE_FLAGS_DEFAULT", AS_MAP_WRITE_DEFAULT},
    {"MAP_WRITE_FLAGS_CREATE_ONLY", AS_MAP_WRITE_CREATE_ONLY},
    {"MAP_WRITE_FLAGS_UPDATE_ONLY", AS_MAP_WRITE_UPDATE_ONLY},
    {"MAP_WRITE_FLAGS_NO_FAIL", AS_MAP_WRITE_NO_FAIL},
    {"MAP_WRITE_FLAGS_PARTIAL", AS_MAP_WRITE_PARTIAL},

    /* READ Mode constants 4.0.0 */

    // AP Read Mode
    {"POLICY_READ_MODE_AP_ONE", AS_POLICY_READ_MODE_AP_ONE},
    {"POLICY_READ_MODE_AP_ALL", AS_POLICY_READ_MODE_AP_ALL},
    // SC Read Mode
    {"POLICY_READ_MODE_SC_SESSION", AS_POLICY_READ_MODE_SC_SESSION},
    {"POLICY_READ_MODE_SC_LINEARIZE", AS_POLICY_READ_MODE_SC_LINEARIZE},
    {"POLICY_READ_MODE_SC_ALLOW_REPLICA", AS_POLICY_READ_MODE_SC_ALLOW_REPLICA},
    {AS_POLICY_READ_MODE_SC_ALLOW_UNAVAILABLE,
     "POLICY_READ_MODE_SC_ALLOW_UNAVAILABLE"},

    /* Bitwise constants: 3.9.0 */
    {"BIT_WRITE_DEFAULT", AS_BIT_WRITE_DEFAULT},
    {"BIT_WRITE_CREATE_ONLY", AS_BIT_WRITE_CREATE_ONLY},
    {"BIT_WRITE_UPDATE_ONLY", AS_BIT_WRITE_UPDATE_ONLY},
    {"BIT_WRITE_NO_FAIL", AS_BIT_WRITE_NO_FAIL},
    {"BIT_WRITE_PARTIAL", AS_BIT_WRITE_PARTIAL},

    {"BIT_RESIZE_DEFAULT", AS_BIT_RESIZE_DEFAULT},
    {"BIT_RESIZE_FROM_FRONT", AS_BIT_RESIZE_FROM_FRONT},
    {"BIT_RESIZE_GROW_ONLY", AS_BIT_RESIZE_GROW_ONLY},
    {"BIT_RESIZE_SHRINK_ONLY", AS_BIT_RESIZE_SHRINK_ONLY},

    {"BIT_OVERFLOW_FAIL", AS_BIT_OVERFLOW_FAIL},
    {"BIT_OVERFLOW_SATURATE", AS_BIT_OVERFLOW_SATURATE},
    {"BIT_OVERFLOW_WRAP", AS_BIT_OVERFLOW_WRAP},

    /* BITWISE OPS: 3.9.0 */
    {"OP_BIT_INSERT", OP_BIT_INSERT},
    {"OP_BIT_RESIZE", OP_BIT_RESIZE},
    {"OP_BIT_REMOVE", OP_BIT_REMOVE},
    {"OP_BIT_SET", OP_BIT_SET},
    {"OP_BIT_OR", OP_BIT_OR},
    {"OP_BIT_XOR", OP_BIT_XOR},
    {"OP_BIT_AND", OP_BIT_AND},
    {"OP_BIT_NOT", OP_BIT_NOT},
    {"OP_BIT_LSHIFT", OP_BIT_LSHIFT},
    {"OP_BIT_RSHIFT", OP_BIT_RSHIFT},
    {"OP_BIT_ADD", OP_BIT_ADD},
    {"OP_BIT_SUBTRACT", OP_BIT_SUBTRACT},
    {"OP_BIT_GET_INT", OP_BIT_GET_INT},
    {"OP_BIT_SET_INT", OP_BIT_SET_INT},
    {"OP_BIT_GET", OP_BIT_GET},
    {"OP_BIT_COUNT", OP_BIT_COUNT},
    {"OP_BIT_LSCAN", OP_BIT_LSCAN},
    {"OP_BIT_RSCAN", OP_BIT_RSCAN},

    /* Nested CDT constants: 3.9.0 */
    {"CDT_CTX_LIST_INDEX", AS_CDT_CTX_LIST_INDEX},
    {"CDT_CTX_LIST_RANK", AS_CDT_CTX_LIST_RANK},
    {"CDT_CTX_LIST_VALUE", AS_CDT_CTX_LIST_VALUE},
    {"CDT_CTX_LIST_INDEX_CREATE", CDT_CTX_LIST_INDEX_CREATE},
    {"CDT_CTX_MAP_INDEX", AS_CDT_CTX_MAP_INDEX},
    {"CDT_CTX_MAP_RANK", AS_CDT_CTX_MAP_RANK},
    {"CDT_CTX_MAP_KEY", AS_CDT_CTX_MAP_KEY},
    {"CDT_CTX_MAP_VALUE", AS_CDT_CTX_MAP_VALUE},
    {"CDT_CTX_MAP_KEY_CREATE", CDT_CTX_MAP_KEY_CREATE},

    /* HLL constants 3.11.0 */
    {"OP_HLL_ADD", OP_HLL_ADD},
    {"OP_HLL_DESCRIBE", OP_HLL_DESCRIBE},
    {"OP_HLL_FOLD", OP_HLL_FOLD},
    {"OP_HLL_GET_COUNT", OP_HLL_GET_COUNT},
    {"OP_HLL_GET_INTERSECT_COUNT", OP_HLL_GET_INTERSECT_COUNT},
    {"OP_HLL_GET_SIMILARITY", OP_HLL_GET_SIMILARITY},
    {"OP_HLL_GET_UNION", OP_HLL_GET_UNION},
    {"OP_HLL_GET_UNION_COUNT", OP_HLL_GET_UNION_COUNT},
    {"OP_HLL_GET_SIMILARITY", OP_HLL_GET_SIMILARITY},
    {"OP_HLL_INIT", OP_HLL_INIT},
    {"OP_HLL_REFRESH_COUNT", OP_HLL_REFRESH_COUNT},
    {"OP_HLL_SET_UNION", OP_HLL_SET_UNION},
    {"OP_HLL_MAY_CONTAIN", OP_HLL_MAY_CONTAIN}, // for expression filters

    {"HLL_WRITE_DEFAULT", AS_HLL_WRITE_DEFAULT},
    {"HLL_WRITE_CREATE_ONLY", AS_HLL_WRITE_CREATE_ONLY},
    {"HLL_WRITE_UPDATE_ONLY", AS_HLL_WRITE_UPDATE_ONLY},
    {"HLL_WRITE_NO_FAIL", AS_HLL_WRITE_NO_FAIL},
    {"HLL_WRITE_ALLOW_FOLD", AS_HLL_WRITE_ALLOW_FOLD},

    {OP_MAP_REMOVE_BY_KEY_REL_INDEX_RANGE_TO_END,
     "OP_MAP_REMOVE_BY_KEY_REL_INDEX_RANGE_TO_END"},
    {OP_MAP_REMOVE_BY_VALUE_REL_RANK_RANGE_TO_END,
     "OP_MAP_REMOVE_BY_VALUE_REL_RANK_RANGE_TO_END"},
    {OP_MAP_REMOVE_BY_INDEX_RANGE_TO_END,
     "OP_MAP_REMOVE_BY_INDEX_RANGE_TO_END"},
    {"OP_MAP_REMOVE_BY_RANK_RANGE_TO_END", OP_MAP_REMOVE_BY_RANK_RANGE_TO_END},
    {OP_MAP_GET_BY_KEY_REL_INDEX_RANGE_TO_END,
     "OP_MAP_GET_BY_KEY_REL_INDEX_RANGE_TO_END"},
    {OP_MAP_REMOVE_BY_KEY_REL_INDEX_RANGE,
     "OP_MAP_REMOVE_BY_KEY_REL_INDEX_RANGE"},
    {OP_MAP_REMOVE_BY_VALUE_REL_INDEX_RANGE,
     "OP_MAP_REMOVE_BY_VALUE_REL_INDEX_RANGE"},
    {OP_MAP_REMOVE_BY_VALUE_REL_RANK_RANGE,
     "OP_MAP_REMOVE_BY_VALUE_REL_RANK_RANGE"},
    {"OP_MAP_GET_BY_KEY_REL_INDEX_RANGE", OP_MAP_GET_BY_KEY_REL_INDEX_RANGE},
    {OP_MAP_GET_BY_VALUE_RANK_RANGE_REL_TO_END,
     "OP_MAP_GET_BY_VALUE_RANK_RANGE_REL_TO_END"},
    {"OP_MAP_GET_BY_INDEX_RANGE_TO_END", OP_MAP_GET_BY_INDEX_RANGE_TO_END},
    {"OP_MAP_GET_BY_RANK_RANGE_TO_END", OP_MAP_GET_BY_RANK_RANGE_TO_END},

    /* Expression operation constants 5.1.0 */
    {"OP_EXPR_READ", OP_EXPR_READ},
    {"OP_EXPR_WRITE", OP_EXPR_WRITE},
    {"EXP_WRITE_DEFAULT", AS_EXP_WRITE_DEFAULT},
    {"EXP_WRITE_CREATE_ONLY", AS_EXP_WRITE_CREATE_ONLY},
    {"EXP_WRITE_UPDATE_ONLY", AS_EXP_WRITE_UPDATE_ONLY},
    {"EXP_WRITE_ALLOW_DELETE", AS_EXP_WRITE_ALLOW_DELETE},
    {"EXP_WRITE_POLICY_NO_FAIL", AS_EXP_WRITE_POLICY_NO_FAIL},
    {"EXP_WRITE_EVAL_NO_FAIL", AS_EXP_WRITE_EVAL_NO_FAIL},
    {"EXP_READ_DEFAULT", AS_EXP_READ_DEFAULT},
    {"EXP_READ_EVAL_NO_FAIL", AS_EXP_READ_EVAL_NO_FAIL},

    /* For BinType expression, as_bytes_type */
    {"AS_BYTES_UNDEF", AS_BYTES_UNDEF},
    {"AS_BYTES_INTEGER", AS_BYTES_INTEGER},
    {"AS_BYTES_DOUBLE", AS_BYTES_DOUBLE},
    {"AS_BYTES_STRING", AS_BYTES_STRING},
    {"AS_BYTES_BLOB", AS_BYTES_BLOB},
    {"AS_BYTES_JAVA", AS_BYTES_JAVA},
    {"AS_BYTES_CSHARP", AS_BYTES_CSHARP},
    {"AS_BYTES_PYTHON", AS_BYTES_PYTHON},
    {"AS_BYTES_RUBY", AS_BYTES_RUBY},
    {"AS_BYTES_PHP", AS_BYTES_PHP},
    {"AS_BYTES_ERLANG", AS_BYTES_ERLANG},
    {"AS_BYTES_BOOL", AS_BYTES_BOOL},
    {"AS_BYTES_HLL", AS_BYTES_HLL},
    {"AS_BYTES_MAP", AS_BYTES_MAP},
    {"AS_BYTES_LIST", AS_BYTES_LIST},
    {"AS_BYTES_GEOJSON", AS_BYTES_GEOJSON},
    {"AS_BYTES_TYPE_MAX", AS_BYTES_TYPE_MAX},

    /* Regex constants from predexp, still used by expressions */
    {"REGEX_NONE", REGEX_NONE},
    {"REGEX_EXTENDED", REGEX_EXTENDED},
    {"REGEX_ICASE", REGEX_ICASE},
    {"REGEX_NOSUB", REGEX_NOSUB},
    {"REGEX_NEWLINE", REGEX_NEWLINE},

    {"QUERY_DURATION_LONG", AS_QUERY_DURATION_LONG},
    {"QUERY_DURATION_LONG_RELAX_AP", AS_QUERY_DURATION_LONG_RELAX_AP},
    {"QUERY_DURATION_SHORT", AS_QUERY_DURATION_SHORT}};

static PyObject (*const pyobject_creation_methods[])(void) = {
    AerospikeException_New,        AerospikePredicates_New,
    AerospikeClient_Ready,         AerospikeQuery_Ready,
    AerospikeGeospatial_Ready,     AerospikeNullObject_Ready,
    AerospikeWildcardObject_Ready, AerospikeInfiniteObject_Ready,
};

PyMODINIT_FUNC PyInit_aerospike(void)
{
    // TODO: use macro for module name
    static struct PyModuleDef moduledef = {
        PyModuleDef_HEAD_INIT,
        .m_name = "aerospike",
        .m_doc = "Aerospike Python Client",
        .m_methods = Aerospike_Methods,
    };

    PyObject *py_aerospike_module = PyModule_Create(&moduledef);
    if (py_aerospike_module == NULL) {
        return NULL;
    }

    Aerospike_Enable_Default_Logging();

    py_global_hosts = PyDict_New();
    if (py_global_hosts == NULL) {
        goto MODULE_CLEANUP_ON_ERROR;
    }

    int i = 0;
    int retval;
    for (int i = 0; i < sizeof(pyobject_creation_methods) /
                            sizeof(pyobject_creation_methods[0]);
         i++) {
        PyObject *(*create_pyobject)(void) = pyobject_creation_methods[i];
        PyObject *py_member = create_pyobject();
        if (py_member == NULL) {
            goto GLOBAL_HOSTS_CLEANUP_ON_ERROR;
        }

        // Get name of pyobject
        PyObject *py_member_name =
            PyObject_GetAttrString(py_member, "__name__");
        if (py_member_name == NULL) {
            goto MODULE_MEMBER_CLEANUP_ON_ERROR;
        }

        const char *member_name = PyUnicode_AsUTF8(py_member_name);
        Py_DECREF(py_member_name);
        if (member_name == NULL) {
            goto MODULE_MEMBER_CLEANUP_ON_ERROR;
        }

        retval =
            PyModule_AddObject(py_aerospike_module, member_name, py_member);
        if (retval == -1) {
            goto MODULE_MEMBER_CLEANUP_ON_ERROR;
        }
        continue;

    MODULE_MEMBER_CLEANUP_ON_ERROR:
        Py_DECREF(py_member);
        goto GLOBAL_HOSTS_CLEANUP_ON_ERROR;
    }

    /*
	 * Add constants to module.
	 */
    for (i = 0; i < sizeof(module_constants) / sizeof(module_constants[0]);
         i++) {
        retval = PyModule_AddIntConstant(py_aerospike_module,
                                         module_constants[i].member_name,
                                         module_constants[i].value);
        if (retval == -1) {
            goto GLOBAL_HOSTS_CLEANUP_ON_ERROR;
        }
    }

    retval = declare_policy_constants(py_aerospike_module);
    if (retval == -1) {
        goto GLOBAL_HOSTS_CLEANUP_ON_ERROR;
    }
    retval = declare_log_constants(py_aerospike_module);
    if (retval == -1) {
        goto GLOBAL_HOSTS_CLEANUP_ON_ERROR;
    }

    return py_aerospike_module;

GLOBAL_HOSTS_CLEANUP_ON_ERROR:
    Py_DECREF(py_global_hosts);
MODULE_CLEANUP_ON_ERROR:
    Py_DECREF(py_aerospike_module);
    // Aerospike_Clear(py_aerospike_module);
    return NULL;
}
