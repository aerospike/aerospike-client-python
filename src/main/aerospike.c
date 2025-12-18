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
#include "serializer.h"
#include "module_functions.h"
#include "nullobject.h"
#include "cdt_types.h"
#include "transaction.h"
#include "config_provider.h"

#include <aerospike/as_operations.h>
#include <aerospike/as_log_macros.h>
#include <aerospike/as_job.h>
#include <aerospike/as_admin.h>
#include <aerospike/as_record.h>
#include <aerospike/as_exp_operations.h>
#include <aerospike/aerospike_txn.h>
#include <aerospike/version.h>

PyObject *py_global_hosts = NULL;
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

static PyMethodDef aerospike_methods[] = {

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

struct module_constant_name_to_value {
    const char *name;
    // If false, is int value
    bool is_str_value;
    union value {
        long integer;
        const char *string;
    } value;
};

#define EXPOSE_AS_MACRO_WITHOUT_AS_PREFIX_AS_PUBLIC_FIELD(                     \
    macro_name_without_prefix)                                                 \
    {                                                                          \
        #macro_name_without_prefix,                                            \
            .value.integer = AS_##macro_name_without_prefix                    \
    }

#define STRINGIFY(X) #X

#define EXPOSE_AS_MACRO_AS_PRIVATE_FIELD(macro_name_without_prefix)            \
    {                                                                          \
        STRINGIFY(_##macro_name_without_prefix),                               \
            .value.integer = macro_name_without_prefix                         \
    }

#define EXPOSE_MACRO(macro_name)                                               \
    {                                                                          \
        #macro_name, .value.integer = macro_name                               \
    }

#define EXPOSE_STRING_MACRO_FOR_AEROSPIKE_HELPERS(macro_name)                  \
    {                                                                          \
        #macro_name, .is_str_value = true, .value.string = macro_name          \
    }

// TODO: many of these names are the same as the enum name
// Is there a way to generate this code?
// TODO: regression tests for all these constants
static struct module_constant_name_to_value module_constants[] = {
    {"OPERATOR_READ", .value.integer = AS_OPERATOR_READ},
    {"OPERATOR_WRITE", .value.integer = AS_OPERATOR_WRITE},
    {"OPERATOR_INCR", .value.integer = AS_OPERATOR_INCR},
    {"OPERATOR_APPEND", .value.integer = AS_OPERATOR_APPEND},
    {"OPERATOR_PREPEND", .value.integer = AS_OPERATOR_PREPEND},
    {"OPERATOR_TOUCH", .value.integer = AS_OPERATOR_TOUCH},
    {"OPERATOR_DELETE", .value.integer = AS_OPERATOR_DELETE},
    EXPOSE_AS_MACRO_AS_PRIVATE_FIELD(AS_OPERATOR_CDT_READ),
    EXPOSE_AS_MACRO_AS_PRIVATE_FIELD(AS_OPERATOR_CDT_MODIFY),

    {"AUTH_INTERNAL", .value.integer = AS_AUTH_INTERNAL},
    {"AUTH_EXTERNAL", .value.integer = AS_AUTH_EXTERNAL},
    {"AUTH_EXTERNAL_INSECURE", .value.integer = AS_AUTH_EXTERNAL_INSECURE},
    {"AUTH_PKI", .value.integer = AS_AUTH_PKI},

    {"POLICY_RETRY_NONE", .value.integer = AS_POLICY_RETRY_NONE},
    {"POLICY_RETRY_ONCE", .value.integer = AS_POLICY_RETRY_ONCE},

    {"POLICY_EXISTS_IGNORE", .value.integer = AS_POLICY_EXISTS_IGNORE},
    {"POLICY_EXISTS_CREATE", .value.integer = AS_POLICY_EXISTS_CREATE},
    {"POLICY_EXISTS_UPDATE", .value.integer = AS_POLICY_EXISTS_UPDATE},
    {"POLICY_EXISTS_REPLACE", .value.integer = AS_POLICY_EXISTS_REPLACE},
    {"POLICY_EXISTS_CREATE_OR_REPLACE",
     .value.integer = AS_POLICY_EXISTS_CREATE_OR_REPLACE},

    {"UDF_TYPE_LUA", .value.integer = AS_UDF_TYPE_LUA},

    {"POLICY_KEY_DIGEST", .value.integer = AS_POLICY_KEY_DIGEST},
    {"POLICY_KEY_SEND", .value.integer = AS_POLICY_KEY_SEND},
    {"POLICY_GEN_IGNORE", .value.integer = AS_POLICY_GEN_IGNORE},
    {"POLICY_GEN_EQ", .value.integer = AS_POLICY_GEN_EQ},
    {"POLICY_GEN_GT", .value.integer = AS_POLICY_GEN_GT},

    {"JOB_STATUS_COMPLETED", .value.integer = AS_JOB_STATUS_COMPLETED},
    {"JOB_STATUS_UNDEF", .value.integer = AS_JOB_STATUS_UNDEF},
    {"JOB_STATUS_INPROGRESS", .value.integer = AS_JOB_STATUS_INPROGRESS},

    {"POLICY_REPLICA_MASTER", .value.integer = AS_POLICY_REPLICA_MASTER},
    {"POLICY_REPLICA_ANY", .value.integer = AS_POLICY_REPLICA_ANY},
    {"POLICY_REPLICA_SEQUENCE", .value.integer = AS_POLICY_REPLICA_SEQUENCE},
    {"POLICY_REPLICA_PREFER_RACK",
     .value.integer = AS_POLICY_REPLICA_PREFER_RACK},
    {"POLICY_REPLICA_RANDOM", .value.integer = AS_POLICY_REPLICA_RANDOM},

    {"POLICY_COMMIT_LEVEL_ALL", .value.integer = AS_POLICY_COMMIT_LEVEL_ALL},
    {"POLICY_COMMIT_LEVEL_MASTER",
     .value.integer = AS_POLICY_COMMIT_LEVEL_MASTER},

    {"SERIALIZER_USER", .value.integer = SERIALIZER_USER},
    {"SERIALIZER_JSON", .value.integer = SERIALIZER_JSON},
    {"SERIALIZER_NONE", .value.integer = SERIALIZER_NONE},

    {"INTEGER", .value.integer = SEND_BOOL_AS_INTEGER},
    {"AS_BOOL", .value.integer = SEND_BOOL_AS_AS_BOOL},

    {"INDEX_STRING", .value.integer = AS_INDEX_STRING},
    {"INDEX_NUMERIC", .value.integer = AS_INDEX_NUMERIC},
    {"INDEX_GEO2DSPHERE", .value.integer = AS_INDEX_GEO2DSPHERE},
    {"INDEX_BLOB", .value.integer = AS_INDEX_BLOB},
    {"INDEX_TYPE_DEFAULT", .value.integer = AS_INDEX_TYPE_DEFAULT},
    {"INDEX_TYPE_LIST", .value.integer = AS_INDEX_TYPE_LIST},
    {"INDEX_TYPE_MAPKEYS", .value.integer = AS_INDEX_TYPE_MAPKEYS},
    {"INDEX_TYPE_MAPVALUES", .value.integer = AS_INDEX_TYPE_MAPVALUES},

    {"PRIV_USER_ADMIN", .value.integer = AS_PRIVILEGE_USER_ADMIN},
    {"PRIV_SYS_ADMIN", .value.integer = AS_PRIVILEGE_SYS_ADMIN},
    {"PRIV_DATA_ADMIN", .value.integer = AS_PRIVILEGE_DATA_ADMIN},
    {"PRIV_READ", .value.integer = AS_PRIVILEGE_READ},
    {"PRIV_WRITE", .value.integer = AS_PRIVILEGE_WRITE},
    {"PRIV_READ_WRITE", .value.integer = AS_PRIVILEGE_READ_WRITE},
    {"PRIV_READ_WRITE_UDF", .value.integer = AS_PRIVILEGE_READ_WRITE_UDF},
    {"PRIV_TRUNCATE", .value.integer = AS_PRIVILEGE_TRUNCATE},
    {"PRIV_UDF_ADMIN", .value.integer = AS_PRIVILEGE_UDF_ADMIN},
    {"PRIV_SINDEX_ADMIN", .value.integer = AS_PRIVILEGE_SINDEX_ADMIN},

    // TODO: If only aerospike_helpers relies on these constants,
    // maybe move these constants to aerospike_helpers package
    {"OP_LIST_APPEND", .value.integer = OP_LIST_APPEND},
    {"OP_LIST_APPEND_ITEMS", .value.integer = OP_LIST_APPEND_ITEMS},
    {"OP_LIST_INSERT", .value.integer = OP_LIST_INSERT},
    {"OP_LIST_INSERT_ITEMS", .value.integer = OP_LIST_INSERT_ITEMS},
    {"OP_LIST_POP", .value.integer = OP_LIST_POP},
    {"OP_LIST_POP_RANGE", .value.integer = OP_LIST_POP_RANGE},
    {"OP_LIST_REMOVE", .value.integer = OP_LIST_REMOVE},
    {"OP_LIST_REMOVE_RANGE", .value.integer = OP_LIST_REMOVE_RANGE},
    {"OP_LIST_CLEAR", .value.integer = OP_LIST_CLEAR},
    {"OP_LIST_SET", .value.integer = OP_LIST_SET},
    {"OP_LIST_GET", .value.integer = OP_LIST_GET},
    {"OP_LIST_GET_RANGE", .value.integer = OP_LIST_GET_RANGE},
    {"OP_LIST_TRIM", .value.integer = OP_LIST_TRIM},
    {"OP_LIST_SIZE", .value.integer = OP_LIST_SIZE},
    {"OP_LIST_INCREMENT", .value.integer = OP_LIST_INCREMENT},
    /* New CDT Operations, post 3.16.0.1 */
    {"OP_LIST_GET_BY_INDEX", .value.integer = OP_LIST_GET_BY_INDEX},
    {"OP_LIST_GET_BY_INDEX_RANGE", .value.integer = OP_LIST_GET_BY_INDEX_RANGE},
    {"OP_LIST_GET_BY_RANK", .value.integer = OP_LIST_GET_BY_RANK},
    {"OP_LIST_GET_BY_RANK_RANGE", .value.integer = OP_LIST_GET_BY_RANK_RANGE},
    {"OP_LIST_GET_BY_VALUE", .value.integer = OP_LIST_GET_BY_VALUE},
    {"OP_LIST_GET_BY_VALUE_LIST", .value.integer = OP_LIST_GET_BY_VALUE_LIST},
    {"OP_LIST_GET_BY_VALUE_RANGE", .value.integer = OP_LIST_GET_BY_VALUE_RANGE},
    {"OP_LIST_REMOVE_BY_INDEX", .value.integer = OP_LIST_REMOVE_BY_INDEX},
    {"OP_LIST_REMOVE_BY_INDEX_RANGE",
     .value.integer = OP_LIST_REMOVE_BY_INDEX_RANGE},
    {"OP_LIST_REMOVE_BY_RANK", .value.integer = OP_LIST_REMOVE_BY_RANK},
    {"OP_LIST_REMOVE_BY_RANK_RANGE",
     .value.integer = OP_LIST_REMOVE_BY_RANK_RANGE},
    {"OP_LIST_REMOVE_BY_VALUE", .value.integer = OP_LIST_REMOVE_BY_VALUE},
    {"OP_LIST_REMOVE_BY_VALUE_LIST",
     .value.integer = OP_LIST_REMOVE_BY_VALUE_LIST},
    {"OP_LIST_REMOVE_BY_VALUE_RANGE",
     .value.integer = OP_LIST_REMOVE_BY_VALUE_RANGE},
    {"OP_LIST_SET_ORDER", .value.integer = OP_LIST_SET_ORDER},
    {"OP_LIST_SORT", .value.integer = OP_LIST_SORT},
    {
        "OP_LIST_REMOVE_BY_VALUE_RANK_RANGE_REL",

        .value.integer = OP_LIST_REMOVE_BY_VALUE_RANK_RANGE_REL,
    },
    {
        "OP_LIST_GET_BY_VALUE_RANK_RANGE_REL",

        .value.integer = OP_LIST_GET_BY_VALUE_RANK_RANGE_REL,
    },
    {"OP_LIST_CREATE", .value.integer = OP_LIST_CREATE},
    {
        "OP_LIST_GET_BY_VALUE_RANK_RANGE_REL_TO_END",

        .value.integer = OP_LIST_GET_BY_VALUE_RANK_RANGE_REL_TO_END,
    },
    {"OP_LIST_GET_BY_INDEX_RANGE_TO_END",
     .value.integer = OP_LIST_GET_BY_INDEX_RANGE_TO_END},
    {"OP_LIST_GET_BY_RANK_RANGE_TO_END",
     .value.integer = OP_LIST_GET_BY_RANK_RANGE_TO_END},
    {"OP_LIST_REMOVE_BY_REL_RANK_RANGE_TO_END",
     .value.integer = OP_LIST_REMOVE_BY_REL_RANK_RANGE_TO_END},
    {"OP_LIST_REMOVE_BY_REL_RANK_RANGE",
     .value.integer = OP_LIST_REMOVE_BY_REL_RANK_RANGE},
    {"OP_LIST_REMOVE_BY_INDEX_RANGE_TO_END",
     .value.integer = OP_LIST_REMOVE_BY_INDEX_RANGE_TO_END},
    {"OP_LIST_REMOVE_BY_RANK_RANGE_TO_END",
     .value.integer = OP_LIST_REMOVE_BY_RANK_RANGE_TO_END},

    {"OP_MAP_SET_POLICY", .value.integer = OP_MAP_SET_POLICY},
    {"OP_MAP_CREATE", .value.integer = OP_MAP_CREATE},
    {"OP_MAP_PUT", .value.integer = OP_MAP_PUT},
    {"OP_MAP_PUT_ITEMS", .value.integer = OP_MAP_PUT_ITEMS},
    {"OP_MAP_INCREMENT", .value.integer = OP_MAP_INCREMENT},
    {"OP_MAP_DECREMENT", .value.integer = OP_MAP_DECREMENT},
    {"OP_MAP_SIZE", .value.integer = OP_MAP_SIZE},
    {"OP_MAP_CLEAR", .value.integer = OP_MAP_CLEAR},
    {"OP_MAP_REMOVE_BY_KEY", .value.integer = OP_MAP_REMOVE_BY_KEY},
    {"OP_MAP_REMOVE_BY_KEY_LIST", .value.integer = OP_MAP_REMOVE_BY_KEY_LIST},
    {"OP_MAP_REMOVE_BY_KEY_RANGE", .value.integer = OP_MAP_REMOVE_BY_KEY_RANGE},
    {"OP_MAP_REMOVE_BY_VALUE", .value.integer = OP_MAP_REMOVE_BY_VALUE},
    {"OP_MAP_REMOVE_BY_VALUE_LIST",
     .value.integer = OP_MAP_REMOVE_BY_VALUE_LIST},
    {"OP_MAP_REMOVE_BY_VALUE_RANGE",
     .value.integer = OP_MAP_REMOVE_BY_VALUE_RANGE},
    {"OP_MAP_REMOVE_BY_INDEX", .value.integer = OP_MAP_REMOVE_BY_INDEX},
    {"OP_MAP_REMOVE_BY_INDEX_RANGE",
     .value.integer = OP_MAP_REMOVE_BY_INDEX_RANGE},
    {"OP_MAP_REMOVE_BY_RANK", .value.integer = OP_MAP_REMOVE_BY_RANK},
    {"OP_MAP_REMOVE_BY_RANK_RANGE",
     .value.integer = OP_MAP_REMOVE_BY_RANK_RANGE},
    {"OP_MAP_GET_BY_KEY", .value.integer = OP_MAP_GET_BY_KEY},
    {"OP_MAP_GET_BY_KEY_RANGE", .value.integer = OP_MAP_GET_BY_KEY_RANGE},
    {"OP_MAP_GET_BY_VALUE", .value.integer = OP_MAP_GET_BY_VALUE},
    {"OP_MAP_GET_BY_VALUE_RANGE", .value.integer = OP_MAP_GET_BY_VALUE_RANGE},
    {"OP_MAP_GET_BY_INDEX", .value.integer = OP_MAP_GET_BY_INDEX},
    {"OP_MAP_GET_BY_INDEX_RANGE", .value.integer = OP_MAP_GET_BY_INDEX_RANGE},
    {"OP_MAP_GET_BY_RANK", .value.integer = OP_MAP_GET_BY_RANK},
    {"OP_MAP_GET_BY_RANK_RANGE", .value.integer = OP_MAP_GET_BY_RANK_RANGE},
    {"OP_MAP_GET_BY_VALUE_LIST", .value.integer = OP_MAP_GET_BY_VALUE_LIST},
    {"OP_MAP_GET_BY_KEY_LIST", .value.integer = OP_MAP_GET_BY_KEY_LIST},
    /* CDT operations for use with expressions, new in 5.0 */
    {"OP_MAP_REMOVE_BY_VALUE_RANK_RANGE_REL",
     .value.integer = OP_MAP_REMOVE_BY_VALUE_RANK_RANGE_REL},
    {"OP_MAP_REMOVE_BY_KEY_INDEX_RANGE_REL",
     .value.integer = OP_MAP_REMOVE_BY_KEY_INDEX_RANGE_REL},
    {"OP_MAP_GET_BY_VALUE_RANK_RANGE_REL",
     .value.integer = OP_MAP_GET_BY_VALUE_RANK_RANGE_REL},
    {"OP_MAP_GET_BY_KEY_INDEX_RANGE_REL",
     .value.integer = OP_MAP_GET_BY_KEY_INDEX_RANGE_REL},
    {"OP_MAP_REMOVE_BY_KEY_REL_INDEX_RANGE_TO_END",
     .value.integer = OP_MAP_REMOVE_BY_KEY_REL_INDEX_RANGE_TO_END},
    {"OP_MAP_REMOVE_BY_VALUE_REL_RANK_RANGE_TO_END",
     .value.integer = OP_MAP_REMOVE_BY_VALUE_REL_RANK_RANGE_TO_END},
    {"OP_MAP_REMOVE_BY_INDEX_RANGE_TO_END",
     .value.integer = OP_MAP_REMOVE_BY_INDEX_RANGE_TO_END},
    {"OP_MAP_REMOVE_BY_RANK_RANGE_TO_END",
     .value.integer = OP_MAP_REMOVE_BY_RANK_RANGE_TO_END},
    {"OP_MAP_GET_BY_KEY_REL_INDEX_RANGE_TO_END",
     .value.integer = OP_MAP_GET_BY_KEY_REL_INDEX_RANGE_TO_END},
    {"OP_MAP_REMOVE_BY_KEY_REL_INDEX_RANGE",
     .value.integer = OP_MAP_REMOVE_BY_KEY_REL_INDEX_RANGE},
    {"OP_MAP_REMOVE_BY_VALUE_REL_INDEX_RANGE",
     .value.integer = OP_MAP_REMOVE_BY_VALUE_REL_INDEX_RANGE},
    {"OP_MAP_REMOVE_BY_VALUE_REL_RANK_RANGE",
     .value.integer = OP_MAP_REMOVE_BY_VALUE_REL_RANK_RANGE},
    {"OP_MAP_GET_BY_KEY_REL_INDEX_RANGE",
     .value.integer = OP_MAP_GET_BY_KEY_REL_INDEX_RANGE},
    {"OP_MAP_GET_BY_VALUE_RANK_RANGE_REL_TO_END",
     .value.integer = OP_MAP_GET_BY_VALUE_RANK_RANGE_REL_TO_END},
    {"OP_MAP_GET_BY_INDEX_RANGE_TO_END",
     .value.integer = OP_MAP_GET_BY_INDEX_RANGE_TO_END},
    {"OP_MAP_GET_BY_RANK_RANGE_TO_END",
     .value.integer = OP_MAP_GET_BY_RANK_RANGE_TO_END},

    {"MAP_UNORDERED", .value.integer = AS_MAP_UNORDERED},
    {"MAP_KEY_ORDERED", .value.integer = AS_MAP_KEY_ORDERED},
    {"MAP_KEY_VALUE_ORDERED", .value.integer = AS_MAP_KEY_VALUE_ORDERED},

    {"MAP_RETURN_NONE", .value.integer = AS_MAP_RETURN_NONE},
    {"MAP_RETURN_INDEX", .value.integer = AS_MAP_RETURN_INDEX},
    {"MAP_RETURN_REVERSE_INDEX", .value.integer = AS_MAP_RETURN_REVERSE_INDEX},
    {"MAP_RETURN_RANK", .value.integer = AS_MAP_RETURN_RANK},
    {"MAP_RETURN_REVERSE_RANK", .value.integer = AS_MAP_RETURN_REVERSE_RANK},
    {"MAP_RETURN_COUNT", .value.integer = AS_MAP_RETURN_COUNT},
    {"MAP_RETURN_KEY", .value.integer = AS_MAP_RETURN_KEY},
    {"MAP_RETURN_VALUE", .value.integer = AS_MAP_RETURN_VALUE},
    {"MAP_RETURN_KEY_VALUE", .value.integer = AS_MAP_RETURN_KEY_VALUE},
    {"MAP_RETURN_EXISTS", .value.integer = AS_MAP_RETURN_EXISTS},
    {"MAP_RETURN_ORDERED_MAP", .value.integer = AS_MAP_RETURN_ORDERED_MAP},
    {"MAP_RETURN_UNORDERED_MAP", .value.integer = AS_MAP_RETURN_UNORDERED_MAP},

    {"TTL_NAMESPACE_DEFAULT", .value.integer = AS_RECORD_DEFAULT_TTL},
    {"TTL_NEVER_EXPIRE", .value.integer = AS_RECORD_NO_EXPIRE_TTL},
    {"TTL_DONT_UPDATE", .value.integer = AS_RECORD_NO_CHANGE_TTL},
    {"TTL_CLIENT_DEFAULT", .value.integer = AS_RECORD_CLIENT_DEFAULT_TTL},

    {"LIST_RETURN_NONE", .value.integer = AS_LIST_RETURN_NONE},
    {"LIST_RETURN_INDEX", .value.integer = AS_LIST_RETURN_INDEX},
    {"LIST_RETURN_REVERSE_INDEX",
     .value.integer = AS_LIST_RETURN_REVERSE_INDEX},
    {"LIST_RETURN_RANK", .value.integer = AS_LIST_RETURN_RANK},
    {"LIST_RETURN_REVERSE_RANK", .value.integer = AS_LIST_RETURN_REVERSE_RANK},
    {"LIST_RETURN_COUNT", .value.integer = AS_LIST_RETURN_COUNT},
    {"LIST_RETURN_VALUE", .value.integer = AS_LIST_RETURN_VALUE},
    {"LIST_RETURN_EXISTS", .value.integer = AS_LIST_RETURN_EXISTS},

    {"LIST_SORT_DROP_DUPLICATES",
     .value.integer = AS_LIST_SORT_DROP_DUPLICATES},
    {"LIST_SORT_DEFAULT", .value.integer = AS_LIST_SORT_DEFAULT},

    {"LIST_WRITE_DEFAULT", .value.integer = AS_LIST_WRITE_DEFAULT},
    {"LIST_WRITE_ADD_UNIQUE", .value.integer = AS_LIST_WRITE_ADD_UNIQUE},
    {"LIST_WRITE_INSERT_BOUNDED",
     .value.integer = AS_LIST_WRITE_INSERT_BOUNDED},

    {"LIST_ORDERED", .value.integer = AS_LIST_ORDERED},
    {"LIST_UNORDERED", .value.integer = AS_LIST_UNORDERED},

    {"MAP_WRITE_NO_FAIL", .value.integer = AS_MAP_WRITE_NO_FAIL},
    {"MAP_WRITE_PARTIAL", .value.integer = AS_MAP_WRITE_PARTIAL},

    {"LIST_WRITE_NO_FAIL", .value.integer = AS_LIST_WRITE_NO_FAIL},
    {"LIST_WRITE_PARTIAL", .value.integer = AS_LIST_WRITE_PARTIAL},

    /* Map write flags post 3.5.0 */
    {"MAP_WRITE_FLAGS_DEFAULT", .value.integer = AS_MAP_WRITE_DEFAULT},
    {"MAP_WRITE_FLAGS_CREATE_ONLY", .value.integer = AS_MAP_WRITE_CREATE_ONLY},
    {"MAP_WRITE_FLAGS_UPDATE_ONLY", .value.integer = AS_MAP_WRITE_UPDATE_ONLY},
    {"MAP_WRITE_FLAGS_NO_FAIL", .value.integer = AS_MAP_WRITE_NO_FAIL},
    {"MAP_WRITE_FLAGS_PARTIAL", .value.integer = AS_MAP_WRITE_PARTIAL},

    /* READ Mode constants 4.0.0 */

    // AP Read Mode
    {"POLICY_READ_MODE_AP_ONE", .value.integer = AS_POLICY_READ_MODE_AP_ONE},
    {"POLICY_READ_MODE_AP_ALL", .value.integer = AS_POLICY_READ_MODE_AP_ALL},

    // SC Read Mode
    {"POLICY_READ_MODE_SC_SESSION",
     .value.integer = AS_POLICY_READ_MODE_SC_SESSION},
    {"POLICY_READ_MODE_SC_LINEARIZE",
     .value.integer = AS_POLICY_READ_MODE_SC_LINEARIZE},
    {"POLICY_READ_MODE_SC_ALLOW_REPLICA",
     .value.integer = AS_POLICY_READ_MODE_SC_ALLOW_REPLICA},
    {"POLICY_READ_MODE_SC_ALLOW_UNAVAILABLE",
     .value.integer = AS_POLICY_READ_MODE_SC_ALLOW_UNAVAILABLE},

    /* Bitwise constants: 3.9.0 */
    {"BIT_WRITE_DEFAULT", .value.integer = AS_BIT_WRITE_DEFAULT},
    {"BIT_WRITE_CREATE_ONLY", .value.integer = AS_BIT_WRITE_CREATE_ONLY},
    {"BIT_WRITE_UPDATE_ONLY", .value.integer = AS_BIT_WRITE_UPDATE_ONLY},
    {"BIT_WRITE_NO_FAIL", .value.integer = AS_BIT_WRITE_NO_FAIL},
    {"BIT_WRITE_PARTIAL", .value.integer = AS_BIT_WRITE_PARTIAL},

    {"BIT_RESIZE_DEFAULT", .value.integer = AS_BIT_RESIZE_DEFAULT},
    {"BIT_RESIZE_FROM_FRONT", .value.integer = AS_BIT_RESIZE_FROM_FRONT},
    {"BIT_RESIZE_GROW_ONLY", .value.integer = AS_BIT_RESIZE_GROW_ONLY},
    {"BIT_RESIZE_SHRINK_ONLY", .value.integer = AS_BIT_RESIZE_SHRINK_ONLY},

    {"BIT_OVERFLOW_FAIL", .value.integer = AS_BIT_OVERFLOW_FAIL},
    {"BIT_OVERFLOW_SATURATE", .value.integer = AS_BIT_OVERFLOW_SATURATE},
    {"BIT_OVERFLOW_WRAP", .value.integer = AS_BIT_OVERFLOW_WRAP},

    /* BITWISE OPS: 3.9.0 */
    {"OP_BIT_INSERT", .value.integer = OP_BIT_INSERT},
    {"OP_BIT_RESIZE", .value.integer = OP_BIT_RESIZE},
    {"OP_BIT_REMOVE", .value.integer = OP_BIT_REMOVE},
    {"OP_BIT_SET", .value.integer = OP_BIT_SET},
    {"OP_BIT_OR", .value.integer = OP_BIT_OR},
    {"OP_BIT_XOR", .value.integer = OP_BIT_XOR},
    {"OP_BIT_AND", .value.integer = OP_BIT_AND},
    {"OP_BIT_NOT", .value.integer = OP_BIT_NOT},
    {"OP_BIT_LSHIFT", .value.integer = OP_BIT_LSHIFT},
    {"OP_BIT_RSHIFT", .value.integer = OP_BIT_RSHIFT},
    {"OP_BIT_ADD", .value.integer = OP_BIT_ADD},
    {"OP_BIT_SUBTRACT", .value.integer = OP_BIT_SUBTRACT},
    {"OP_BIT_GET_INT", .value.integer = OP_BIT_GET_INT},
    {"OP_BIT_SET_INT", .value.integer = OP_BIT_SET_INT},
    {"OP_BIT_GET", .value.integer = OP_BIT_GET},
    {"OP_BIT_COUNT", .value.integer = OP_BIT_COUNT},
    {"OP_BIT_LSCAN", .value.integer = OP_BIT_LSCAN},
    {"OP_BIT_RSCAN", .value.integer = OP_BIT_RSCAN},

    /* Nested CDT constants: 3.9.0 */
    {"CDT_CTX_LIST_INDEX", .value.integer = AS_CDT_CTX_LIST_INDEX},
    {"CDT_CTX_LIST_RANK", .value.integer = AS_CDT_CTX_LIST_RANK},
    {"CDT_CTX_LIST_VALUE", .value.integer = AS_CDT_CTX_LIST_VALUE},
    {"CDT_CTX_LIST_INDEX_CREATE", .value.integer = CDT_CTX_LIST_INDEX_CREATE},
    {"CDT_CTX_MAP_INDEX", .value.integer = AS_CDT_CTX_MAP_INDEX},
    {"CDT_CTX_MAP_RANK", .value.integer = AS_CDT_CTX_MAP_RANK},
    {"CDT_CTX_MAP_KEY", .value.integer = AS_CDT_CTX_MAP_KEY},
    {"CDT_CTX_MAP_VALUE", .value.integer = AS_CDT_CTX_MAP_VALUE},
    {"CDT_CTX_MAP_KEY_CREATE", .value.integer = CDT_CTX_MAP_KEY_CREATE},
    EXPOSE_AS_MACRO_AS_PRIVATE_FIELD(AS_CDT_CTX_EXP),

    /* HLL constants 3.11.0 */
    {"OP_HLL_ADD", .value.integer = OP_HLL_ADD},
    {"OP_HLL_DESCRIBE", .value.integer = OP_HLL_DESCRIBE},
    {"OP_HLL_FOLD", .value.integer = OP_HLL_FOLD},
    {"OP_HLL_GET_COUNT", .value.integer = OP_HLL_GET_COUNT},
    {"OP_HLL_GET_INTERSECT_COUNT", .value.integer = OP_HLL_GET_INTERSECT_COUNT},
    {"OP_HLL_GET_SIMILARITY", .value.integer = OP_HLL_GET_SIMILARITY},
    {"OP_HLL_GET_UNION", .value.integer = OP_HLL_GET_UNION},
    {"OP_HLL_GET_UNION_COUNT", .value.integer = OP_HLL_GET_UNION_COUNT},
    {"OP_HLL_GET_SIMILARITY", .value.integer = OP_HLL_GET_SIMILARITY},
    {"OP_HLL_INIT", .value.integer = OP_HLL_INIT},
    {"OP_HLL_REFRESH_COUNT", .value.integer = OP_HLL_REFRESH_COUNT},
    {"OP_HLL_SET_UNION", .value.integer = OP_HLL_SET_UNION},
    {"OP_HLL_MAY_CONTAIN",
     .value.integer = OP_HLL_MAY_CONTAIN}, // for expression filters

    {"HLL_WRITE_DEFAULT", .value.integer = AS_HLL_WRITE_DEFAULT},
    {"HLL_WRITE_CREATE_ONLY", .value.integer = AS_HLL_WRITE_CREATE_ONLY},
    {"HLL_WRITE_UPDATE_ONLY", .value.integer = AS_HLL_WRITE_UPDATE_ONLY},
    {"HLL_WRITE_NO_FAIL", .value.integer = AS_HLL_WRITE_NO_FAIL},
    {"HLL_WRITE_ALLOW_FOLD", .value.integer = AS_HLL_WRITE_ALLOW_FOLD},

    /* Expression operation constants 5.1.0 */
    {"OP_EXPR_READ", .value.integer = OP_EXPR_READ},
    {"OP_EXPR_WRITE", .value.integer = OP_EXPR_WRITE},
    {"EXP_WRITE_DEFAULT", .value.integer = AS_EXP_WRITE_DEFAULT},
    {"EXP_WRITE_CREATE_ONLY", .value.integer = AS_EXP_WRITE_CREATE_ONLY},
    {"EXP_WRITE_UPDATE_ONLY", .value.integer = AS_EXP_WRITE_UPDATE_ONLY},
    {"EXP_WRITE_ALLOW_DELETE", .value.integer = AS_EXP_WRITE_ALLOW_DELETE},
    {"EXP_WRITE_POLICY_NO_FAIL", .value.integer = AS_EXP_WRITE_POLICY_NO_FAIL},
    {"EXP_WRITE_EVAL_NO_FAIL", .value.integer = AS_EXP_WRITE_EVAL_NO_FAIL},
    {"EXP_READ_DEFAULT", .value.integer = AS_EXP_READ_DEFAULT},
    {"EXP_READ_EVAL_NO_FAIL", .value.integer = AS_EXP_READ_EVAL_NO_FAIL},

    /* For BinType expression, as_bytes_type */
    {"AS_BYTES_UNDEF", .value.integer = AS_BYTES_UNDEF},
    {"AS_BYTES_INTEGER", .value.integer = AS_BYTES_INTEGER},
    {"AS_BYTES_DOUBLE", .value.integer = AS_BYTES_DOUBLE},
    {"AS_BYTES_STRING", .value.integer = AS_BYTES_STRING},
    {"AS_BYTES_BLOB", .value.integer = AS_BYTES_BLOB},
    {"AS_BYTES_JAVA", .value.integer = AS_BYTES_JAVA},
    {"AS_BYTES_CSHARP", .value.integer = AS_BYTES_CSHARP},
    {"AS_BYTES_PYTHON", .value.integer = AS_BYTES_PYTHON},
    {"AS_BYTES_RUBY", .value.integer = AS_BYTES_RUBY},
    {"AS_BYTES_PHP", .value.integer = AS_BYTES_PHP},
    {"AS_BYTES_ERLANG", .value.integer = AS_BYTES_ERLANG},
    {"AS_BYTES_BOOL", .value.integer = AS_BYTES_BOOL},
    {"AS_BYTES_HLL", .value.integer = AS_BYTES_HLL},
    {"AS_BYTES_MAP", .value.integer = AS_BYTES_MAP},
    {"AS_BYTES_LIST", .value.integer = AS_BYTES_LIST},
    {"AS_BYTES_GEOJSON", .value.integer = AS_BYTES_GEOJSON},
    {"AS_BYTES_TYPE_MAX", .value.integer = AS_BYTES_TYPE_MAX},

    /* Regex constants from predexp, still used by expressions */
    {"REGEX_NONE", .value.integer = REGEX_NONE},
    {"REGEX_EXTENDED", .value.integer = REGEX_EXTENDED},
    {"REGEX_ICASE", .value.integer = REGEX_ICASE},
    {"REGEX_NOSUB", .value.integer = REGEX_NOSUB},
    {"REGEX_NEWLINE", .value.integer = REGEX_NEWLINE},

    {"QUERY_DURATION_LONG", .value.integer = AS_QUERY_DURATION_LONG},
    {"QUERY_DURATION_LONG_RELAX_AP",
     .value.integer = AS_QUERY_DURATION_LONG_RELAX_AP},
    {"QUERY_DURATION_SHORT", .value.integer = AS_QUERY_DURATION_SHORT},

    {"LOG_LEVEL_OFF", .value.integer = -1},
    {"LOG_LEVEL_ERROR", .value.integer = AS_LOG_LEVEL_ERROR},
    {"LOG_LEVEL_WARN", .value.integer = AS_LOG_LEVEL_WARN},
    {"LOG_LEVEL_INFO", .value.integer = AS_LOG_LEVEL_INFO},
    {"LOG_LEVEL_DEBUG", .value.integer = AS_LOG_LEVEL_DEBUG},
    {"LOG_LEVEL_TRACE", .value.integer = AS_LOG_LEVEL_TRACE},

    {"COMMIT_OK", .value.integer = AS_COMMIT_OK},
    {"COMMIT_ALREADY_COMMITTED", .value.integer = AS_COMMIT_ALREADY_COMMITTED},
    {"COMMIT_ROLL_FORWARD_ABANDONED",
     .value.integer = AS_COMMIT_ROLL_FORWARD_ABANDONED},
    {"COMMIT_CLOSE_ABANDONED", .value.integer = AS_COMMIT_CLOSE_ABANDONED},

    {"ABORT_OK", .value.integer = AS_ABORT_OK},
    {"ABORT_ALREADY_ABORTED", .value.integer = AS_ABORT_ALREADY_ABORTED},
    {"ABORT_ROLL_BACK_ABANDONED",
     .value.integer = AS_ABORT_ROLL_BACK_ABANDONED},
    {"ABORT_CLOSE_ABANDONED", .value.integer = AS_ABORT_CLOSE_ABANDONED},

    {"TXN_STATE_OPEN", .value.integer = AS_TXN_STATE_OPEN},
    {"TXN_STATE_VERIFIED", .value.integer = AS_TXN_STATE_VERIFIED},
    {"TXN_STATE_COMMITTED", .value.integer = AS_TXN_STATE_COMMITTED},
    {"TXN_STATE_ABORTED", .value.integer = AS_TXN_STATE_ABORTED},

    {"JOB_SCAN", .is_str_value = true, .value.string = "scan"},
    {"JOB_QUERY", .is_str_value = true, .value.string = "query"},

    /*
        When doing a path expression select/apply operation, and applying an expression on each
        iterated object, this lets us choose a specific value over each iterated
        object.
    */
    EXPOSE_AS_MACRO_WITHOUT_AS_PREFIX_AS_PUBLIC_FIELD(EXP_LOOPVAR_KEY),
    EXPOSE_AS_MACRO_WITHOUT_AS_PREFIX_AS_PUBLIC_FIELD(EXP_LOOPVAR_VALUE),
    EXPOSE_AS_MACRO_WITHOUT_AS_PREFIX_AS_PUBLIC_FIELD(EXP_LOOPVAR_INDEX),

    EXPOSE_AS_MACRO_WITHOUT_AS_PREFIX_AS_PUBLIC_FIELD(
        EXP_PATH_SELECT_MATCHING_TREE),
    EXPOSE_AS_MACRO_WITHOUT_AS_PREFIX_AS_PUBLIC_FIELD(EXP_PATH_SELECT_VALUE),
    EXPOSE_AS_MACRO_WITHOUT_AS_PREFIX_AS_PUBLIC_FIELD(
        EXP_PATH_SELECT_MAP_VALUE),
    EXPOSE_AS_MACRO_WITHOUT_AS_PREFIX_AS_PUBLIC_FIELD(
        EXP_PATH_SELECT_LIST_VALUE),
    EXPOSE_AS_MACRO_WITHOUT_AS_PREFIX_AS_PUBLIC_FIELD(EXP_PATH_SELECT_MAP_KEY),
    EXPOSE_AS_MACRO_WITHOUT_AS_PREFIX_AS_PUBLIC_FIELD(
        EXP_PATH_SELECT_MAP_KEY_VALUE),
    EXPOSE_AS_MACRO_WITHOUT_AS_PREFIX_AS_PUBLIC_FIELD(EXP_PATH_SELECT_NO_FAIL),

    EXPOSE_AS_MACRO_WITHOUT_AS_PREFIX_AS_PUBLIC_FIELD(EXP_PATH_MODIFY_NO_FAIL),
    EXPOSE_AS_MACRO_WITHOUT_AS_PREFIX_AS_PUBLIC_FIELD(EXP_PATH_MODIFY_DEFAULT),

    // For aerospike_helpers to use. Not to be exposed in public API
    // TODO: move all internal constants used by aerospike_helpers to this loc

    EXPOSE_MACRO(_AS_EXP_LOOPVAR_FLOAT),
    EXPOSE_MACRO(_AS_EXP_LOOPVAR_INT),
    EXPOSE_MACRO(_AS_EXP_LOOPVAR_LIST),
    EXPOSE_MACRO(_AS_EXP_LOOPVAR_MAP),
    EXPOSE_MACRO(_AS_EXP_LOOPVAR_STR),
    EXPOSE_MACRO(_AS_EXP_LOOPVAR_BLOB),
    EXPOSE_MACRO(_AS_EXP_LOOPVAR_BOOL),
    // EXPOSE_MACRO(_AS_EXP_LOOPVAR_INF),
    EXPOSE_MACRO(_AS_EXP_LOOPVAR_NIL),
    EXPOSE_MACRO(_AS_EXP_LOOPVAR_GEOJSON),

    // C client uses the same expression code for these two expressions
    // so we define unique ones in the Python client code
    EXPOSE_MACRO(_AS_EXP_CODE_CALL_SELECT),
    EXPOSE_MACRO(_AS_EXP_CODE_CALL_APPLY),
    EXPOSE_MACRO(_AS_EXP_CODE_RESULT_REMOVE),

    EXPOSE_STRING_MACRO_FOR_AEROSPIKE_HELPERS(_CDT_FLAGS_KEY),
    EXPOSE_STRING_MACRO_FOR_AEROSPIKE_HELPERS(_CDT_APPLY_MOD_EXP_KEY),

    EXPOSE_STRING_MACRO_FOR_AEROSPIKE_HELPERS(_CDT_CTX_FILTER_EXPR_KEY),
};

struct submodule_name_to_creation_method {
    const char *name;
    const char *fully_qualified_name;
    PyObject *(*pyobject_creation_method)(void);
};

#define SHORT_AND_FULLY_QUALIFIED_NAME(s) #s, "aerospike." #s

static struct submodule_name_to_creation_method py_submodules[] = {
    // We don't use module's __name__ attribute
    // because the modules' __name__ is the fully qualified name which includes the package name
    {SHORT_AND_FULLY_QUALIFIED_NAME(exception), AerospikeException_New},
    {SHORT_AND_FULLY_QUALIFIED_NAME(predicates), AerospikePredicates_New},
};

struct type_name_to_creation_method {
    const char *name;
    PyTypeObject *(*pytype_ready_method)(void);
};

static struct type_name_to_creation_method py_module_types[] = {
    // We also don't retrieve the type's __name__ because:
    // 1. Some of the objects have names different from the class name when accessed from the package
    // 2. We don't want to deal with extracting an object's __name__ from a Unicode object.
    // We have to make sure the Unicode object lives as long as we need its internal buffer
    // It's easier to just use a C string directly
    {"Client", AerospikeClient_Ready},
    {"Query", AerospikeQuery_Ready},
    {"Scan", AerospikeScan_Ready},
    {"KeyOrderedDict", AerospikeKeyOrderedDict_Ready},
    {"GeoJSON", AerospikeGeospatial_Ready},
    {"null", AerospikeNullObject_Ready},
    {"CDTWildcard", AerospikeWildcardObject_Ready},
    {"CDTInfinite", AerospikeInfiniteObject_Ready},
    {"Transaction", AerospikeTransaction_Ready},
    {"ConfigProvider", AerospikeConfigProvider_Ready},
};

// We use a macro to avoid repetition
#define DEFINE_SET_OF_VALID_KEYS(array_name_prefix, ...)                       \
    const char *array_name_prefix##_valid_keys[] = {__VA_ARGS__};              \
    PyObject *py_##array_name_prefix##_valid_keys = NULL;

DEFINE_SET_OF_VALID_KEYS(
    client_config, "lua", "config_provider", "tls", "hosts", "shm",
    "serialization", "policies", "thread_pool_size", "max_threads",
    "min_conns_per_node", "max_conns_per_node", "max_error_rate",
    "error_rate_window", "connect_timeout", "use_shared_connection",
    "send_bool_as", "compression_threshold", "tend_interval", "cluster_name",
    "strict_types", "rack_aware", "rack_id", "rack_ids",
    "use_services_alternate", "max_socket_idle", "fail_if_not_connected",
    "user", "password", "validate_keys", "app_id", "force_single_node", NULL)

DEFINE_SET_OF_VALID_KEYS(client_config_shm, "shm_max_nodes", "max_nodes",
                         "shm_max_namespaces", "max_namespaces",
                         "shm_takeover_threshold_sec", "takeover_threshold_sec",
                         "shm_key", NULL)

DEFINE_SET_OF_VALID_KEYS(client_config_lua, "system_path", "user_path", NULL

)

DEFINE_SET_OF_VALID_KEYS(client_config_policies, "read", "write", "apply",
                         "operate", "remove", "query", "scan", "batch",
                         "batch_remove", "batch_apply", "batch_write",
                         "batch_parent_write", "info", "admin", "txn_verify",
                         "txn_roll", "total_timeout", "auth_mode",
                         "login_timeout_ms", "key", "exists", "max_retries",
                         "replica", "commit_level", "metrics", NULL)

DEFINE_SET_OF_VALID_KEYS(client_config_tls, "enable", "cafile", "capath",
                         "protocols", "cipher_suite", "keyfile", "keyfile_pw",
                         "cert_blacklist", "certfile", "crl_check",
                         "crl_check_all", "log_session_info", "for_login_only",
                         NULL

)

#define BASE_POLICY_KEYS                                                       \
    "total_timeout", "socket_timeout", "max_retries", "sleep_between_retries", \
        "compress", "txn", "expressions", "connect_timeout", "timeout_delay"

DEFINE_SET_OF_VALID_KEYS(apply_policy, BASE_POLICY_KEYS, "key", "replica",
                         "commit_level", "durable_delete", "ttl",
                         "on_locking_only", NULL

)

// send_as_is and check_bounds should not be used by the user
// That's why they are not documented.
// But they were already exposed in the API for a long time, so we allow them to be used
#define INFO_POLICY_KEYS "timeout", "send_as_is", "check_bounds"
DEFINE_SET_OF_VALID_KEYS(info_policy, INFO_POLICY_KEYS, NULL

)

DEFINE_SET_OF_VALID_KEYS(query_policy, BASE_POLICY_KEYS, "deserialize",
                         "replica", "short_query", "expected_duration",
                         "partition_filter", NULL

)

DEFINE_SET_OF_VALID_KEYS(read_policy, BASE_POLICY_KEYS, "key", "replica",
                         "deserialize", "read_touch_ttl_percent",
                         "read_mode_ap", "read_mode_sc", NULL

)

DEFINE_SET_OF_VALID_KEYS(remove_policy, BASE_POLICY_KEYS, "generation", "key",
                         "gen", "commit_level", "replica", "durable_delete",
                         NULL

)

#define SCAN_POLICY_KEYS                                                       \
    "durable_delete", "records_per_second", "max_records", "replica", "ttl",   \
        "partition_filter"

DEFINE_SET_OF_VALID_KEYS(scan_policy, BASE_POLICY_KEYS, SCAN_POLICY_KEYS, NULL)

DEFINE_SET_OF_VALID_KEYS(info_and_scan_policy, BASE_POLICY_KEYS,
                         SCAN_POLICY_KEYS, INFO_POLICY_KEYS, NULL)

#define WRITE_POLICY_KEYS                                                      \
    "key", "gen", "exists", "commit_level", "durable_delete", "replica",       \
        "compression_threshold", "on_locking_only", "ttl"

DEFINE_SET_OF_VALID_KEYS(write_policy, BASE_POLICY_KEYS, WRITE_POLICY_KEYS, NULL

)

DEFINE_SET_OF_VALID_KEYS(info_and_write_policy, BASE_POLICY_KEYS,
                         WRITE_POLICY_KEYS, INFO_POLICY_KEYS, NULL)

DEFINE_SET_OF_VALID_KEYS(operate_policy, BASE_POLICY_KEYS, "key", "gen",
                         "commit_level", "replica", "durable_delete",
                         "deserialize", "exists", "read_touch_ttl_percent",
                         "on_locking_only", "read_mode_ap", "read_mode_sc",
                         "ttl", NULL

)

DEFINE_SET_OF_VALID_KEYS(batch_policy, BASE_POLICY_KEYS, "concurrent",
                         "allow_inline", "deserialize", "replica",
                         "read_touch_ttl_percent", "read_mode_ap",
                         "read_mode_sc", "allow_inline_ssd", "respond_all_keys",
                         NULL

)

DEFINE_SET_OF_VALID_KEYS(batch_write_policy, "key", "gen", "commit_level",
                         "durable_delete", "exists", "on_locking_only",
                         "expressions", "ttl", NULL

)

DEFINE_SET_OF_VALID_KEYS(batch_read_policy, "read_touch_ttl_percent",
                         "read_mode_ap", "read_mode_sc", "expressions", NULL

)

DEFINE_SET_OF_VALID_KEYS(batch_apply_policy, "key", "commit_level", "ttl",
                         "durable_delete", "on_locking_only", "expressions",
                         NULL

)

DEFINE_SET_OF_VALID_KEYS(batch_remove_policy, "key", "commit_level", "gen",
                         "durable_delete", "generation", "expressions", NULL

)

DEFINE_SET_OF_VALID_KEYS(bit_policy, "bit_write_flags", NULL

)

DEFINE_SET_OF_VALID_KEYS(map_policy, "map_order", "map_write_flags",
                         "persist_index", NULL

)

DEFINE_SET_OF_VALID_KEYS(list_policy, "list_order", "write_flags", NULL)

DEFINE_SET_OF_VALID_KEYS(hll_policy, "flags", NULL)

DEFINE_SET_OF_VALID_KEYS(admin_policy, "timeout", NULL)

DEFINE_SET_OF_VALID_KEYS(record_metadata, "gen", "ttl", NULL)

// Use a struct to create pairs of pyobjects and list of strings defined above
// When we initialize the module, we create sets for the valid keys that the client can use later

struct py_set_name_to_str_list {
    // We are setting the global PyObject *variables above, which can be accessed externally
    PyObject **py_set_of_keys;
    const char **valid_keys;
};

#define PY_SET_NAME_TO_STR_LIST(array_name)                                    \
    {                                                                          \
        &py_##array_name, array_name                                           \
    }

static struct py_set_name_to_str_list py_set_name_to_str_lists[] = {
    PY_SET_NAME_TO_STR_LIST(client_config_valid_keys),
    PY_SET_NAME_TO_STR_LIST(client_config_shm_valid_keys),
    PY_SET_NAME_TO_STR_LIST(client_config_lua_valid_keys),
    PY_SET_NAME_TO_STR_LIST(client_config_policies_valid_keys),
    PY_SET_NAME_TO_STR_LIST(client_config_tls_valid_keys),
    PY_SET_NAME_TO_STR_LIST(apply_policy_valid_keys),
    PY_SET_NAME_TO_STR_LIST(info_policy_valid_keys),
    PY_SET_NAME_TO_STR_LIST(admin_policy_valid_keys),
    PY_SET_NAME_TO_STR_LIST(query_policy_valid_keys),
    PY_SET_NAME_TO_STR_LIST(read_policy_valid_keys),
    PY_SET_NAME_TO_STR_LIST(remove_policy_valid_keys),
    PY_SET_NAME_TO_STR_LIST(scan_policy_valid_keys),
    PY_SET_NAME_TO_STR_LIST(write_policy_valid_keys),
    PY_SET_NAME_TO_STR_LIST(operate_policy_valid_keys),
    PY_SET_NAME_TO_STR_LIST(batch_policy_valid_keys),
    PY_SET_NAME_TO_STR_LIST(batch_write_policy_valid_keys),
    PY_SET_NAME_TO_STR_LIST(batch_read_policy_valid_keys),
    PY_SET_NAME_TO_STR_LIST(batch_apply_policy_valid_keys),
    PY_SET_NAME_TO_STR_LIST(batch_remove_policy_valid_keys),
    PY_SET_NAME_TO_STR_LIST(bit_policy_valid_keys),
    PY_SET_NAME_TO_STR_LIST(map_policy_valid_keys),
    PY_SET_NAME_TO_STR_LIST(list_policy_valid_keys),
    PY_SET_NAME_TO_STR_LIST(hll_policy_valid_keys),
    PY_SET_NAME_TO_STR_LIST(info_and_write_policy_valid_keys),
    PY_SET_NAME_TO_STR_LIST(info_and_scan_policy_valid_keys),
    PY_SET_NAME_TO_STR_LIST(record_metadata_valid_keys),
};

// Return NULL if an exception is raised
// Returns strong reference to new Python dictionary
static PyObject *py_set_new_from_str_list(const char *const *str_list)
{
    PyObject *py_valid_keys = PySet_New(NULL);
    if (py_valid_keys == NULL) {
        goto error;
    }

    const char *const *curr_str_ref = str_list;
    while (*curr_str_ref) {
        PyObject *py_str = PyUnicode_FromString(*curr_str_ref);
        if (py_str == NULL) {
            goto CLEANUP_SET_ON_ERROR;
        }

        int result = PySet_Add(py_valid_keys, py_str);
        Py_DECREF(py_str);
        if (result == -1) {
            goto CLEANUP_SET_ON_ERROR;
        }
        curr_str_ref++;
    }

    return py_valid_keys;

CLEANUP_SET_ON_ERROR:
    Py_DECREF(py_valid_keys);
error:
    return NULL;
}

AS_EXTERN extern char *aerospike_client_language;

bool is_python_client_version_set_for_user_agent = false;

void aerospike_free(void *self)
{
    // The aerospike module may be created, but initializing the module may fail.
    // In that case, our module will be cleaned up by the garbage collector and this m_free callback will be called.
    // We don't want to deallocate aerospike_client_version pointing to a string in data section of memory
    // That is the default value for the C client
    if (is_python_client_version_set_for_user_agent) {
        cf_free(aerospike_client_version);
        is_python_client_version_set_for_user_agent = false;
    }

    for (unsigned long i = 0; i < sizeof(py_set_name_to_str_lists) /
                                      sizeof(py_set_name_to_str_lists[0]);
         i++) {
        Py_XDECREF(*(py_set_name_to_str_lists[i].py_set_of_keys));
    }

    Py_XDECREF(py_global_hosts);
}

PyMODINIT_FUNC PyInit_aerospike(void)
{
    aerospike_client_language = "python";
    static struct PyModuleDef moduledef = {PyModuleDef_HEAD_INIT,
                                           .m_name = AEROSPIKE_MODULE_NAME,
                                           .m_doc = "Aerospike Python Client",
                                           .m_methods = aerospike_methods,
                                           .m_size = -1,
                                           .m_free = aerospike_free};

    PyObject *py_aerospike_module = PyModule_Create(&moduledef);
    if (py_aerospike_module == NULL) {
        return NULL;
    }

    Aerospike_Enable_Default_Logging();

    int retval;
    for (unsigned long i = 0; i < sizeof(py_set_name_to_str_lists) /
                                      sizeof(py_set_name_to_str_lists[0]);
         i++) {
        // just use a Python set so we don't need to implement a hashset in C
        // The C client does not have a public API for a hashset yet
        // Time complexity of set should be constant on avg:
        // https://wiki.python.org/moin/TimeComplexity
        PyObject *py_valid_keys =
            py_set_new_from_str_list(py_set_name_to_str_lists[i].valid_keys);
        if (py_valid_keys == NULL) {
            goto AEROSPIKE_MODULE_CLEANUP_ON_ERROR;
        }

        *(py_set_name_to_str_lists[i].py_set_of_keys) = py_valid_keys;
    }

    py_global_hosts = PyDict_New();
    if (py_global_hosts == NULL) {
        goto AEROSPIKE_MODULE_CLEANUP_ON_ERROR;
    }

    unsigned long i = 0;
    for (i = 0; i < sizeof(py_module_types) / sizeof(py_module_types[0]); i++) {
        PyTypeObject *(*py_type_ready_func)(void) =
            py_module_types[i].pytype_ready_method;
        PyTypeObject *py_type = py_type_ready_func();
        if (py_type == NULL) {
            goto AEROSPIKE_MODULE_CLEANUP_ON_ERROR;
        }

        Py_INCREF(py_type);
        retval = PyModule_AddObject(
            py_aerospike_module, py_module_types[i].name, (PyObject *)py_type);
        if (retval == -1) {
            Py_DECREF(py_type);
            goto AEROSPIKE_MODULE_CLEANUP_ON_ERROR;
        }
    }

    /*
	 * Add constants to module.
	 */
    for (i = 0; i < sizeof(module_constants) / sizeof(module_constants[0]);
         i++) {
        if (module_constants[i].is_str_value == false) {
            retval = PyModule_AddIntConstant(py_aerospike_module,
                                             module_constants[i].name,
                                             module_constants[i].value.integer);
        }
        else {
            retval = PyModule_AddStringConstant(
                py_aerospike_module, module_constants[i].name,
                module_constants[i].value.string);
        }

        if (retval == -1) {
            goto AEROSPIKE_MODULE_CLEANUP_ON_ERROR;
        }
    }

    // Allows submodules to be imported using "import aerospike.<submodule-name>"
    // https://github.com/python/cpython/issues/87533#issuecomment-2373119452
    PyObject *py_sys = PyImport_ImportModule("sys");
    if (py_sys == NULL) {
        goto AEROSPIKE_MODULE_CLEANUP_ON_ERROR;
    }

    PyObject *py_sys_dot_modules_dict =
        PyObject_GetAttrString(py_sys, "modules");
    Py_DECREF(py_sys);
    if (py_sys_dot_modules_dict == NULL) {
        goto AEROSPIKE_MODULE_CLEANUP_ON_ERROR;
    }

    for (i = 0; i < sizeof(py_submodules) / sizeof(py_submodules[0]); i++) {
        PyObject *(*create_py_submodule)(void) =
            py_submodules[i].pyobject_creation_method;
        PyObject *py_submodule = create_py_submodule();
        if (py_submodule == NULL) {
            goto SYS_DOT_MODULES_DICT_CLEANUP_ON_ERROR;
        }

        int retval = PyDict_SetItemString(py_sys_dot_modules_dict,
                                          py_submodules[i].fully_qualified_name,
                                          py_submodule);
        if (retval == -1) {
            goto SUBMODULE_CLEANUP_ON_ERROR;
        }

        retval = PyModule_AddObject(py_aerospike_module, py_submodules[i].name,
                                    py_submodule);
        if (retval == -1) {
            goto SUBMODULE_CLEANUP_ON_ERROR;
        }
        continue;

    SUBMODULE_CLEANUP_ON_ERROR:
        Py_DECREF(py_submodule);
        goto SYS_DOT_MODULES_DICT_CLEANUP_ON_ERROR;
    }

    Py_DECREF(py_sys_dot_modules_dict);

    PyObject *py_metadata_subpackage =
        PyImport_ImportModule("importlib.metadata");
    if (py_metadata_subpackage == NULL) {
        goto AEROSPIKE_MODULE_CLEANUP_ON_ERROR;
    }

    PyObject *py_version_callback =
        PyObject_GetAttrString(py_metadata_subpackage, "version");
    Py_DECREF(py_metadata_subpackage);
    if (py_version_callback == NULL) {
        goto AEROSPIKE_MODULE_CLEANUP_ON_ERROR;
    }

    PyObject *py_aerospike_module_version_str =
        PyObject_CallFunction(py_version_callback, "s", AEROSPIKE_MODULE_NAME);
    Py_DECREF(py_version_callback);
    if (py_aerospike_module_version_str == NULL) {
        goto AEROSPIKE_MODULE_CLEANUP_ON_ERROR;
    }

    const char *aerospike_module_version =
        PyUnicode_AsUTF8(py_aerospike_module_version_str);
    if (aerospike_module_version == NULL) {
        Py_DECREF(py_aerospike_module_version_str);
        goto AEROSPIKE_MODULE_CLEANUP_ON_ERROR;
    }

    // Here we assume that the original value of aerospike_client_version was not heap allocated
    aerospike_client_version = cf_strdup(aerospike_module_version);
    is_python_client_version_set_for_user_agent = true;
    Py_DECREF(py_aerospike_module_version_str);

    return py_aerospike_module;

SYS_DOT_MODULES_DICT_CLEANUP_ON_ERROR:
    Py_DECREF(py_sys_dot_modules_dict);

AEROSPIKE_MODULE_CLEANUP_ON_ERROR:
    Py_DECREF(py_aerospike_module);

    // TODO: Clean up any submodules that were manually added to sys.modules
    // This isn't a big deal though, so just leave off for now
    return NULL;
}
