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

#include <aerospike/as_error.h>
#include <aerospike/as_exp.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>
#include <aerospike/as_admin.h>
#include <aerospike/as_operations.h>
#include <aerospike/as_exp_operations.h>
#include <aerospike/aerospike_index.h>
#include "aerospike/as_scan.h"
#include "aerospike/as_job.h"
#include <aerospike/as_metrics.h>
#include <aerospike/as_cluster.h>

#include "conversions.h"
#include "policy.h"
#include "macros.h"
#include "policy_config.h"

#define MAP_WRITE_FLAGS_KEY "map_write_flags"
#define BIT_WRITE_FLAGS_KEY "bit_write_flags"

#define POLICY_INIT(__policy)                                                  \
    as_error_reset(err);                                                       \
    if (!py_policy || py_policy == Py_None) {                                  \
        return err->code;                                                      \
    }                                                                          \
    if (!PyDict_Check(py_policy)) {                                            \
        return as_error_update(err, AEROSPIKE_ERR_PARAM,                       \
                               "policy must be a dict");                       \
    }                                                                          \
    __policy##_init(policy);

#define POLICY_UPDATE() *policy_p = policy;

#define POLICY_SET_FIELD(__field, __type)                                      \
    {                                                                          \
        PyObject *py_field = PyDict_GetItemString(py_policy, #__field);        \
        if (py_field) {                                                        \
            if (PyLong_Check(py_field)) {                                      \
                policy->__field = (__type)PyLong_AsLong(py_field);             \
            }                                                                  \
            else {                                                             \
                return as_error_update(err, AEROSPIKE_ERR_PARAM,               \
                                       "%s is invalid", #__field);             \
            }                                                                  \
        }                                                                      \
    }

#define POLICY_SET_BASE_FIELD(__field, __type)                                 \
    {                                                                          \
        PyObject *py_field = PyDict_GetItemString(py_policy, #__field);        \
        if (py_field) {                                                        \
            if (PyLong_Check(py_field)) {                                      \
                policy->base.__field = (__type)PyLong_AsLong(py_field);        \
            }                                                                  \
            else {                                                             \
                return as_error_update(err, AEROSPIKE_ERR_PARAM,               \
                                       "%s is invalid", #__field);             \
            }                                                                  \
        }                                                                      \
    }

#define POLICY_SET_EXPRESSIONS_BASE_FIELD()                                    \
    {                                                                          \
        if (exp_list) {                                                        \
            PyObject *py_exp_list =                                            \
                PyDict_GetItemString(py_policy, "expressions");                \
            if (py_exp_list) {                                                 \
                if (convert_exp_list(self, py_exp_list, &exp_list, err) ==     \
                    AEROSPIKE_OK) {                                            \
                    policy->base.filter_exp = exp_list;                        \
                    *exp_list_p = exp_list;                                    \
                }                                                              \
            }                                                                  \
        }                                                                      \
    }

#define POLICY_SET_EXPRESSIONS_FIELD()                                         \
    {                                                                          \
        PyObject *py_exp_list =                                                \
            PyDict_GetItemString(py_policy, "expressions");                    \
        if (py_exp_list) {                                                     \
            if (convert_exp_list(self, py_exp_list, &exp_list, err) ==         \
                AEROSPIKE_OK) {                                                \
                policy->filter_exp = exp_list;                                 \
                *exp_list_p = exp_list;                                        \
            }                                                                  \
        }                                                                      \
    }

#define MAP_POLICY_SET_FIELD(__field)                                          \
    {                                                                          \
        PyObject *py_field = PyDict_GetItemString(py_policy, #__field);        \
        if (py_field) {                                                        \
            if (PyLong_Check(py_field)) {                                      \
                __field = PyLong_AsLong(py_field);                             \
            }                                                                  \
            else {                                                             \
                return as_error_update(err, AEROSPIKE_ERR_PARAM,               \
                                       "%s is invalid", #__field);             \
            }                                                                  \
        }                                                                      \
    }

/*
 *******************************************************************************************************
 * Mapping of constant number to constant name string.
 *******************************************************************************************************
 */
static AerospikeConstants aerospike_constants[] = {
    {AS_POLICY_RETRY_NONE, "POLICY_RETRY_NONE"},
    {AS_POLICY_RETRY_ONCE, "POLICY_RETRY_ONCE"},
    {AS_POLICY_EXISTS_IGNORE, "POLICY_EXISTS_IGNORE"},
    {AS_POLICY_EXISTS_CREATE, "POLICY_EXISTS_CREATE"},
    {AS_POLICY_EXISTS_UPDATE, "POLICY_EXISTS_UPDATE"},
    {AS_POLICY_EXISTS_REPLACE, "POLICY_EXISTS_REPLACE"},
    {AS_POLICY_EXISTS_CREATE_OR_REPLACE, "POLICY_EXISTS_CREATE_OR_REPLACE"},
    {AS_UDF_TYPE_LUA, "UDF_TYPE_LUA"},
    {AS_POLICY_KEY_DIGEST, "POLICY_KEY_DIGEST"},
    {AS_POLICY_KEY_SEND, "POLICY_KEY_SEND"},
    {AS_POLICY_GEN_IGNORE, "POLICY_GEN_IGNORE"},
    {AS_POLICY_GEN_EQ, "POLICY_GEN_EQ"},
    {AS_POLICY_GEN_GT, "POLICY_GEN_GT"},
    {AS_JOB_STATUS_COMPLETED, "JOB_STATUS_COMPLETED"},
    {AS_JOB_STATUS_UNDEF, "JOB_STATUS_UNDEF"},
    {AS_JOB_STATUS_INPROGRESS, "JOB_STATUS_INPROGRESS"},
    {AS_POLICY_REPLICA_MASTER, "POLICY_REPLICA_MASTER"},
    {AS_POLICY_REPLICA_ANY, "POLICY_REPLICA_ANY"},
    {AS_POLICY_REPLICA_SEQUENCE, "POLICY_REPLICA_SEQUENCE"},
    {AS_POLICY_REPLICA_PREFER_RACK, "POLICY_REPLICA_PREFER_RACK"},
    {AS_POLICY_COMMIT_LEVEL_ALL, "POLICY_COMMIT_LEVEL_ALL"},
    {AS_POLICY_COMMIT_LEVEL_MASTER, "POLICY_COMMIT_LEVEL_MASTER"},
    {SERIALIZER_USER, "SERIALIZER_USER"},
    {SERIALIZER_JSON, "SERIALIZER_JSON"},
    {SERIALIZER_NONE, "SERIALIZER_NONE"},
    {SEND_BOOL_AS_INTEGER, "INTEGER"},
    {SEND_BOOL_AS_AS_BOOL, "AS_BOOL"},
    {AS_INDEX_STRING, "INDEX_STRING"},
    {AS_INDEX_NUMERIC, "INDEX_NUMERIC"},
    {AS_INDEX_GEO2DSPHERE, "INDEX_GEO2DSPHERE"},
    {AS_INDEX_BLOB, "INDEX_BLOB"},
    {AS_INDEX_TYPE_DEFAULT, "INDEX_TYPE_DEFAULT"},
    {AS_INDEX_TYPE_LIST, "INDEX_TYPE_LIST"},
    {AS_INDEX_TYPE_MAPKEYS, "INDEX_TYPE_MAPKEYS"},
    {AS_INDEX_TYPE_MAPVALUES, "INDEX_TYPE_MAPVALUES"},
    {AS_PRIVILEGE_USER_ADMIN, "PRIV_USER_ADMIN"},
    {AS_PRIVILEGE_SYS_ADMIN, "PRIV_SYS_ADMIN"},
    {AS_PRIVILEGE_DATA_ADMIN, "PRIV_DATA_ADMIN"},
    {AS_PRIVILEGE_READ, "PRIV_READ"},
    {AS_PRIVILEGE_WRITE, "PRIV_WRITE"},
    {AS_PRIVILEGE_READ_WRITE, "PRIV_READ_WRITE"},
    {AS_PRIVILEGE_READ_WRITE_UDF, "PRIV_READ_WRITE_UDF"},
    {AS_PRIVILEGE_TRUNCATE, "PRIV_TRUNCATE"},
    {AS_PRIVILEGE_UDF_ADMIN, "PRIV_UDF_ADMIN"},
    {AS_PRIVILEGE_SINDEX_ADMIN, "PRIV_SINDEX_ADMIN"},

    {OP_LIST_APPEND, "OP_LIST_APPEND"},
    {OP_LIST_APPEND_ITEMS, "OP_LIST_APPEND_ITEMS"},
    {OP_LIST_INSERT, "OP_LIST_INSERT"},
    {OP_LIST_INSERT_ITEMS, "OP_LIST_INSERT_ITEMS"},
    {OP_LIST_POP, "OP_LIST_POP"},
    {OP_LIST_POP_RANGE, "OP_LIST_POP_RANGE"},
    {OP_LIST_REMOVE, "OP_LIST_REMOVE"},
    {OP_LIST_REMOVE_RANGE, "OP_LIST_REMOVE_RANGE"},
    {OP_LIST_CLEAR, "OP_LIST_CLEAR"},
    {OP_LIST_SET, "OP_LIST_SET"},
    {OP_LIST_GET, "OP_LIST_GET"},
    {OP_LIST_GET_RANGE, "OP_LIST_GET_RANGE"},
    {OP_LIST_TRIM, "OP_LIST_TRIM"},
    {OP_LIST_SIZE, "OP_LIST_SIZE"},
    {OP_LIST_INCREMENT, "OP_LIST_INCREMENT"},

    {OP_MAP_SET_POLICY, "OP_MAP_SET_POLICY"},
    {OP_MAP_CREATE, "OP_MAP_CREATE"},
    {OP_MAP_PUT, "OP_MAP_PUT"},
    {OP_MAP_PUT_ITEMS, "OP_MAP_PUT_ITEMS"},
    {OP_MAP_INCREMENT, "OP_MAP_INCREMENT"},
    {OP_MAP_DECREMENT, "OP_MAP_DECREMENT"},
    {OP_MAP_SIZE, "OP_MAP_SIZE"},
    {OP_MAP_CLEAR, "OP_MAP_CLEAR"},
    {OP_MAP_REMOVE_BY_KEY, "OP_MAP_REMOVE_BY_KEY"},
    {OP_MAP_REMOVE_BY_KEY_LIST, "OP_MAP_REMOVE_BY_KEY_LIST"},
    {OP_MAP_REMOVE_BY_KEY_RANGE, "OP_MAP_REMOVE_BY_KEY_RANGE"},
    {OP_MAP_REMOVE_BY_VALUE, "OP_MAP_REMOVE_BY_VALUE"},
    {OP_MAP_REMOVE_BY_VALUE_LIST, "OP_MAP_REMOVE_BY_VALUE_LIST"},
    {OP_MAP_REMOVE_BY_VALUE_RANGE, "OP_MAP_REMOVE_BY_VALUE_RANGE"},
    {OP_MAP_REMOVE_BY_INDEX, "OP_MAP_REMOVE_BY_INDEX"},
    {OP_MAP_REMOVE_BY_INDEX_RANGE, "OP_MAP_REMOVE_BY_INDEX_RANGE"},
    {OP_MAP_REMOVE_BY_RANK, "OP_MAP_REMOVE_BY_RANK"},
    {OP_MAP_REMOVE_BY_RANK_RANGE, "OP_MAP_REMOVE_BY_RANK_RANGE"},
    {OP_MAP_GET_BY_KEY, "OP_MAP_GET_BY_KEY"},
    {OP_MAP_GET_BY_KEY_RANGE, "OP_MAP_GET_BY_KEY_RANGE"},
    {OP_MAP_GET_BY_VALUE, "OP_MAP_GET_BY_VALUE"},
    {OP_MAP_GET_BY_VALUE_RANGE, "OP_MAP_GET_BY_VALUE_RANGE"},
    {OP_MAP_GET_BY_INDEX, "OP_MAP_GET_BY_INDEX"},
    {OP_MAP_GET_BY_INDEX_RANGE, "OP_MAP_GET_BY_INDEX_RANGE"},
    {OP_MAP_GET_BY_RANK, "OP_MAP_GET_BY_RANK"},
    {OP_MAP_GET_BY_RANK_RANGE, "OP_MAP_GET_BY_RANK_RANGE"},
    {OP_MAP_GET_BY_VALUE_LIST, "OP_MAP_GET_BY_VALUE_LIST"},
    {OP_MAP_GET_BY_KEY_LIST, "OP_MAP_GET_BY_KEY_LIST"},

    {AS_MAP_UNORDERED, "MAP_UNORDERED"},
    {AS_MAP_KEY_ORDERED, "MAP_KEY_ORDERED"},
    {AS_MAP_KEY_VALUE_ORDERED, "MAP_KEY_VALUE_ORDERED"},

    {AS_MAP_RETURN_NONE, "MAP_RETURN_NONE"},
    {AS_MAP_RETURN_INDEX, "MAP_RETURN_INDEX"},
    {AS_MAP_RETURN_REVERSE_INDEX, "MAP_RETURN_REVERSE_INDEX"},
    {AS_MAP_RETURN_RANK, "MAP_RETURN_RANK"},
    {AS_MAP_RETURN_REVERSE_RANK, "MAP_RETURN_REVERSE_RANK"},
    {AS_MAP_RETURN_COUNT, "MAP_RETURN_COUNT"},
    {AS_MAP_RETURN_KEY, "MAP_RETURN_KEY"},
    {AS_MAP_RETURN_VALUE, "MAP_RETURN_VALUE"},
    {AS_MAP_RETURN_KEY_VALUE, "MAP_RETURN_KEY_VALUE"},
    {AS_MAP_RETURN_EXISTS, "MAP_RETURN_EXISTS"},
    {AS_MAP_RETURN_ORDERED_MAP, "MAP_RETURN_ORDERED_MAP"},
    {AS_MAP_RETURN_UNORDERED_MAP, "MAP_RETURN_UNORDERED_MAP"},

    {AS_RECORD_DEFAULT_TTL, "TTL_NAMESPACE_DEFAULT"},
    {AS_RECORD_NO_EXPIRE_TTL, "TTL_NEVER_EXPIRE"},
    {AS_RECORD_NO_CHANGE_TTL, "TTL_DONT_UPDATE"},
    {AS_RECORD_CLIENT_DEFAULT_TTL, "TTL_CLIENT_DEFAULT"},
    {AS_AUTH_INTERNAL, "AUTH_INTERNAL"},
    {AS_AUTH_EXTERNAL, "AUTH_EXTERNAL"},
    {AS_AUTH_EXTERNAL_INSECURE, "AUTH_EXTERNAL_INSECURE"},
    {AS_AUTH_PKI, "AUTH_PKI"},
    /* New CDT Operations, post 3.16.0.1 */
    {OP_LIST_GET_BY_INDEX, "OP_LIST_GET_BY_INDEX"},
    {OP_LIST_GET_BY_INDEX_RANGE, "OP_LIST_GET_BY_INDEX_RANGE"},
    {OP_LIST_GET_BY_RANK, "OP_LIST_GET_BY_RANK"},
    {OP_LIST_GET_BY_RANK_RANGE, "OP_LIST_GET_BY_RANK_RANGE"},
    {OP_LIST_GET_BY_VALUE, "OP_LIST_GET_BY_VALUE"},
    {OP_LIST_GET_BY_VALUE_LIST, "OP_LIST_GET_BY_VALUE_LIST"},
    {OP_LIST_GET_BY_VALUE_RANGE, "OP_LIST_GET_BY_VALUE_RANGE"},
    {OP_LIST_REMOVE_BY_INDEX, "OP_LIST_REMOVE_BY_INDEX"},
    {OP_LIST_REMOVE_BY_INDEX_RANGE, "OP_LIST_REMOVE_BY_INDEX_RANGE"},
    {OP_LIST_REMOVE_BY_RANK, "OP_LIST_REMOVE_BY_RANK"},
    {OP_LIST_REMOVE_BY_RANK_RANGE, "OP_LIST_REMOVE_BY_RANK_RANGE"},
    {OP_LIST_REMOVE_BY_VALUE, "OP_LIST_REMOVE_BY_VALUE"},
    {OP_LIST_REMOVE_BY_VALUE_LIST, "OP_LIST_REMOVE_BY_VALUE_LIST"},
    {OP_LIST_REMOVE_BY_VALUE_RANGE, "OP_LIST_REMOVE_BY_VALUE_RANGE"},
    {OP_LIST_SET_ORDER, "OP_LIST_SET_ORDER"},
    {OP_LIST_SORT, "OP_LIST_SORT"},
    {AS_LIST_RETURN_NONE, "LIST_RETURN_NONE"},
    {AS_LIST_RETURN_INDEX, "LIST_RETURN_INDEX"},
    {AS_LIST_RETURN_REVERSE_INDEX, "LIST_RETURN_REVERSE_INDEX"},
    {AS_LIST_RETURN_RANK, "LIST_RETURN_RANK"},
    {AS_LIST_RETURN_REVERSE_RANK, "LIST_RETURN_REVERSE_RANK"},
    {AS_LIST_RETURN_COUNT, "LIST_RETURN_COUNT"},
    {AS_LIST_RETURN_VALUE, "LIST_RETURN_VALUE"},
    {AS_LIST_RETURN_EXISTS, "LIST_RETURN_EXISTS"},
    {AS_LIST_SORT_DROP_DUPLICATES, "LIST_SORT_DROP_DUPLICATES"},
    {AS_LIST_SORT_DEFAULT, "LIST_SORT_DEFAULT"},
    {AS_LIST_WRITE_DEFAULT, "LIST_WRITE_DEFAULT"},
    {AS_LIST_WRITE_ADD_UNIQUE, "LIST_WRITE_ADD_UNIQUE"},
    {AS_LIST_WRITE_INSERT_BOUNDED, "LIST_WRITE_INSERT_BOUNDED"},
    {AS_LIST_ORDERED, "LIST_ORDERED"},
    {AS_LIST_UNORDERED, "LIST_UNORDERED"},
    {OP_LIST_REMOVE_BY_VALUE_RANK_RANGE_REL,
     "OP_LIST_REMOVE_BY_VALUE_RANK_RANGE_REL"},
    {OP_LIST_GET_BY_VALUE_RANK_RANGE_REL,
     "OP_LIST_GET_BY_VALUE_RANK_RANGE_REL"},
    {OP_LIST_CREATE, "OP_LIST_CREATE"},

    /* CDT operations for use with expressions, new in 5.0 */
    {OP_MAP_REMOVE_BY_VALUE_RANK_RANGE_REL,
     "OP_MAP_REMOVE_BY_VALUE_RANK_RANGE_REL"},
    {OP_MAP_REMOVE_BY_KEY_INDEX_RANGE_REL,
     "OP_MAP_REMOVE_BY_KEY_INDEX_RANGE_REL"},
    {OP_MAP_GET_BY_VALUE_RANK_RANGE_REL, "OP_MAP_GET_BY_VALUE_RANK_RANGE_REL"},
    {OP_MAP_GET_BY_KEY_INDEX_RANGE_REL, "OP_MAP_GET_BY_KEY_INDEX_RANGE_REL"},

    {OP_LIST_GET_BY_VALUE_RANK_RANGE_REL_TO_END,
     "OP_LIST_GET_BY_VALUE_RANK_RANGE_REL_TO_END"},
    {OP_LIST_GET_BY_INDEX_RANGE_TO_END, "OP_LIST_GET_BY_INDEX_RANGE_TO_END"},
    {OP_LIST_GET_BY_RANK_RANGE_TO_END, "OP_LIST_GET_BY_RANK_RANGE_TO_END"},
    {OP_LIST_REMOVE_BY_REL_RANK_RANGE_TO_END,
     "OP_LIST_REMOVE_BY_REL_RANK_RANGE_TO_END"},
    {OP_LIST_REMOVE_BY_REL_RANK_RANGE, "OP_LIST_REMOVE_BY_REL_RANK_RANGE"},
    {OP_LIST_REMOVE_BY_INDEX_RANGE_TO_END,
     "OP_LIST_REMOVE_BY_INDEX_RANGE_TO_END"},
    {OP_LIST_REMOVE_BY_RANK_RANGE_TO_END,
     "OP_LIST_REMOVE_BY_RANK_RANGE_TO_END"},

    {AS_MAP_WRITE_NO_FAIL, "MAP_WRITE_NO_FAIL"},
    {AS_MAP_WRITE_PARTIAL, "MAP_WRITE_PARTIAL"},

    {AS_LIST_WRITE_NO_FAIL, "LIST_WRITE_NO_FAIL"},
    {AS_LIST_WRITE_PARTIAL, "LIST_WRITE_PARTIAL"},

    /* Map write flags post 3.5.0 */
    {AS_MAP_WRITE_DEFAULT, "MAP_WRITE_FLAGS_DEFAULT"},
    {AS_MAP_WRITE_CREATE_ONLY, "MAP_WRITE_FLAGS_CREATE_ONLY"},
    {AS_MAP_WRITE_UPDATE_ONLY, "MAP_WRITE_FLAGS_UPDATE_ONLY"},
    {AS_MAP_WRITE_NO_FAIL, "MAP_WRITE_FLAGS_NO_FAIL"},
    {AS_MAP_WRITE_PARTIAL, "MAP_WRITE_FLAGS_PARTIAL"},

    /* READ Mode constants 4.0.0 */

    // AP Read Mode
    {AS_POLICY_READ_MODE_AP_ONE, "POLICY_READ_MODE_AP_ONE"},
    {AS_POLICY_READ_MODE_AP_ALL, "POLICY_READ_MODE_AP_ALL"},
    // SC Read Mode
    {AS_POLICY_READ_MODE_SC_SESSION, "POLICY_READ_MODE_SC_SESSION"},
    {AS_POLICY_READ_MODE_SC_LINEARIZE, "POLICY_READ_MODE_SC_LINEARIZE"},
    {AS_POLICY_READ_MODE_SC_ALLOW_REPLICA, "POLICY_READ_MODE_SC_ALLOW_REPLICA"},
    {AS_POLICY_READ_MODE_SC_ALLOW_UNAVAILABLE,
     "POLICY_READ_MODE_SC_ALLOW_UNAVAILABLE"},

    /* Bitwise constants: 3.9.0 */
    {AS_BIT_WRITE_DEFAULT, "BIT_WRITE_DEFAULT"},
    {AS_BIT_WRITE_CREATE_ONLY, "BIT_WRITE_CREATE_ONLY"},
    {AS_BIT_WRITE_UPDATE_ONLY, "BIT_WRITE_UPDATE_ONLY"},
    {AS_BIT_WRITE_NO_FAIL, "BIT_WRITE_NO_FAIL"},
    {AS_BIT_WRITE_PARTIAL, "BIT_WRITE_PARTIAL"},

    {AS_BIT_RESIZE_DEFAULT, "BIT_RESIZE_DEFAULT"},
    {AS_BIT_RESIZE_FROM_FRONT, "BIT_RESIZE_FROM_FRONT"},
    {AS_BIT_RESIZE_GROW_ONLY, "BIT_RESIZE_GROW_ONLY"},
    {AS_BIT_RESIZE_SHRINK_ONLY, "BIT_RESIZE_SHRINK_ONLY"},

    {AS_BIT_OVERFLOW_FAIL, "BIT_OVERFLOW_FAIL"},
    {AS_BIT_OVERFLOW_SATURATE, "BIT_OVERFLOW_SATURATE"},
    {AS_BIT_OVERFLOW_WRAP, "BIT_OVERFLOW_WRAP"},

    /* BITWISE OPS: 3.9.0 */
    {OP_BIT_INSERT, "OP_BIT_INSERT"},
    {OP_BIT_RESIZE, "OP_BIT_RESIZE"},
    {OP_BIT_REMOVE, "OP_BIT_REMOVE"},
    {OP_BIT_SET, "OP_BIT_SET"},
    {OP_BIT_OR, "OP_BIT_OR"},
    {OP_BIT_XOR, "OP_BIT_XOR"},
    {OP_BIT_AND, "OP_BIT_AND"},
    {OP_BIT_NOT, "OP_BIT_NOT"},
    {OP_BIT_LSHIFT, "OP_BIT_LSHIFT"},
    {OP_BIT_RSHIFT, "OP_BIT_RSHIFT"},
    {OP_BIT_ADD, "OP_BIT_ADD"},
    {OP_BIT_SUBTRACT, "OP_BIT_SUBTRACT"},
    {OP_BIT_GET_INT, "OP_BIT_GET_INT"},
    {OP_BIT_SET_INT, "OP_BIT_SET_INT"},
    {OP_BIT_GET, "OP_BIT_GET"},
    {OP_BIT_COUNT, "OP_BIT_COUNT"},
    {OP_BIT_LSCAN, "OP_BIT_LSCAN"},
    {OP_BIT_RSCAN, "OP_BIT_RSCAN"},

    /* Nested CDT constants: 3.9.0 */
    {AS_CDT_CTX_LIST_INDEX, "CDT_CTX_LIST_INDEX"},
    {AS_CDT_CTX_LIST_RANK, "CDT_CTX_LIST_RANK"},
    {AS_CDT_CTX_LIST_VALUE, "CDT_CTX_LIST_VALUE"},
    {CDT_CTX_LIST_INDEX_CREATE, "CDT_CTX_LIST_INDEX_CREATE"},
    {AS_CDT_CTX_MAP_INDEX, "CDT_CTX_MAP_INDEX"},
    {AS_CDT_CTX_MAP_RANK, "CDT_CTX_MAP_RANK"},
    {AS_CDT_CTX_MAP_KEY, "CDT_CTX_MAP_KEY"},
    {AS_CDT_CTX_MAP_VALUE, "CDT_CTX_MAP_VALUE"},
    {CDT_CTX_MAP_KEY_CREATE, "CDT_CTX_MAP_KEY_CREATE"},

    /* HLL constants 3.11.0 */
    {OP_HLL_ADD, "OP_HLL_ADD"},
    {OP_HLL_DESCRIBE, "OP_HLL_DESCRIBE"},
    {OP_HLL_FOLD, "OP_HLL_FOLD"},
    {OP_HLL_GET_COUNT, "OP_HLL_GET_COUNT"},
    {OP_HLL_GET_INTERSECT_COUNT, "OP_HLL_GET_INTERSECT_COUNT"},
    {OP_HLL_GET_SIMILARITY, "OP_HLL_GET_SIMILARITY"},
    {OP_HLL_GET_UNION, "OP_HLL_GET_UNION"},
    {OP_HLL_GET_UNION_COUNT, "OP_HLL_GET_UNION_COUNT"},
    {OP_HLL_GET_SIMILARITY, "OP_HLL_GET_SIMILARITY"},
    {OP_HLL_INIT, "OP_HLL_INIT"},
    {OP_HLL_REFRESH_COUNT, "OP_HLL_REFRESH_COUNT"},
    {OP_HLL_SET_UNION, "OP_HLL_SET_UNION"},
    {OP_HLL_MAY_CONTAIN, "OP_HLL_MAY_CONTAIN"}, // for expression filters

    {AS_HLL_WRITE_DEFAULT, "HLL_WRITE_DEFAULT"},
    {AS_HLL_WRITE_CREATE_ONLY, "HLL_WRITE_CREATE_ONLY"},
    {AS_HLL_WRITE_UPDATE_ONLY, "HLL_WRITE_UPDATE_ONLY"},
    {AS_HLL_WRITE_NO_FAIL, "HLL_WRITE_NO_FAIL"},
    {AS_HLL_WRITE_ALLOW_FOLD, "HLL_WRITE_ALLOW_FOLD"},

    {OP_MAP_REMOVE_BY_KEY_REL_INDEX_RANGE_TO_END,
     "OP_MAP_REMOVE_BY_KEY_REL_INDEX_RANGE_TO_END"},
    {OP_MAP_REMOVE_BY_VALUE_REL_RANK_RANGE_TO_END,
     "OP_MAP_REMOVE_BY_VALUE_REL_RANK_RANGE_TO_END"},
    {OP_MAP_REMOVE_BY_INDEX_RANGE_TO_END,
     "OP_MAP_REMOVE_BY_INDEX_RANGE_TO_END"},
    {OP_MAP_REMOVE_BY_RANK_RANGE_TO_END, "OP_MAP_REMOVE_BY_RANK_RANGE_TO_END"},
    {OP_MAP_GET_BY_KEY_REL_INDEX_RANGE_TO_END,
     "OP_MAP_GET_BY_KEY_REL_INDEX_RANGE_TO_END"},
    {OP_MAP_REMOVE_BY_KEY_REL_INDEX_RANGE,
     "OP_MAP_REMOVE_BY_KEY_REL_INDEX_RANGE"},
    {OP_MAP_REMOVE_BY_VALUE_REL_INDEX_RANGE,
     "OP_MAP_REMOVE_BY_VALUE_REL_INDEX_RANGE"},
    {OP_MAP_REMOVE_BY_VALUE_REL_RANK_RANGE,
     "OP_MAP_REMOVE_BY_VALUE_REL_RANK_RANGE"},
    {OP_MAP_GET_BY_KEY_REL_INDEX_RANGE, "OP_MAP_GET_BY_KEY_REL_INDEX_RANGE"},
    {OP_MAP_GET_BY_VALUE_RANK_RANGE_REL_TO_END,
     "OP_MAP_GET_BY_VALUE_RANK_RANGE_REL_TO_END"},
    {OP_MAP_GET_BY_INDEX_RANGE_TO_END, "OP_MAP_GET_BY_INDEX_RANGE_TO_END"},
    {OP_MAP_GET_BY_RANK_RANGE_TO_END, "OP_MAP_GET_BY_RANK_RANGE_TO_END"},

    /* Expression operation constants 5.1.0 */
    {OP_EXPR_READ, "OP_EXPR_READ"},
    {OP_EXPR_WRITE, "OP_EXPR_WRITE"},
    {AS_EXP_WRITE_DEFAULT, "EXP_WRITE_DEFAULT"},
    {AS_EXP_WRITE_CREATE_ONLY, "EXP_WRITE_CREATE_ONLY"},
    {AS_EXP_WRITE_UPDATE_ONLY, "EXP_WRITE_UPDATE_ONLY"},
    {AS_EXP_WRITE_ALLOW_DELETE, "EXP_WRITE_ALLOW_DELETE"},
    {AS_EXP_WRITE_POLICY_NO_FAIL, "EXP_WRITE_POLICY_NO_FAIL"},
    {AS_EXP_WRITE_EVAL_NO_FAIL, "EXP_WRITE_EVAL_NO_FAIL"},
    {AS_EXP_READ_DEFAULT, "EXP_READ_DEFAULT"},
    {AS_EXP_READ_EVAL_NO_FAIL, "EXP_READ_EVAL_NO_FAIL"},

    /* For BinType expression, as_bytes_type */
    {AS_BYTES_UNDEF, "AS_BYTES_UNDEF"},
    {AS_BYTES_INTEGER, "AS_BYTES_INTEGER"},
    {AS_BYTES_DOUBLE, "AS_BYTES_DOUBLE"},
    {AS_BYTES_STRING, "AS_BYTES_STRING"},
    {AS_BYTES_BLOB, "AS_BYTES_BLOB"},
    {AS_BYTES_JAVA, "AS_BYTES_JAVA"},
    {AS_BYTES_CSHARP, "AS_BYTES_CSHARP"},
    {AS_BYTES_PYTHON, "AS_BYTES_PYTHON"},
    {AS_BYTES_RUBY, "AS_BYTES_RUBY"},
    {AS_BYTES_PHP, "AS_BYTES_PHP"},
    {AS_BYTES_ERLANG, "AS_BYTES_ERLANG"},
    {AS_BYTES_BOOL, "AS_BYTES_BOOL"},
    {AS_BYTES_HLL, "AS_BYTES_HLL"},
    {AS_BYTES_MAP, "AS_BYTES_MAP"},
    {AS_BYTES_LIST, "AS_BYTES_LIST"},
    {AS_BYTES_GEOJSON, "AS_BYTES_GEOJSON"},
    {AS_BYTES_TYPE_MAX, "AS_BYTES_TYPE_MAX"},

    /* Regex constants from predexp, still used by expressions */
    {REGEX_NONE, "REGEX_NONE"},
    {REGEX_EXTENDED, "REGEX_EXTENDED"},
    {REGEX_ICASE, "REGEX_ICASE"},
    {REGEX_NOSUB, "REGEX_NOSUB"},
    {REGEX_NEWLINE, "REGEX_NEWLINE"},

    {AS_QUERY_DURATION_LONG, "QUERY_DURATION_LONG"},
    {AS_QUERY_DURATION_LONG_RELAX_AP, "QUERY_DURATION_LONG_RELAX_AP"},
    {AS_QUERY_DURATION_SHORT, "QUERY_DURATION_SHORT"}};

static AerospikeJobConstants aerospike_job_constants[] = {
    {"scan", "JOB_SCAN"}, {"query", "JOB_QUERY"}};
/**
 * Function for setting scan parameters in scan.
 * Like Percentage, Concurrent, Nobins
 *
 * @param err                   The as_error to be populated by the function
 *                              with the encountered error if any.
 * @scan_p                      Scan parameter.
 * @py_options                  The user's optional scan options.
 */
void set_scan_options(as_error *err, as_scan *scan_p, PyObject *py_options)
{
    if (!scan_p) {
        as_error_update(err, AEROSPIKE_ERR_CLIENT, "Scan is not initialized");
        return;
    }

    if (PyDict_Check(py_options)) {
        PyObject *key = NULL, *value = NULL;
        Py_ssize_t pos = 0;
        int64_t val = 0;
        while (PyDict_Next(py_options, &pos, &key, &value)) {
            char *key_name = (char *)PyUnicode_AsUTF8(key);
            if (!PyUnicode_Check(key)) {
                as_error_update(err, AEROSPIKE_ERR_PARAM,
                                "Policy key must be string");
                break;
            }

            if (strcmp("concurrent", key_name) == 0) {
                if (!PyBool_Check(value)) {
                    as_error_update(err, AEROSPIKE_ERR_PARAM,
                                    "Invalid value(type) for concurrent");
                    break;
                }
                val = (int64_t)PyObject_IsTrue(value);
                if (val == -1 || (!as_scan_set_concurrent(scan_p, val))) {
                    as_error_update(err, AEROSPIKE_ERR_PARAM,
                                    "Unable to set scan concurrent");
                    break;
                }
            }
            else if (strcmp("nobins", key_name) == 0) {
                if (!PyBool_Check(value)) {
                    as_error_update(err, AEROSPIKE_ERR_PARAM,
                                    "Invalid value(type) for nobins");
                    break;
                }
                val = (int64_t)PyObject_IsTrue(value);
                if (val == -1 || (!as_scan_set_nobins(scan_p, val))) {
                    as_error_update(err, AEROSPIKE_ERR_PARAM,
                                    "Unable to set scan nobins");
                    break;
                }
            }
            else {
                as_error_update(err, AEROSPIKE_ERR_PARAM,
                                "Invalid value for scan options");
                break;
            }
        }
    }
    else {
        as_error_update(err, AEROSPIKE_ERR_PARAM, "Invalid option(type)");
    }
}

as_status set_query_options(as_error *err, PyObject *query_options,
                            as_query *query)
{
    PyObject *no_bins_val = NULL;
    if (!query_options || query_options == Py_None) {
        return AEROSPIKE_OK;
    }

    if (!PyDict_Check(query_options)) {
        return as_error_update(err, AEROSPIKE_ERR_PARAM,
                               "query options must be a dictionary");
    }

    no_bins_val = PyDict_GetItemString(query_options, "nobins");
    if (no_bins_val) {
        if (!PyBool_Check(no_bins_val)) {
            return as_error_update(err, AEROSPIKE_ERR_PARAM,
                                   "nobins value must be a bool");
        }
        query->no_bins = PyObject_IsTrue(no_bins_val);
    }
    return AEROSPIKE_OK;
}
/**
 * Declares policy constants.
 */
as_status declare_policy_constants(PyObject *aerospike)
{
    as_status status = AEROSPIKE_OK;
    int i;

    if (!aerospike) {
        status = AEROSPIKE_ERR;
        goto exit;
    }
    for (i = 0; i < (int)AEROSPIKE_CONSTANTS_ARR_SIZE; i++) {
        PyModule_AddIntConstant(aerospike, aerospike_constants[i].constant_str,
                                aerospike_constants[i].constantno);
    }

    for (i = 0; i < (int)AEROSPIKE_JOB_CONSTANTS_ARR_SIZE; i++) {
        PyModule_AddStringConstant(aerospike,
                                   aerospike_job_constants[i].exposed_job_str,
                                   aerospike_job_constants[i].job_str);
    }
exit:
    return status;
}

/**
 * Converts a PyObject into an as_policy_admin object.
 * Returns AEROSPIKE_OK on success. On error, the err argument is populated.
 * We assume that the error object and the policy object are already allocated
 * and initialized (although, we do reset the error object here).
 */
as_status pyobject_to_policy_admin(AerospikeClient *self, as_error *err,
                                   PyObject *py_policy, // remove self
                                   as_policy_admin *policy,
                                   as_policy_admin **policy_p,
                                   as_policy_admin *config_admin_policy)
{

    if (py_policy && py_policy != Py_None) {
        // Initialize Policy
        POLICY_INIT(as_policy_admin);
    }
    //Initialize policy with global defaults
    as_policy_admin_copy(config_admin_policy, policy);

    if (py_policy && py_policy != Py_None) {
        // Set policy fields
        POLICY_SET_FIELD(timeout, uint32_t);
    }
    // Update the policy
    POLICY_UPDATE();

    return err->code;
}

/**
 * Converts a PyObject into an as_policy_apply object.
 * Returns AEROSPIKE_OK on success. On error, the err argument is populated.
 * We assume that the error object and the policy object are already allocated
 * and initialized (although, we do reset the error object here).
 */
as_status pyobject_to_policy_apply(AerospikeClient *self, as_error *err,
                                   PyObject *py_policy, as_policy_apply *policy,
                                   as_policy_apply **policy_p,
                                   as_policy_apply *config_apply_policy,
                                   as_exp *exp_list, as_exp **exp_list_p)
{
    if (py_policy && py_policy != Py_None) {
        // Initialize Policy
        POLICY_INIT(as_policy_apply);
    }
    //Initialize policy with global defaults
    as_policy_apply_copy(config_apply_policy, policy);

    if (py_policy && py_policy != Py_None) {
        // Set policy fields
        POLICY_SET_BASE_FIELD(total_timeout, uint32_t);
        POLICY_SET_BASE_FIELD(socket_timeout, uint32_t);
        POLICY_SET_BASE_FIELD(max_retries, uint32_t);
        POLICY_SET_BASE_FIELD(sleep_between_retries, uint32_t);
        POLICY_SET_BASE_FIELD(compress, bool);

        POLICY_SET_FIELD(key, as_policy_key);
        POLICY_SET_FIELD(replica, as_policy_replica);
        //POLICY_SET_FIELD(gen, as_policy_gen); removed
        POLICY_SET_FIELD(commit_level, as_policy_commit_level);
        POLICY_SET_FIELD(durable_delete, bool);
        POLICY_SET_FIELD(ttl, uint32_t);

        // C client 5.0 new expressions
        POLICY_SET_EXPRESSIONS_BASE_FIELD();
    }

    // Update the policy
    POLICY_UPDATE();

    return err->code;
}

/**
 * Converts a PyObject into an as_policy_info object.
 * Returns AEROSPIKE_OK on success. On error, the err argument is populated.
 * We assume that the error object and the policy object are already allocated
 * and initialized (although, we do reset the error object here).
 */
as_status pyobject_to_policy_info(as_error *err, PyObject *py_policy,
                                  as_policy_info *policy,
                                  as_policy_info **policy_p,
                                  as_policy_info *config_info_policy)
{
    if (py_policy && py_policy != Py_None) {
        // Initialize Policy
        POLICY_INIT(as_policy_info);
    }
    //Initialize policy with global defaults
    as_policy_info_copy(config_info_policy, policy);

    if (py_policy && py_policy != Py_None) {
        // Set policy fields
        POLICY_SET_FIELD(timeout, uint32_t);
        POLICY_SET_FIELD(send_as_is, bool);
        POLICY_SET_FIELD(check_bounds, bool);
    }
    // Update the policy
    POLICY_UPDATE();

    return err->code;
}

/**
 * Converts a PyObject into an as_policy_query object.
 * Returns AEROSPIKE_OK on success. On error, the err argument is populated.
 * We assume that the error object and the policy object are already allocated
 * and initialized (although, we do reset the error object here).
 * exp_list are initialized by this function, caller must free.
 */
as_status pyobject_to_policy_query(AerospikeClient *self, as_error *err,
                                   PyObject *py_policy, as_policy_query *policy,
                                   as_policy_query **policy_p,
                                   as_policy_query *config_query_policy,
                                   as_exp *exp_list, as_exp **exp_list_p)
{
    if (py_policy && py_policy != Py_None) {
        // Initialize Policy
        POLICY_INIT(as_policy_query);
    }
    //Initialize policy with global defaults
    as_policy_query_copy(config_query_policy, policy);

    if (py_policy && py_policy != Py_None) {
        // Set policy fields
        POLICY_SET_BASE_FIELD(total_timeout, uint32_t);
        POLICY_SET_BASE_FIELD(socket_timeout, uint32_t);
        POLICY_SET_BASE_FIELD(max_retries, uint32_t);
        POLICY_SET_BASE_FIELD(sleep_between_retries, uint32_t);
        POLICY_SET_BASE_FIELD(compress, bool);

        POLICY_SET_FIELD(deserialize, bool);
        POLICY_SET_FIELD(replica, as_policy_replica);

        // C client 5.0 new expressions
        POLICY_SET_EXPRESSIONS_BASE_FIELD();

        // C client 6.0.0
        POLICY_SET_FIELD(short_query, bool);

        POLICY_SET_FIELD(expected_duration, as_query_duration);
    }

    // Update the policy
    POLICY_UPDATE();

    return err->code;
}

/**
 * Converts a PyObject into an as_policy_read object.
 * Returns AEROSPIKE_OK on success. On error, the err argument is populated.
 * We assume that the error object and the policy object are already allocated
 * and initialized (although, we do reset the error object here).
 */
as_status pyobject_to_policy_read(AerospikeClient *self, as_error *err,
                                  PyObject *py_policy, as_policy_read *policy,
                                  as_policy_read **policy_p,
                                  as_policy_read *config_read_policy,
                                  as_exp *exp_list, as_exp **exp_list_p)
{
    if (py_policy && py_policy != Py_None) {
        // Initialize Policy
        POLICY_INIT(as_policy_read);
    }

    //Initialize policy with global defaults
    as_policy_read_copy(config_read_policy, policy);

    if (py_policy && py_policy != Py_None) {
        // Set policy fields
        POLICY_SET_BASE_FIELD(total_timeout, uint32_t);
        POLICY_SET_BASE_FIELD(socket_timeout, uint32_t);
        POLICY_SET_BASE_FIELD(max_retries, uint32_t);
        POLICY_SET_BASE_FIELD(sleep_between_retries, uint32_t);
        POLICY_SET_BASE_FIELD(compress, bool);

        POLICY_SET_FIELD(key, as_policy_key);
        POLICY_SET_FIELD(replica, as_policy_replica);
        POLICY_SET_FIELD(deserialize, bool);
        POLICY_SET_FIELD(read_touch_ttl_percent, int);

        // 4.0.0 new policies
        POLICY_SET_FIELD(read_mode_ap, as_policy_read_mode_ap);
        POLICY_SET_FIELD(read_mode_sc, as_policy_read_mode_sc);

        // C client 5.0 new expressions
        POLICY_SET_EXPRESSIONS_BASE_FIELD();
    }

    // Update the policy
    POLICY_UPDATE();

    return err->code;
}

/**
 * Converts a PyObject into an as_policy_remove object.
 * Returns AEROSPIKE_OK on success. On error, the err argument is populated.
 * We assume that the error object and the policy object are already allocated
 * and initialized (although, we do reset the error object here).
 */
as_status pyobject_to_policy_remove(AerospikeClient *self, as_error *err,
                                    PyObject *py_policy,
                                    as_policy_remove *policy,
                                    as_policy_remove **policy_p,
                                    as_policy_remove *config_remove_policy,
                                    as_exp *exp_list, as_exp **exp_list_p)
{
    if (py_policy && py_policy != Py_None) {
        // Initialize Policy
        POLICY_INIT(as_policy_remove);
    }
    //Initialize policy with global defaults
    as_policy_remove_copy(config_remove_policy, policy);

    if (py_policy && py_policy != Py_None) {
        // Set policy fields
        POLICY_SET_BASE_FIELD(total_timeout, uint32_t);
        POLICY_SET_BASE_FIELD(socket_timeout, uint32_t);
        POLICY_SET_BASE_FIELD(max_retries, uint32_t);
        POLICY_SET_BASE_FIELD(sleep_between_retries, uint32_t);
        POLICY_SET_BASE_FIELD(compress, bool);

        POLICY_SET_FIELD(generation, uint16_t);

        POLICY_SET_FIELD(key, as_policy_key);
        POLICY_SET_FIELD(gen, as_policy_gen);
        POLICY_SET_FIELD(commit_level, as_policy_commit_level);
        POLICY_SET_FIELD(replica, as_policy_replica);
        POLICY_SET_FIELD(durable_delete, bool);

        // C client 5.0 new expressions
        POLICY_SET_EXPRESSIONS_BASE_FIELD();
    }

    // Update the policy
    POLICY_UPDATE();

    return err->code;
}

/**
 * Converts a PyObject into an as_policy_scan object.
 * Returns AEROSPIKE_OK on success. On error, the err argument is populated.
 * We assume that the error object and the policy object are already allocated
 * and initialized (although, we do reset the error object here).
 */
as_status pyobject_to_policy_scan(AerospikeClient *self, as_error *err,
                                  PyObject *py_policy, as_policy_scan *policy,
                                  as_policy_scan **policy_p,
                                  as_policy_scan *config_scan_policy,
                                  as_exp *exp_list, as_exp **exp_list_p)
{
    if (py_policy && py_policy != Py_None) {
        // Initialize Policy
        POLICY_INIT(as_policy_scan);
    }
    //Initialize policy with global defaults
    as_policy_scan_copy(config_scan_policy, policy);

    if (py_policy && py_policy != Py_None) {
        // Set policy fields
        POLICY_SET_BASE_FIELD(total_timeout, uint32_t);
        POLICY_SET_BASE_FIELD(socket_timeout, uint32_t);
        POLICY_SET_BASE_FIELD(max_retries, uint32_t);
        POLICY_SET_BASE_FIELD(sleep_between_retries, uint32_t);
        POLICY_SET_BASE_FIELD(compress, bool);

        POLICY_SET_FIELD(durable_delete, bool);
        POLICY_SET_FIELD(records_per_second, uint32_t);
        POLICY_SET_FIELD(max_records, uint64_t);
        POLICY_SET_FIELD(replica, as_policy_replica);

        // C client 5.0 new expressions
        POLICY_SET_EXPRESSIONS_BASE_FIELD();
    }

    // Update the policy
    POLICY_UPDATE();

    return err->code;
}

/**
 * Converts a PyObject into an as_policy_write object.
 * Returns AEROSPIKE_OK on success. On error, the err argument is populated.
 * We assume that the error object and the policy object are already allocated
 * and initialized (although, we do reset the error object here).
 */
as_status pyobject_to_policy_write(AerospikeClient *self, as_error *err,
                                   PyObject *py_policy, as_policy_write *policy,
                                   as_policy_write **policy_p,
                                   as_policy_write *config_write_policy,
                                   as_exp *exp_list, as_exp **exp_list_p)
{
    if (py_policy && py_policy != Py_None) {
        // Initialize Policy
        POLICY_INIT(as_policy_write);
    }
    //Initialize policy with global defaults
    as_policy_write_copy(config_write_policy, policy);

    if (py_policy && py_policy != Py_None) {
        // Set policy fields
        // Base policy_fields
        POLICY_SET_BASE_FIELD(total_timeout, uint32_t);
        POLICY_SET_BASE_FIELD(socket_timeout, uint32_t);
        POLICY_SET_BASE_FIELD(max_retries, uint32_t);
        POLICY_SET_BASE_FIELD(sleep_between_retries, uint32_t);
        POLICY_SET_BASE_FIELD(compress, bool);

        POLICY_SET_FIELD(key, as_policy_key);
        POLICY_SET_FIELD(gen, as_policy_gen);
        POLICY_SET_FIELD(exists, as_policy_exists);
        POLICY_SET_FIELD(commit_level, as_policy_commit_level);
        POLICY_SET_FIELD(durable_delete, bool);
        POLICY_SET_FIELD(replica, as_policy_replica);
        POLICY_SET_FIELD(compression_threshold, uint32_t);

        // C client 5.0 new expressions
        POLICY_SET_EXPRESSIONS_BASE_FIELD();
    }

    // Update the policy
    POLICY_UPDATE();

    return err->code;
}

/**
 * Converts a PyObject into an as_policy_operate object.
 * Returns AEROSPIKE_OK on success. On error, the err argument is populated.
 * We assume that the error object and the policy object are already allocated
 * and initialized (although, we do reset the error object here).
 */
as_status pyobject_to_policy_operate(AerospikeClient *self, as_error *err,
                                     PyObject *py_policy,
                                     as_policy_operate *policy,
                                     as_policy_operate **policy_p,
                                     as_policy_operate *config_operate_policy,
                                     as_exp *exp_list, as_exp **exp_list_p)
{
    if (py_policy && py_policy != Py_None) {
        // Initialize Policy
        POLICY_INIT(as_policy_operate);
    }
    //Initialize policy with global defaults
    as_policy_operate_copy(config_operate_policy, policy);

    if (py_policy && py_policy != Py_None) {
        // Set policy fields
        POLICY_SET_BASE_FIELD(total_timeout, uint32_t);
        POLICY_SET_BASE_FIELD(socket_timeout, uint32_t);
        POLICY_SET_BASE_FIELD(max_retries, uint32_t);
        POLICY_SET_BASE_FIELD(sleep_between_retries, uint32_t);
        POLICY_SET_BASE_FIELD(compress, bool);

        POLICY_SET_FIELD(key, as_policy_key);
        POLICY_SET_FIELD(gen, as_policy_gen);
        POLICY_SET_FIELD(commit_level, as_policy_commit_level);
        POLICY_SET_FIELD(replica, as_policy_replica);
        POLICY_SET_FIELD(durable_delete, bool);
        POLICY_SET_FIELD(deserialize, bool);
        POLICY_SET_FIELD(exists, as_policy_exists);
        POLICY_SET_FIELD(read_touch_ttl_percent, int);

        // 4.0.0 new policies
        POLICY_SET_FIELD(read_mode_ap, as_policy_read_mode_ap);
        POLICY_SET_FIELD(read_mode_sc, as_policy_read_mode_sc);

        // C client 5.0 new expressions
        POLICY_SET_EXPRESSIONS_BASE_FIELD();
    }

    // Update the policy
    POLICY_UPDATE();

    return err->code;
}

/**
 * Converts a PyObject into an as_policy_batch object.
 * Returns AEROSPIKE_OK on success. On error, the err argument is populated.
 * We assume that the error object and the policy object are already allocated
 * and initialized (although, we do reset the error object here).
 */
as_status pyobject_to_policy_batch(AerospikeClient *self, as_error *err,
                                   PyObject *py_policy, as_policy_batch *policy,
                                   as_policy_batch **policy_p,
                                   as_policy_batch *config_batch_policy,
                                   as_exp *exp_list, as_exp **exp_list_p)
{
    if (py_policy && py_policy != Py_None) {
        // Initialize Policy
        POLICY_INIT(as_policy_batch);
    }
    //Initialize policy with global defaults
    as_policy_batch_copy(config_batch_policy, policy);

    if (py_policy && py_policy != Py_None) {
        // Set policy fields
        POLICY_SET_BASE_FIELD(total_timeout, uint32_t);
        POLICY_SET_BASE_FIELD(socket_timeout, uint32_t);
        POLICY_SET_BASE_FIELD(max_retries, uint32_t);
        POLICY_SET_BASE_FIELD(sleep_between_retries, uint32_t);
        POLICY_SET_BASE_FIELD(compress, bool);

        POLICY_SET_FIELD(concurrent, bool);
        POLICY_SET_FIELD(allow_inline, bool);
        POLICY_SET_FIELD(deserialize, bool);
        POLICY_SET_FIELD(replica, as_policy_replica);
        POLICY_SET_FIELD(read_touch_ttl_percent, int);

        // 4.0.0 new policies
        POLICY_SET_FIELD(read_mode_ap, as_policy_read_mode_ap);
        POLICY_SET_FIELD(read_mode_sc, as_policy_read_mode_sc);

        // C client 5.0 new expressions
        POLICY_SET_EXPRESSIONS_BASE_FIELD();

        // C client 6.0.0 (batch writes)
        POLICY_SET_FIELD(allow_inline_ssd, bool);
        POLICY_SET_FIELD(respond_all_keys, bool);
    }

    // Update the policy
    POLICY_UPDATE();

    return err->code;
}

// New with server 6.0, C client 5.2.0 (batch writes)
as_status pyobject_to_batch_write_policy(AerospikeClient *self, as_error *err,
                                         PyObject *py_policy,
                                         as_policy_batch_write *policy,
                                         as_policy_batch_write **policy_p,
                                         as_exp *exp_list, as_exp **exp_list_p)
{
    POLICY_INIT(as_policy_batch_write);

    // Set policy fields
    POLICY_SET_FIELD(key, as_policy_key);
    POLICY_SET_FIELD(commit_level, as_policy_commit_level);
    POLICY_SET_FIELD(gen, as_policy_gen);
    POLICY_SET_FIELD(exists, as_policy_exists);
    POLICY_SET_FIELD(durable_delete, bool);

    // C client 5.0 new expressions
    POLICY_SET_EXPRESSIONS_FIELD();

    // Update the policy
    POLICY_UPDATE();

    return err->code;
}

// New with server 6.0, C client 5.2.0 (batch writes)
as_status pyobject_to_batch_read_policy(AerospikeClient *self, as_error *err,
                                        PyObject *py_policy,
                                        as_policy_batch_read *policy,
                                        as_policy_batch_read **policy_p,
                                        as_exp *exp_list, as_exp **exp_list_p)
{
    POLICY_INIT(as_policy_batch_read);

    // Set policy fields
    POLICY_SET_FIELD(read_mode_ap, as_policy_read_mode_ap);
    POLICY_SET_FIELD(read_mode_sc, as_policy_read_mode_sc);
    POLICY_SET_FIELD(read_touch_ttl_percent, int);

    // C client 5.0 new expressions
    POLICY_SET_EXPRESSIONS_FIELD();

    // Update the policy
    POLICY_UPDATE();

    return err->code;
}

// New with server 6.0, C client 5.2.0 (batch writes)
as_status pyobject_to_batch_apply_policy(AerospikeClient *self, as_error *err,
                                         PyObject *py_policy,
                                         as_policy_batch_apply *policy,
                                         as_policy_batch_apply **policy_p,
                                         as_exp *exp_list, as_exp **exp_list_p)
{
    POLICY_INIT(as_policy_batch_apply);

    // Set policy fields
    POLICY_SET_FIELD(key, as_policy_key);
    POLICY_SET_FIELD(commit_level, as_policy_commit_level);
    POLICY_SET_FIELD(ttl, uint32_t);
    POLICY_SET_FIELD(durable_delete, bool);

    // C client 5.0 new expressions
    POLICY_SET_EXPRESSIONS_FIELD();

    // Update the policy
    POLICY_UPDATE();

    return err->code;
}

// New with server 6.0, C client 5.2.0 (batch writes)
as_status pyobject_to_batch_remove_policy(AerospikeClient *self, as_error *err,
                                          PyObject *py_policy,
                                          as_policy_batch_remove *policy,
                                          as_policy_batch_remove **policy_p,
                                          as_exp *exp_list, as_exp **exp_list_p)
{
    POLICY_INIT(as_policy_batch_remove);

    // Set policy fields
    POLICY_SET_FIELD(key, as_policy_key);
    POLICY_SET_FIELD(commit_level, as_policy_commit_level);
    POLICY_SET_FIELD(gen, as_policy_gen);
    POLICY_SET_FIELD(durable_delete, bool);
    POLICY_SET_FIELD(generation, uint16_t);

    // C client 5.0 new expressions
    POLICY_SET_EXPRESSIONS_FIELD();

    // Update the policy
    POLICY_UPDATE();

    return err->code;
}

as_status pyobject_to_bit_policy(as_error *err, PyObject *py_policy,
                                 as_bit_policy *policy)
{
    as_bit_policy_init(policy);
    POLICY_INIT(as_bit_policy);

    PyObject *py_bit_flags =
        PyDict_GetItemString(py_policy, BIT_WRITE_FLAGS_KEY);
    if (py_bit_flags) {
        if (PyLong_Check(py_bit_flags)) {
            as_bit_write_flags bit_write_flags =
                (as_bit_write_flags)PyLong_AsLong(py_bit_flags);
            as_bit_policy_set_write_flags(policy, bit_write_flags);
        }
    }
    else if (PyErr_Occurred()) {
        /* Fetching a map key failed internally for some reason, raise an error and exit.*/
        PyErr_Clear();
        return as_error_update(err, AEROSPIKE_ERR_CLIENT,
                               "Unable to get bit_write_flags");
    }

    return err->code;
}
as_status pyobject_to_map_policy(as_error *err, PyObject *py_policy,
                                 as_map_policy *policy)
{
    // Initialize Policy
    POLICY_INIT(as_map_policy);

    // Defaults
    long map_order = AS_MAP_UNORDERED;
    uint32_t map_write_flags = AS_MAP_WRITE_DEFAULT;
    bool persist_index = false;

    MAP_POLICY_SET_FIELD(map_order);
    MAP_POLICY_SET_FIELD(map_write_flags);

    PyObject *py_persist_index =
        PyDict_GetItemString(py_policy, "persist_index");
    if (py_persist_index) {
        if (PyBool_Check(py_persist_index)) {
            persist_index = (bool)PyObject_IsTrue(py_persist_index);
        }
        else {
            // persist_index value must be valid if it is set
            return as_error_update(err, AEROSPIKE_ERR_PARAM,
                                   "persist_index is not a boolean");
        }
    }

    as_map_policy_set_all(policy, map_order, map_write_flags, persist_index);

    return err->code;
}

as_status pyobject_to_list_policy(as_error *err, PyObject *py_policy,
                                  as_list_policy *list_policy)
{
    as_list_policy_init(list_policy);
    PyObject *py_val = NULL;
    long list_order = AS_LIST_UNORDERED;
    long flags = AS_LIST_WRITE_DEFAULT;

    if (!py_policy || py_policy == Py_None) {
        return AEROSPIKE_OK;
    }

    if (!PyDict_Check(py_policy)) {
        return as_error_update(err, AEROSPIKE_ERR_PARAM,
                               "List policy must be a dictionary.");
    }

    py_val = PyDict_GetItemString(py_policy, "list_order");
    if (py_val && py_val != Py_None) {
        if (PyLong_Check(py_val)) {
            list_order = (int64_t)PyLong_AsLong(py_val);
            if (PyErr_Occurred()) {
                return as_error_update(err, AEROSPIKE_ERR_PARAM,
                                       "Failed to convert list_order");
            }
        }
        else {
            return as_error_update(err, AEROSPIKE_ERR_PARAM,
                                   "Invalid List order");
        }
    }

    py_val = PyDict_GetItemString(py_policy, "write_flags");
    if (py_val && py_val != Py_None) {
        if (PyLong_Check(py_val)) {
            flags = (int64_t)PyLong_AsLong(py_val);
            if (PyErr_Occurred()) {
                return as_error_update(err, AEROSPIKE_ERR_PARAM,
                                       "Failed to convert write_flags");
            }
        }
        else {
            return as_error_update(err, AEROSPIKE_ERR_PARAM,
                                   "Invalid write_flags");
        }
    }

    as_list_policy_set(list_policy, (as_list_order)list_order,
                       (as_list_write_flags)flags);

    return AEROSPIKE_OK;
}

as_status pyobject_to_hll_policy(as_error *err, PyObject *py_policy,
                                 as_hll_policy *hll_policy)
{
    int64_t flags = 0;
    as_hll_policy_init(hll_policy);
    PyObject *py_val = NULL;

    if (!py_policy || py_policy == Py_None) {
        return AEROSPIKE_OK;
    }

    if (!PyDict_Check(py_policy)) {
        return as_error_update(err, AEROSPIKE_ERR_PARAM,
                               "Hll policy must be a dictionary.");
    }

    py_val = PyDict_GetItemString(py_policy, "flags");
    if (py_val && py_val != Py_None) {
        if (PyLong_Check(py_val)) {
            flags = (int64_t)PyLong_AsLong(py_val);
            if (PyErr_Occurred()) {
                return as_error_update(err, AEROSPIKE_ERR_PARAM,
                                       "Failed to convert flags.");
            }
        }
        else {
            return as_error_update(err, AEROSPIKE_ERR_PARAM,
                                   "Invalid hll policy flags.");
        }
    }

    as_hll_policy_set_write_flags(hll_policy, flags);

    return AEROSPIKE_OK;
}

enum {
    ENABLE_LISTENER_INDEX,
    DISABLE_LISTENER_INDEX,
    NODE_CLOSE_LISTENER_INDEX,
    SNAPSHOT_LISTENER_INDEX
};

// Call Python callback defined in udata at index "py_listener_data_index"
// If py_arg is NULL, pass no arguments to the Python callback
as_status call_py_callback(as_error *err, unsigned int py_listener_data_index,
                           void *udata, PyObject *py_arg)
{
    PyListenerData *py_listener_data = (PyListenerData *)udata;
    PyObject *py_args = NULL;
    if (py_arg) {
        py_args = PyTuple_New(1);
    }
    else {
        py_args = PyTuple_New(0);
    }
    if (!py_args) {
        return as_error_update(
            err, AEROSPIKE_ERR,
            "Unable to construct tuple of arguments for Python callback %s",
            py_listener_data[py_listener_data_index].listener_name);
    }

    if (py_arg) {
        int result = PyTuple_SetItem(py_args, 0, py_arg);
        if (result == -1) {
            PyErr_Clear();
            Py_DECREF(py_args);
            return as_error_update(
                err, AEROSPIKE_ERR,
                "Unable to set Python argument in tuple for Python callback %s",
                py_listener_data[py_listener_data_index].listener_name);
        }
    }

    PyObject *py_result = PyObject_Call(
        py_listener_data[py_listener_data_index].py_callback, py_args, NULL);
    Py_DECREF(py_args);
    if (!py_result) {
        // Python callback threw an exception, but just ignore it and set our own exception
        // When some C client listeners that return an error code, the C client only prints a warning
        // like the node close and snapshot listeners
        // We don't want Python to throw an exception in those cases
        // But to make debugging more helpful, we print the original Python exception in a C client's error message
        PyObject *py_exc_type, *py_exc_value, *py_traceback;
        PyErr_Fetch(&py_exc_type, &py_exc_value, &py_traceback);
        Py_XDECREF(py_traceback);

        const char *exc_type_str = ((PyTypeObject *)py_exc_type)->tp_name;
        Py_DECREF(py_exc_type);

        // Contains either the exception value or gives the reason it can't retrieve it
        char *err_msg_details = NULL;
        if (py_exc_value != NULL) {
            // Exception value can be anything, not necessarily just strings
            // e.g Aerospike exception values are tuples
            PyObject *py_exc_value_str = PyObject_Str(py_exc_value);
            Py_DECREF(py_exc_value);

            if (!py_exc_value_str) {
                err_msg_details =
                    strdup("str() on exception value threw an error");
            }
            else {
                const char *ERR_MSG_DETAILS_PREFIX = "Exception value: ";

                const char *exc_value_str = PyUnicode_AsUTF8(py_exc_value_str);
                size_t allocate_size =
                    strlen(ERR_MSG_DETAILS_PREFIX) + strlen(exc_value_str) + 1;
                err_msg_details = malloc(allocate_size);
                snprintf(err_msg_details, allocate_size, "%s%s",
                         ERR_MSG_DETAILS_PREFIX, exc_value_str);

                Py_DECREF(py_exc_value_str);
            }
        }
        else {
            err_msg_details = strdup("Exception value could not be retrieved");
        }

        as_error_update(err, AEROSPIKE_ERR,
                        "Python callback %s threw a %s exception. %s",
                        py_listener_data[py_listener_data_index].listener_name,
                        exc_type_str, err_msg_details);

        free(err_msg_details);

        return AEROSPIKE_ERR;
    }

    // We don't care about the return value of the callback. It should be None as defined in the API
    Py_DECREF(py_result);
    return AEROSPIKE_OK;
}

// This is called by
// client.enable_metrics() -> release GIL and call aerospike_enable_metrics() -> as_cluster_enable_metrics()
// We need to reacquire the GIL in this callback
as_status enable_listener_wrapper(as_error *err, void *py_listener_data)
{
    PyGILState_STATE state = PyGILState_Ensure();
    as_status status =
        call_py_callback(err, ENABLE_LISTENER_INDEX, py_listener_data, NULL);
    PyGILState_Release(state);
    return status;
}

const unsigned int num_listeners = 4;

void free_py_listener_data(PyListenerData *py_listener_data)
{
    for (unsigned int i = 0; i < num_listeners; i++) {
        Py_CLEAR(py_listener_data[i].py_callback);
    }
    free(py_listener_data);
}

// This can be called by
// client.close() -> releases GIL and calls aerospike_close() -> aerospike_disable_metrics()
// ... -> as_cluster_disable_metrics()
// We need to reacquire the GIL in this callback
as_status disable_listener_wrapper(as_error *err, struct as_cluster_s *cluster,
                                   void *py_listener_data)
{
    PyGILState_STATE state = PyGILState_Ensure();
    PyObject *py_cluster = create_py_cluster_from_as_cluster(err, cluster);
    if (!py_cluster) {
        return err->code;
    }
    as_status status = call_py_callback(err, DISABLE_LISTENER_INDEX,
                                        py_listener_data, py_cluster);

    // When this C callback is called, we are done using the current Python MetricsListeners callbacks
    // When re-enabling metrics, a new PyListenerData array will be heap allocated with new MetricsListeners callbacks
    free_py_listener_data((PyListenerData *)py_listener_data);

    PyGILState_Release(state);
    return status;
}

// This is called by
// as_cluster_tend() -> as_cluster_remove_nodes()
// This is called by the C client's tend thread, so we need to make sure the GIL is held
as_status node_close_listener_wrapper(as_error *err, struct as_node_s *node,
                                      void *py_listener_data)
{
    PyGILState_STATE state = PyGILState_Ensure();
    PyObject *py_node = create_py_node_from_as_node(err, node);
    if (!py_node) {
        return err->code;
    }
    as_status status = call_py_callback(err, NODE_CLOSE_LISTENER_INDEX,
                                        py_listener_data, py_node);
    PyGILState_Release(state);
    return status;
}

// This is called by
// as_cluster_tend() -> as_cluster_manage()
// This is called by the C client's tend thread, so we need to make sure the GIL is held
as_status snapshot_listener_wrapper(as_error *err, struct as_cluster_s *cluster,
                                    void *py_listener_data)
{
    PyGILState_STATE state = PyGILState_Ensure();
    PyObject *py_cluster = create_py_cluster_from_as_cluster(err, cluster);
    if (!py_cluster) {
        return err->code;
    }
    as_status status = call_py_callback(err, SNAPSHOT_LISTENER_INDEX,
                                        py_listener_data, py_cluster);
    PyGILState_Release(state);
    return status;
}

#define INVALID_ATTR_TYPE_ERROR_MSG "MetricsPolicy.%s must be a %s type"

// Define this conversion function here instead of conversions.c
// because it is only used to convert a PyObject to a C client metrics policy
// C client metrics policy "listeners" must already be declared (i.e in as_metrics_policy).
as_status
set_as_metrics_listeners_using_pyobject(as_error *err,
                                        PyObject *py_metricslisteners,
                                        as_metrics_listeners *listeners)
{
    if (!py_metricslisteners || py_metricslisteners == Py_None) {
        // Use default metrics writer callbacks that were set when initializing metrics policy
        return AEROSPIKE_OK;
    }

    if (!is_pyobj_correct_as_helpers_type(py_metricslisteners, "metrics",
                                          "MetricsListeners")) {
        as_error_update(err, AEROSPIKE_ERR_PARAM, INVALID_ATTR_TYPE_ERROR_MSG,
                        "metrics_listeners",
                        "aerospike_helpers.metrics.MetricsListeners");
        return AEROSPIKE_ERR_PARAM;
    }

    // When a MetricsListeners object is defined with callbacks
    // Pass those Python callbacks to C client "wrapper" callbacks using udata
    // Then in those wrapper callbacks, call those Python callbacks
    PyListenerData *py_listener_data =
        (PyListenerData *)malloc(sizeof(PyListenerData) * num_listeners);
    py_listener_data[ENABLE_LISTENER_INDEX] = (PyListenerData){
        "enable_listener",
        NULL,
    };
    py_listener_data[DISABLE_LISTENER_INDEX] = (PyListenerData){
        "disable_listener",
        NULL,
    };
    py_listener_data[NODE_CLOSE_LISTENER_INDEX] = (PyListenerData){
        "node_close_listener",
        NULL,
    };
    py_listener_data[SNAPSHOT_LISTENER_INDEX] = (PyListenerData){
        "snapshot_listener",
        NULL,
    };

    for (unsigned int i = 0; i < num_listeners; i++) {
        PyObject *py_listener = PyObject_GetAttrString(
            py_metricslisteners, py_listener_data[i].listener_name);
        if (!py_listener) {
            as_error_update(err, AEROSPIKE_ERR_PARAM,
                            "Unable to fetch %s attribute from "
                            "MetricsListeners instance",
                            py_listener_data[i].listener_name);
            goto error;
        }

        if (!PyCallable_Check(py_listener)) {
            as_error_update(
                err, AEROSPIKE_ERR_PARAM,
                "MetricsPolicy.metrics_listeners.%s must be a callable type",
                py_listener_data[i].listener_name);
            Py_DECREF(py_listener);
            goto error;
        }

        py_listener_data[i].py_callback = py_listener;
    }

    listeners->enable_listener = enable_listener_wrapper;
    listeners->disable_listener = disable_listener_wrapper;
    listeners->node_close_listener = node_close_listener_wrapper;
    listeners->snapshot_listener = snapshot_listener_wrapper;
    listeners->udata = py_listener_data;

    return AEROSPIKE_OK;
error:
    free_py_listener_data(py_listener_data);
    return AEROSPIKE_ERR_PARAM;
}

#define GET_ATTR_ERROR_MSG "Unable to fetch %s attribute"

// metrics_policy must be declared already
as_status
init_and_set_as_metrics_policy_using_pyobject(as_error *err,
                                              PyObject *py_metrics_policy,
                                              as_metrics_policy *metrics_policy)
{
    as_metrics_policy_init(metrics_policy);

    if (!py_metrics_policy || py_metrics_policy == Py_None) {
        // Use default metrics policy
        return AEROSPIKE_OK;
    }

    if (!is_pyobj_correct_as_helpers_type(py_metrics_policy, "metrics",
                                          "MetricsPolicy")) {
        return as_error_update(
            err, AEROSPIKE_ERR_PARAM,
            "policy parameter must be an aerospike_helpers.MetricsPolicy type");
    }

    PyObject *py_metrics_listeners =
        PyObject_GetAttrString(py_metrics_policy, "metrics_listeners");
    if (!py_metrics_listeners) {
        return as_error_update(err, AEROSPIKE_ERR_PARAM, GET_ATTR_ERROR_MSG,
                               "metrics_listeners");
    }

    as_status result = set_as_metrics_listeners_using_pyobject(
        err, py_metrics_listeners, &metrics_policy->metrics_listeners);
    Py_DECREF(py_metrics_listeners);
    if (result != AEROSPIKE_OK) {
        return result;
    }

    PyObject *py_report_dir =
        PyObject_GetAttrString(py_metrics_policy, "report_dir");
    if (!py_report_dir) {
        as_error_update(err, AEROSPIKE_ERR_PARAM, GET_ATTR_ERROR_MSG,
                        "report_dir");
        // Need to deallocate metrics listeners' udata
        goto error;
    }
    if (!PyUnicode_Check(py_report_dir)) {
        as_error_update(err, AEROSPIKE_ERR_PARAM, INVALID_ATTR_TYPE_ERROR_MSG,
                        "report_dir", "str");
        goto error;
    }
    const char *report_dir = PyUnicode_AsUTF8(py_report_dir);
    if (strlen(report_dir) >= sizeof(metrics_policy->report_dir)) {
        as_error_update(err, AEROSPIKE_ERR_PARAM,
                        "MetricsPolicy.report_dir must be less than 256 chars");
        goto error;
    }
    strcpy(metrics_policy->report_dir, report_dir);

    PyObject *py_report_size_limit =
        PyObject_GetAttrString(py_metrics_policy, "report_size_limit");
    if (!py_report_size_limit) {
        as_error_update(err, AEROSPIKE_ERR_PARAM, GET_ATTR_ERROR_MSG,
                        "report_size_limit");
        goto error;
    }
    if (!PyLong_CheckExact(py_report_size_limit)) {
        as_error_update(err, AEROSPIKE_ERR_PARAM, INVALID_ATTR_TYPE_ERROR_MSG,
                        "report_size_limit", "unsigned 64-bit integer");
        goto error;
    }
    unsigned long long report_size_limit =
        PyLong_AsUnsignedLongLong(py_report_size_limit);
    if (report_size_limit == (unsigned long long)-1 && PyErr_Occurred()) {
        PyErr_Clear();
        as_error_update(err, AEROSPIKE_ERR_PARAM, INVALID_ATTR_TYPE_ERROR_MSG,
                        "report_size_limit", "unsigned 64-bit integer");
        goto error;
    }
    if (report_size_limit > UINT64_MAX) {
        as_error_update(err, AEROSPIKE_ERR_PARAM, INVALID_ATTR_TYPE_ERROR_MSG,
                        "report_size_limit", "unsigned 64-bit integer");
        goto error;
    }

    metrics_policy->report_size_limit = (uint64_t)report_size_limit;

    const char *uint32_fields[] = {"interval", "latency_columns",
                                   "latency_shift"};
    uint32_t *uint32_ptrs[] = {&metrics_policy->interval,
                               &metrics_policy->latency_columns,
                               &metrics_policy->latency_shift};
    for (unsigned long i = 0;
         i < sizeof(uint32_fields) / sizeof(uint32_fields[0]); i++) {
        PyObject *py_field_value =
            PyObject_GetAttrString(py_metrics_policy, uint32_fields[i]);
        if (!py_field_value) {
            as_error_update(err, AEROSPIKE_ERR_PARAM, GET_ATTR_ERROR_MSG,
                            uint32_fields[i]);
            goto error;
        }

        // There's a helper function in the Python client wrapper code called
        // get_uint32_value
        // But we don't use it because it doesn't set which exact line
        // an error occurs. It only returns an error code when it happens
        if (!PyLong_CheckExact(py_field_value)) {
            as_error_update(err, AEROSPIKE_ERR_PARAM,
                            INVALID_ATTR_TYPE_ERROR_MSG, uint32_fields[i],
                            "unsigned 32-bit integer");
            Py_DECREF(py_field_value);
            goto error;
        }

        unsigned long field_value = PyLong_AsUnsignedLong(py_field_value);
        if (field_value == (unsigned long)-1 && PyErr_Occurred()) {
            PyErr_Clear();
            as_error_update(err, AEROSPIKE_ERR_PARAM,
                            INVALID_ATTR_TYPE_ERROR_MSG, uint32_fields[i],
                            "unsigned 32-bit integer");
            Py_DECREF(py_field_value);
            goto error;
        }

        if (field_value > UINT32_MAX) {
            as_error_update(err, AEROSPIKE_ERR_PARAM,
                            INVALID_ATTR_TYPE_ERROR_MSG, uint32_fields[i],
                            "unsigned 32-bit integer");
            Py_DECREF(py_field_value);
            goto error;
        }

        *uint32_ptrs[i] = (uint32_t)field_value;
    }

    return AEROSPIKE_OK;

error:
    // udata would've been allocated if MetricsListener was successfully converted to C code
    if (py_metrics_listeners && py_metrics_listeners != Py_None) {
        free_py_listener_data(
            (PyListenerData *)metrics_policy->metrics_listeners.udata);
    }
    return err->code;
}
