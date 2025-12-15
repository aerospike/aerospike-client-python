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

#pragma once

#include <Python.h>

#include <aerospike/as_error.h>
#include <aerospike/as_query.h>
#include <aerospike/as_exp.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_map_operations.h>
#include <aerospike/as_list_operations.h>
#include <aerospike/as_bit_operations.h>
#include <aerospike/as_hll_operations.h>
#include <aerospike/as_partition_filter.h>
#include <aerospike/as_metrics.h>

enum Aerospike_serializer_values {
    SERIALIZER_NONE, /* default handler for serializer type */
    SERIALIZER_PYTHON,
    SERIALIZER_JSON,
    SERIALIZER_USER,
};

enum Aerospike_send_bool_as_values {
    SEND_BOOL_AS_INTEGER,
    SEND_BOOL_AS_AS_BOOL, /* default for writing Python bools */
};

enum Aerospike_list_operations {
    OP_LIST_APPEND = 1001,
    OP_LIST_APPEND_ITEMS,
    OP_LIST_INSERT,
    OP_LIST_INSERT_ITEMS,
    OP_LIST_POP,
    OP_LIST_POP_RANGE,
    OP_LIST_REMOVE,
    OP_LIST_REMOVE_RANGE,
    OP_LIST_CLEAR,
    OP_LIST_SET,
    OP_LIST_GET,
    OP_LIST_GET_RANGE,
    OP_LIST_TRIM,
    OP_LIST_SIZE,
    OP_LIST_INCREMENT,
    OP_LIST_GET_BY_INDEX,
    OP_LIST_GET_BY_INDEX_RANGE,
    OP_LIST_GET_BY_RANK,
    OP_LIST_GET_BY_RANK_RANGE,
    OP_LIST_GET_BY_VALUE,
    OP_LIST_GET_BY_VALUE_LIST,
    OP_LIST_GET_BY_VALUE_RANGE,
    OP_LIST_REMOVE_BY_INDEX,
    OP_LIST_REMOVE_BY_INDEX_RANGE,
    OP_LIST_REMOVE_BY_RANK,
    OP_LIST_REMOVE_BY_RANK_RANGE,
    OP_LIST_REMOVE_BY_VALUE,
    OP_LIST_REMOVE_BY_VALUE_LIST,
    OP_LIST_REMOVE_BY_VALUE_RANGE,
    OP_LIST_SET_ORDER,
    OP_LIST_SORT,
    OP_LIST_REMOVE_BY_VALUE_RANK_RANGE_REL,
    OP_LIST_GET_BY_VALUE_RANK_RANGE_REL,

    // for use in expressions
    OP_LIST_GET_BY_VALUE_RANK_RANGE_REL_TO_END,
    OP_LIST_GET_BY_INDEX_RANGE_TO_END,
    OP_LIST_GET_BY_RANK_RANGE_TO_END,
    OP_LIST_REMOVE_BY_REL_RANK_RANGE_TO_END,
    OP_LIST_REMOVE_BY_REL_RANK_RANGE,
    OP_LIST_REMOVE_BY_INDEX_RANGE_TO_END,
    OP_LIST_REMOVE_BY_RANK_RANGE_TO_END,
    OP_LIST_CREATE
};

enum Aerospike_map_operations {
    OP_MAP_SET_POLICY = 1101,
    OP_MAP_PUT,
    OP_MAP_PUT_ITEMS,
    OP_MAP_INCREMENT,
    OP_MAP_DECREMENT,
    OP_MAP_SIZE,
    OP_MAP_CLEAR,
    OP_MAP_REMOVE_BY_KEY,
    OP_MAP_REMOVE_BY_KEY_LIST,
    OP_MAP_REMOVE_BY_KEY_RANGE,
    OP_MAP_REMOVE_BY_VALUE,
    OP_MAP_REMOVE_BY_VALUE_LIST,
    OP_MAP_REMOVE_BY_VALUE_RANGE,
    OP_MAP_REMOVE_BY_INDEX,
    OP_MAP_REMOVE_BY_INDEX_RANGE,
    OP_MAP_REMOVE_BY_RANK,
    OP_MAP_REMOVE_BY_RANK_RANGE,
    OP_MAP_GET_BY_KEY,
    OP_MAP_GET_BY_KEY_RANGE,
    OP_MAP_GET_BY_VALUE,
    OP_MAP_GET_BY_VALUE_RANGE,
    OP_MAP_GET_BY_INDEX,
    OP_MAP_GET_BY_INDEX_RANGE,
    OP_MAP_GET_BY_RANK,
    OP_MAP_GET_BY_RANK_RANGE,
    OP_MAP_GET_BY_VALUE_LIST,
    OP_MAP_GET_BY_KEY_LIST,
    OP_MAP_REMOVE_BY_VALUE_RANK_RANGE_REL,
    OP_MAP_REMOVE_BY_KEY_INDEX_RANGE_REL,
    OP_MAP_GET_BY_VALUE_RANK_RANGE_REL,
    OP_MAP_GET_BY_KEY_INDEX_RANGE_REL,
    OP_MAP_REMOVE_BY_KEY_REL_INDEX_RANGE_TO_END,
    OP_MAP_REMOVE_BY_VALUE_REL_RANK_RANGE_TO_END,
    OP_MAP_REMOVE_BY_INDEX_RANGE_TO_END,
    OP_MAP_REMOVE_BY_RANK_RANGE_TO_END,
    OP_MAP_GET_BY_KEY_REL_INDEX_RANGE_TO_END,
    OP_MAP_REMOVE_BY_KEY_REL_INDEX_RANGE,
    OP_MAP_REMOVE_BY_VALUE_REL_INDEX_RANGE,
    OP_MAP_REMOVE_BY_VALUE_REL_RANK_RANGE,
    OP_MAP_GET_BY_KEY_REL_INDEX_RANGE,
    OP_MAP_GET_BY_VALUE_RANK_RANGE_REL_TO_END,
    OP_MAP_GET_BY_INDEX_RANGE_TO_END,
    OP_MAP_GET_BY_RANK_RANGE_TO_END,
    OP_MAP_CREATE
};

enum aerospike_bitwise_operations {
    OP_BIT_RESIZE = 2000,
    OP_BIT_INSERT,
    OP_BIT_REMOVE,
    OP_BIT_SET,
    OP_BIT_OR,
    OP_BIT_XOR,
    OP_BIT_AND,
    OP_BIT_NOT,
    OP_BIT_LSHIFT,
    OP_BIT_RSHIFT,
    OP_BIT_ADD,
    OP_BIT_SUBTRACT,
    OP_BIT_GET_INT,
    OP_BIT_SET_INT,
    OP_BIT_GET,
    OP_BIT_COUNT,
    OP_BIT_LSCAN,
    OP_BIT_RSCAN
};

enum aerospike_hll_operations {
    OP_HLL_ADD = 2100,
    OP_HLL_DESCRIBE,
    OP_HLL_FOLD,
    OP_HLL_GET_COUNT,
    OP_HLL_GET_INTERSECT_COUNT,
    OP_HLL_GET_SIMILARITY,
    OP_HLL_GET_UNION,
    OP_HLL_GET_UNION_COUNT,
    OP_HLL_INIT,
    OP_HLL_REFRESH_COUNT,
    OP_HLL_SET_UNION,
    OP_HLL_MAY_CONTAIN
};

enum aerospike_expression_operations { OP_EXPR_READ = 2200, OP_EXPR_WRITE };

// Module constants to be used by aerospike_helpers

enum {
    _AS_EXP_LOOPVAR_FLOAT = 3000,
    _AS_EXP_LOOPVAR_INT,
    _AS_EXP_LOOPVAR_LIST,
    _AS_EXP_LOOPVAR_MAP,
    _AS_EXP_LOOPVAR_STR,
    _AS_EXP_LOOPVAR_BLOB,
    _AS_EXP_LOOPVAR_BOOL,
    // _AS_EXP_LOOPVAR_INF,
    _AS_EXP_LOOPVAR_NIL,
    _AS_EXP_LOOPVAR_GEOJSON,
    _AS_EXP_CODE_CALL_SELECT,
    _AS_EXP_CODE_CALL_APPLY,
};

// Can be either for select or apply
#define _CDT_FLAGS_KEY "cdt_flags"
#define _CDT_APPLY_MOD_EXP_KEY "mod_exp"
#define _CDT_CTX_FILTER_EXPR_KEY "filter_expr"

enum aerospike_regex_constants {
    REGEX_NONE = 0,
    REGEX_EXTENDED,
    REGEX_ICASE,
    REGEX_NOSUB = 4,
    REGEX_NEWLINE = 8,
};

enum aerospike_cdt_ctx_identifiers {
    CDT_CTX_LIST_INDEX_CREATE = 0x14,
    CDT_CTX_MAP_KEY_CREATE = 0x24
};

#define ERR_MSG_FAILED_TO_VALIDATE_POLICY_KEYS                                 \
    "Failed to validate keys for policy dictionary"

as_status pyobject_to_policy_admin(AerospikeClient *self, as_error *err,
                                   PyObject *py_policy, as_policy_admin *policy,
                                   as_policy_admin **policy_p,
                                   as_policy_admin *config_admin_policy);

as_status pyobject_to_policy_apply(AerospikeClient *self, as_error *err,
                                   PyObject *py_policy, as_policy_apply *policy,
                                   as_policy_apply **policy_p,
                                   as_policy_apply *config_apply_policy,
                                   as_exp *exp_list, as_exp **exp_list_p);

typedef enum {
    SECOND_AS_POLICY_WRITE,
    SECOND_AS_POLICY_SCAN,
    SECOND_AS_POLICY_NONE
} as_policy_with_extra_keys_allowed;

// as_policy_with_extra_keys_allowed only applies if validate_keys is true
as_status
pyobject_to_policy_info(as_error *err, PyObject *py_policy,
                        as_policy_info *policy, as_policy_info **policy_p,
                        as_policy_info *config_info_policy, bool validate_keys,
                        as_policy_with_extra_keys_allowed other_policy);

as_status pyobject_to_policy_query(AerospikeClient *self, as_error *err,
                                   PyObject *py_policy, as_policy_query *policy,
                                   as_policy_query **policy_p,
                                   as_policy_query *config_query_policy,
                                   as_exp *exp_list, as_exp **exp_list_p);

as_status pyobject_to_policy_read(AerospikeClient *self, as_error *err,
                                  PyObject *py_policy, as_policy_read *policy,
                                  as_policy_read **policy_p,
                                  as_policy_read *config_read_policy,
                                  as_exp *exp_list, as_exp **exp_list_p);

as_status pyobject_to_policy_remove(AerospikeClient *self, as_error *err,
                                    PyObject *py_policy,
                                    as_policy_remove *policy,
                                    as_policy_remove **policy_p,
                                    as_policy_remove *config_remove_policy,
                                    as_exp *exp_list, as_exp **exp_list_p);

// py_policy_also_supports_info_policy_fields only applies if self->validate_keys is true
as_status pyobject_to_policy_scan(
    AerospikeClient *self, as_error *err, PyObject *py_policy,
    as_policy_scan *policy, as_policy_scan **policy_p,
    as_policy_scan *config_scan_policy, as_exp *exp_list, as_exp **exp_list_p,
    bool py_policy_also_supports_info_policy_fields);

// py_policy_also_supports_info_policy_fields only applies if self->validate_keys is true
as_status pyobject_to_policy_write(
    AerospikeClient *self, as_error *err, PyObject *py_policy,
    as_policy_write *policy, as_policy_write **policy_p,
    as_policy_write *config_write_policy, as_exp *exp_list, as_exp **exp_list_p,
    bool py_policy_also_supports_info_policy_fields);

as_status pyobject_to_policy_operate(AerospikeClient *self, as_error *err,
                                     PyObject *py_policy,
                                     as_policy_operate *policy,
                                     as_policy_operate **policy_p,
                                     as_policy_operate *config_operate_policy,
                                     as_exp *exp_list, as_exp **exp_list_p);

as_status pyobject_to_policy_batch(AerospikeClient *self, as_error *err,
                                   PyObject *py_policy, as_policy_batch *policy,
                                   as_policy_batch **policy_p,
                                   as_policy_batch *config_batch_policy,
                                   as_exp *exp_list, as_exp **exp_list_p);

as_status pyobject_to_map_policy(as_error *err, PyObject *py_policy,
                                 as_map_policy *policy, bool validate_keys);

void set_scan_options(as_error *err, as_scan *scan_p, PyObject *py_options);

as_status set_query_options(as_error *err, PyObject *query_options,
                            as_query *query);

as_status pyobject_to_list_policy(as_error *err, PyObject *py_policy,
                                  as_list_policy *policy, bool validate_keys);

as_status pyobject_to_bit_policy(as_error *err, PyObject *py_policy,
                                 as_bit_policy *policy, bool validate_keys);

as_status pyobject_to_hll_policy(as_error *err, PyObject *py_policy,
                                 as_hll_policy *hll_policy, bool validate_keys);

as_status pyobject_to_batch_write_policy(AerospikeClient *self, as_error *err,
                                         PyObject *py_policy,
                                         as_policy_batch_write *policy,
                                         as_policy_batch_write **policy_p,
                                         as_exp *exp_list, as_exp **exp_list_p);

as_status pyobject_to_batch_read_policy(AerospikeClient *self, as_error *err,
                                        PyObject *py_policy,
                                        as_policy_batch_read *policy,
                                        as_policy_batch_read **policy_p,
                                        as_exp *exp_list, as_exp **exp_list_p);

as_status pyobject_to_batch_apply_policy(AerospikeClient *self, as_error *err,
                                         PyObject *py_policy,
                                         as_policy_batch_apply *policy,
                                         as_policy_batch_apply **policy_p,
                                         as_exp *exp_list, as_exp **exp_list_p);

as_status pyobject_to_batch_remove_policy(AerospikeClient *self, as_error *err,
                                          PyObject *py_policy,
                                          as_policy_batch_remove *policy,
                                          as_policy_batch_remove **policy_p,
                                          as_exp *exp_list,
                                          as_exp **exp_list_p);

// metrics_policy must be declared already
// py_metrics_policy must be non-NULL
// Returns non-zero integer value on error.
// On error, all memory from this function is freed
int set_as_metrics_policy_using_pyobject(as_error *err,
                                         PyObject *py_metrics_policy,
                                         as_metrics_policy *metrics_policy);

typedef struct {
    // Use listener name for error messages
    const char *listener_name;
    PyObject *py_callback;
} PyListenerData;

void free_py_listener_data(PyListenerData *py_listener_data);

#define POLICY_DICTIONARY_ADJECTIVE_FOR_ERROR_MESSAGE "policy"
