/*******************************************************************************
* Copyright 2013-2019 Aerospike, Inc.
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
#include <stdlib.h>
#include <string.h>
#include <aerospike/as_operations.h>
#include <aerospike/as_list_operations.h>
#include <aerospike/as_cdt_ctx.h>

#include "client.h"
#include "conversions.h"
#include "exceptions.h"
#include "policy.h"
#include "serializer.h"
#include "cdt_list_operations.h"
#include "cdt_operation_utils.h"

typedef struct {
    int operation_code;
    const char *operation_name;
} op_code_to_name;

const char *op_code_to_names[] = {
#define X(op_name) [OP_##op_name] = #op_name
    X(LIST_APPEND), LIST_OP_NAMES_EXCEPT_LIST_APPEND, STRING_OP_NAMES
#undef X
};

// String operation dictionary keys

#define STRING_OP_START_KEY "start"
#define NEEDLE_OP_START_KEY "needle"

as_status add_list_or_string_op(AerospikeClient *self, as_error *err,
                                PyObject *op_dict, as_vector *unicodeStrVector,
                                as_static_pool *static_pool, as_operations *ops,
                                long operation_code, long *ret_type,
                                int serializer_type)

{
    // as_operations_add_* API methods can take ownership of heap allocated as_val
    // objects even if the methods fail and return false.
    bool has_as_operations_taken_ownership_of_as_val_objs = false;
    char *bin = NULL;

    const char *bin_key = NULL;
    if (is_list_op(operation_code)) {
        bin_key = "bin";
    }
    else {
        bin_key = "bin_name";
    }

    if (get_str(err, bin_key, op_dict, unicodeStrVector, &bin, true) !=
        AEROSPIKE_OK) {
        goto exit;
    }

    // Specific to list operations

    as_list_policy list_policy;
    as_list_policy *list_policy_ref = NULL;

    switch (operation_code) {
    case OP_LIST_APPEND:
    case OP_LIST_APPEND_ITEMS:
    case OP_LIST_INSERT:
    case OP_LIST_INSERT_ITEMS:
    case OP_LIST_INCREMENT:
    case OP_LIST_SET: {
        bool policy_in_use = false;
        if (get_list_policy(err, op_dict, &list_policy, &policy_in_use,
                            self->validate_keys) != AEROSPIKE_OK) {
            goto exit;
        }
        list_policy_ref = policy_in_use ? &list_policy : NULL;
        break;
    }
    }

    int64_t count;
    bool range_specified = false;
    switch (operation_code) {
    case OP_LIST_POP_RANGE:
    case OP_LIST_REMOVE_RANGE:
    case OP_LIST_GET_RANGE:
    case OP_LIST_TRIM:
        if (get_int64_t(err, AS_PY_VAL_KEY, op_dict, &count) != AEROSPIKE_OK) {
            goto exit;
        }
        break;
    case OP_LIST_GET_BY_INDEX_RANGE:
    case OP_LIST_GET_BY_RANK_RANGE:
    case OP_LIST_REMOVE_BY_INDEX_RANGE:
    case OP_LIST_REMOVE_BY_RANK_RANGE:
    case OP_LIST_REMOVE_BY_VALUE_RANK_RANGE_REL:
    case OP_LIST_GET_BY_VALUE_RANK_RANGE_REL:
        if (get_optional_int64_t(err, AS_PY_COUNT_KEY, op_dict, &count,
                                 &range_specified) != AEROSPIKE_OK) {
            goto exit;
        }
        break;
    }

    int return_type = AS_LIST_RETURN_VALUE;
    if ((operation_code >= OP_LIST_GET_BY_INDEX &&
         operation_code <= OP_LIST_REMOVE_BY_VALUE_RANGE) ||
        (operation_code >= OP_LIST_REMOVE_BY_VALUE_RANK_RANGE_REL &&
         operation_code <= OP_LIST_GET_BY_VALUE_RANK_RANGE_REL)) {
        if (get_list_return_type(err, op_dict, &return_type) != AEROSPIKE_OK) {
            goto exit;
        }
    }

    int64_t index;
    switch (operation_code) {
    case OP_LIST_INSERT:
    case OP_LIST_INSERT_ITEMS:
    case OP_LIST_POP:
    case OP_LIST_POP_RANGE:
    case OP_LIST_REMOVE:
    case OP_LIST_REMOVE_RANGE:
    case OP_LIST_SET:
    case OP_LIST_GET:
    case OP_LIST_GET_RANGE:
    case OP_LIST_TRIM:
    case OP_LIST_INCREMENT:
    case OP_LIST_GET_BY_INDEX:
    case OP_LIST_GET_BY_INDEX_RANGE:
    case OP_LIST_REMOVE_BY_INDEX:
    case OP_LIST_REMOVE_BY_INDEX_RANGE:
    case OP_STRING_CHAR_AT:
    case OP_STRING_INSERT:
    case OP_STRING_OVERWRITE:
        if (get_int64_t(err, AS_PY_INDEX_KEY, op_dict, &index) !=
            AEROSPIKE_OK) {
            goto exit;
        }
    }

    int64_t rank;
    switch (operation_code) {
    case OP_LIST_GET_BY_RANK:
    case OP_LIST_GET_BY_RANK_RANGE:
    case OP_LIST_REMOVE_BY_RANK:
    case OP_LIST_REMOVE_BY_RANK_RANGE:
    case OP_LIST_GET_BY_VALUE_RANK_RANGE_REL:
    case OP_LIST_REMOVE_BY_VALUE_RANK_RANGE_REL:
        if (get_int64_t(err, AS_PY_RANK_KEY, op_dict, &rank) != AEROSPIKE_OK) {
            goto exit;
        }
        break;
    }

    int64_t order_type_int;
    switch (operation_code) {
    case OP_LIST_SET_ORDER:
    case OP_LIST_CREATE:
        if (get_int64_t(err, AS_PY_LIST_ORDER, op_dict, &order_type_int) !=
            AEROSPIKE_OK) {
            goto exit;
        }
    }

    bool ctx_in_use = false;
    as_cdt_ctx ctx;
    if (get_cdt_ctx(self, err, &ctx, op_dict, &ctx_in_use, static_pool,
                    serializer_type) != AEROSPIKE_OK) {
        goto exit;
    }
    as_cdt_ctx *ctx_ref = (ctx_in_use ? &ctx : NULL);

    as_val *val1 = NULL;
    switch (operation_code) {
    case OP_LIST_APPEND:
    case OP_LIST_INSERT:
    case OP_LIST_SET:
    case OP_LIST_INCREMENT:
    case OP_LIST_GET_BY_VALUE:
    case OP_LIST_REMOVE_BY_VALUE:
    case OP_LIST_REMOVE_BY_VALUE_RANK_RANGE_REL:
    case OP_LIST_GET_BY_VALUE_RANK_RANGE_REL:
        if (get_asval(self, err, AS_PY_VAL_KEY, op_dict, &val1, static_pool,
                      serializer_type, true) != AEROSPIKE_OK) {
            goto CLEANUP_CTX_ON_ERROR;
        }
        break;
    }

    const char *list_values_key = NULL;
    switch (operation_code) {
    case OP_LIST_GET_BY_VALUE_LIST:
    case OP_LIST_REMOVE_BY_VALUE_LIST:
    case OP_STRING_CONCAT_LIST:
        list_values_key = AS_PY_VALUES_KEY;
        break;
    case OP_LIST_APPEND_ITEMS:
    case OP_LIST_INSERT_ITEMS:
        list_values_key = AS_PY_VAL_KEY;
        break;
    }

    switch (operation_code) {
    case OP_LIST_GET_BY_VALUE_LIST:
    case OP_LIST_REMOVE_BY_VALUE_LIST:
    case OP_LIST_APPEND_ITEMS:
    case OP_LIST_INSERT_ITEMS:
    case OP_STRING_CONCAT_LIST:
        if (get_val_list(self, err, list_values_key, op_dict, (as_list **)&val1,
                         static_pool, serializer_type) != AEROSPIKE_OK) {
            goto CLEANUP_CTX_ON_ERROR;
        }
        break;
    }

    as_val *val2 = NULL;
    switch (operation_code) {
    case OP_LIST_GET_BY_VALUE_RANGE:
    case OP_LIST_REMOVE_BY_VALUE_RANGE:
        if (get_asval(self, err, AS_PY_VAL_BEGIN_KEY, op_dict, &val1,
                      static_pool, serializer_type, false) != AEROSPIKE_OK) {
            goto CLEANUP_CTX_ON_ERROR;
        }

        if (get_asval(self, err, AS_PY_VAL_END_KEY, op_dict, &val2, static_pool,
                      serializer_type, false) != AEROSPIKE_OK) {
            goto CLEANUP_VAL1_ON_ERROR;
        }
        break;
    }

    // Attributes only found in string operations

    int64_t start;
    switch (operation_code) {
    case OP_STRING_SUBSTR:
    case OP_STRING_SNIP:
        if (get_int64_t(err, STRING_OP_START_KEY, op_dict, &start) !=
            AEROSPIKE_OK) {
            goto CLEANUP_VAL2_ON_ERROR;
        }
    }

    uint64_t length = 0;
    bool length_found = false;
    switch (operation_code) {
    case OP_STRING_SUBSTR:
    case OP_STRING_PAD_START:
    case OP_STRING_PAD_END: {
        const char *length_key = NULL;
        switch (operation_code) {
        case OP_STRING_PAD_START:
        case OP_STRING_PAD_END:
            length_key = "target_length";
            break;
        default:
            length_key = "length";
            break;
        }

        as_status status = get_optional_uint64_t(err, length_key, op_dict,
                                                 &length, &length_found);
        if (status != AEROSPIKE_OK) {
            goto CLEANUP_VAL2_ON_ERROR;
        }

        if (!length_found && operation_code == OP_STRING_PAD_START) {
            as_error_update(err, AEROSPIKE_ERR_PARAM,
                            "length argument is required for pad_start");
            goto CLEANUP_VAL2_ON_ERROR;
        }
    }
    }

    int64_t end = 0;
    bool end_found = false;
    switch (operation_code) {
    case OP_STRING_SNIP:
        as_status status =
            get_optional_int64_t(err, "end", op_dict, &end, &end_found);
        if (status != AEROSPIKE_OK) {
            goto CLEANUP_VAL2_ON_ERROR;
        }
    }

    int64_t occurrence = 0;
    switch (operation_code) {
    case OP_STRING_FIND:
        if (get_int64_t(err, "occurrence", op_dict, &occurrence) !=
            AEROSPIKE_OK) {
            goto CLEANUP_VAL2_ON_ERROR;
        }
    }

    // Handle enum attributes

    as_string_numeric_type numeric_type = AS_STRING_NUMERIC_ANY;
    as_string_regex_flags regex_flags = AS_STRING_REGEX_FLAGS_NONE;
    int64_t tmp_value;
    switch (operation_code) {
    case OP_STRING_IS_NUMERIC: {
        if (get_int64_t(err, "numeric_type", op_dict, &tmp_value) !=
            AEROSPIKE_OK) {
            goto CLEANUP_VAL2_ON_ERROR;
        }
        numeric_type = (as_string_numeric_type)tmp_value;
        break;
    }
    case OP_STRING_REGEX_COMPARE: {
        if (get_int64_t(err, "regex_flags", op_dict, &tmp_value) !=
            AEROSPIKE_OK) {
            goto CLEANUP_VAL2_ON_ERROR;
        }
        regex_flags = (as_string_regex_flags)tmp_value;
        break;
    }
    }

    char *str_attr_value1 = NULL;
    const char *str_attr_key = NULL;
    bool is_str_attr_optional = false;
    switch (operation_code) {
    case OP_STRING_FIND:
    case OP_STRING_CONTAINS:
    case OP_STRING_STARTS_WITH:
    case OP_STRING_ENDS_WITH:
    case OP_STRING_SPLIT:
    case OP_STRING_REGEX_COMPARE:
    case OP_STRING_INSERT:
    case OP_STRING_OVERWRITE:
    case OP_STRING_CONCAT:
    case OP_STRING_REPLACE:
    case OP_STRING_REPLACE_ALL:
    case OP_STRING_PAD_START:
    case OP_STRING_PAD_END:
    case OP_STRING_REGEX_REPLACE:
        switch (operation_code) {
        case OP_STRING_FIND:
        case OP_STRING_CONTAINS:
        case OP_STRING_REPLACE:
        case OP_STRING_REPLACE_ALL:
        case OP_STRING_REGEX_REPLACE:
            str_attr_key = NEEDLE_OP_START_KEY;
            break;
        case OP_STRING_STARTS_WITH:
            str_attr_key = "prefix";
            break;
        case OP_STRING_ENDS_WITH:
            str_attr_key = "suffix";
            break;
        case OP_STRING_SPLIT:
            str_attr_key = "separator";
            is_str_attr_optional = true;
            break;
        case OP_STRING_REGEX_COMPARE:
            str_attr_key = "pattern";
            break;
        case OP_STRING_INSERT:
        case OP_STRING_OVERWRITE:
        case OP_STRING_CONCAT:
            str_attr_key = "value";
            break;
        case OP_STRING_PAD_START:
        case OP_STRING_PAD_END:
            str_attr_key = "pad_string";
            break;
        }

        // TODO: review what unicodeStrVector is for.
        if (get_str(err, str_attr_key, op_dict, unicodeStrVector,
                    &str_attr_value1, is_str_attr_optional) != AEROSPIKE_OK) {
            goto CLEANUP_VAL2_ON_ERROR;
        }
    }

    char *str_attr_value2 = NULL;
    switch (operation_code) {
    case OP_STRING_REPLACE:
    case OP_STRING_REPLACE_ALL:
    case OP_STRING_REGEX_REPLACE:
        if (get_str(err, "replacement", op_dict, unicodeStrVector,
                    &str_attr_value2, true) != AEROSPIKE_OK) {
            goto CLEANUP_VAL2_ON_ERROR;
        }
    }

    as_string_policy str_policy;
    switch (operation_code) {
    case OP_STRING_INSERT: {
        PyObject *py_str_policy = PyDict_GetItemString(op_dict, "policy");
        if (!py_str_policy) {
            goto CLEANUP_VAL2_ON_ERROR;
        }

        as_status status = as_string_policy_init_from_pyobject(err, &str_policy,
                                                               py_str_policy);
        if (status != AEROSPIKE_OK) {
            goto CLEANUP_VAL2_ON_ERROR;
        }
        break;
    }
    }

    bool success = false;
    switch (operation_code) {
    case OP_LIST_SIZE:
        success = as_operations_list_size(ops, bin, ctx_ref);
        break;
    case OP_LIST_POP:
        success = as_operations_list_pop(ops, bin, ctx_ref, index);
        break;
    case OP_LIST_POP_RANGE:
        success = as_operations_list_pop_range(ops, bin, ctx_ref, index,
                                               (uint64_t)count);
        break;
    case OP_LIST_REMOVE:
        success = as_operations_list_remove(ops, bin, ctx_ref, index);
        break;
    case OP_LIST_REMOVE_RANGE:
        success = as_operations_list_remove_range(ops, bin, ctx_ref, index,
                                                  (uint64_t)count);
        break;
    case OP_LIST_CLEAR:
        success = as_operations_list_clear(ops, bin, ctx_ref);
        break;
    case OP_LIST_SET:
        success = as_operations_list_set(ops, bin, ctx_ref, list_policy_ref,
                                         index, val1);
        break;
    case OP_LIST_GET:
        success = as_operations_list_get(ops, bin, ctx_ref, index);
        break;
    case OP_LIST_GET_RANGE:
        success = as_operations_list_get_range(ops, bin, ctx_ref, index,
                                               (uint64_t)count);
        break;
    case OP_LIST_TRIM:
        success =
            as_operations_list_trim(ops, bin, ctx_ref, index, (uint64_t)count);
        break;
    case OP_LIST_GET_BY_INDEX:
        success = as_operations_list_get_by_index(ops, bin, ctx_ref, index,
                                                  return_type);
        break;

    case OP_LIST_GET_BY_INDEX_RANGE:
        if (range_specified) {
            success = as_operations_list_get_by_index_range(
                ops, bin, ctx_ref, index, (uint64_t)count, return_type);
        }
        else {
            success = as_operations_list_get_by_index_range_to_end(
                ops, bin, ctx_ref, index, return_type);
        }
        break;
    case OP_LIST_GET_BY_RANK:
        success = as_operations_list_get_by_rank(ops, bin, ctx_ref, rank,
                                                 return_type);
        break;

    case OP_LIST_GET_BY_RANK_RANGE:
        if (range_specified) {
            success = as_operations_list_get_by_rank_range(
                ops, bin, ctx_ref, rank, (uint64_t)count, return_type);
        }
        else {
            success = as_operations_list_get_by_rank_range_to_end(
                ops, bin, ctx_ref, rank, return_type);
        }
        break;
    case OP_LIST_GET_BY_VALUE:
        success = as_operations_list_get_by_value(ops, bin, ctx_ref, val1,
                                                  return_type);
        break;
    case OP_LIST_GET_BY_VALUE_LIST:
        success = as_operations_list_get_by_value_list(
            ops, bin, ctx_ref, (as_list *)val1, return_type);
        break;
    case OP_LIST_GET_BY_VALUE_RANGE:
        success = as_operations_list_get_by_value_range(ops, bin, ctx_ref, val1,
                                                        val2, return_type);
        break;

    case OP_LIST_REMOVE_BY_INDEX:
        success = as_operations_list_remove_by_index(ops, bin, ctx_ref, index,
                                                     return_type);
        break;
    case OP_LIST_REMOVE_BY_INDEX_RANGE:
        if (range_specified) {
            success = as_operations_list_remove_by_index_range(
                ops, bin, ctx_ref, index, (uint64_t)count, return_type);
        }
        else {
            success = as_operations_list_remove_by_index_range_to_end(
                ops, bin, ctx_ref, index, return_type);
        }
        break;

    case OP_LIST_REMOVE_BY_RANK:
        success = as_operations_list_remove_by_rank(ops, bin, ctx_ref, rank,
                                                    return_type);
        break;

    case OP_LIST_REMOVE_BY_RANK_RANGE:
        if (range_specified) {
            success = as_operations_list_remove_by_rank_range(
                ops, bin, ctx_ref, rank, (uint64_t)count, return_type);
        }
        else {
            success = as_operations_list_remove_by_rank_range_to_end(
                ops, bin, ctx_ref, rank, return_type);
        }
        break;

    case OP_LIST_REMOVE_BY_VALUE:
        success = as_operations_list_remove_by_value(ops, bin, ctx_ref, val1,
                                                     return_type);
        break;
    case OP_LIST_REMOVE_BY_VALUE_LIST:
        success = as_operations_list_remove_by_value_list(
            ops, bin, ctx_ref, (as_list *)val1, return_type);
        break;
    case OP_LIST_REMOVE_BY_VALUE_RANGE:
        success = as_operations_list_remove_by_value_range(
            ops, bin, ctx_ref, val1, val2, return_type);
        break;
    case OP_LIST_SET_ORDER:
        success = as_operations_list_set_order(ops, bin, ctx_ref,
                                               (as_list_order)order_type_int);
        break;
    case OP_LIST_SORT: {
        int64_t sort_flags;

        if (get_int64_t(err, AS_PY_LIST_SORT_FLAGS, op_dict, &sort_flags) !=
            AEROSPIKE_OK) {
            goto CLEANUP_VAL2_ON_ERROR;
        }
        success = as_operations_list_sort(ops, bin, ctx_ref,
                                          (as_list_sort_flags)sort_flags);
        break;
    }
    case OP_LIST_GET_BY_VALUE_RANK_RANGE_REL:
        if (range_specified) {
            success = as_operations_list_get_by_value_rel_rank_range(
                ops, bin, ctx_ref, val1, rank, (uint64_t)count, return_type);
        }
        else {
            success = as_operations_list_get_by_value_rel_rank_range_to_end(
                ops, bin, ctx_ref, val1, rank, return_type);
        }
        break;
    case OP_LIST_CREATE: {
        bool pad, persist_index;
        if (get_bool_from_pyargs(err, AS_PY_PAD, op_dict, &pad) !=
            AEROSPIKE_OK) {
            goto CLEANUP_VAL2_ON_ERROR;
        }

        if (get_bool_from_pyargs(err, AS_PY_PERSIST_INDEX, op_dict,
                                 &persist_index) != AEROSPIKE_OK) {
            goto CLEANUP_VAL2_ON_ERROR;
        }

        success = as_operations_list_create_all(ops, bin, ctx_ref,
                                                (as_list_order)order_type_int,
                                                pad, persist_index);
        break;
    }
    case OP_LIST_APPEND:
        success =
            as_operations_list_append(ops, bin, ctx_ref, list_policy_ref, val1);
        break;
    case OP_LIST_APPEND_ITEMS:
        success = as_operations_list_append_items(
            ops, bin, ctx_ref, list_policy_ref, (as_list *)val1);
        break;
    case OP_LIST_INSERT:
        success = as_operations_list_insert(ops, bin, ctx_ref, list_policy_ref,
                                            index, val1);
        break;
    case OP_LIST_INSERT_ITEMS:
        success = as_operations_list_insert_items(
            ops, bin, ctx_ref, list_policy_ref, index, (as_list *)val1);
        break;
    case OP_LIST_INCREMENT:
        success = as_operations_list_increment(ops, bin, ctx_ref,
                                               list_policy_ref, index, val1);
        break;
    case OP_LIST_REMOVE_BY_VALUE_RANK_RANGE_REL:
        if (range_specified) {
            success = as_operations_list_remove_by_value_rel_rank_range(
                ops, bin, ctx_ref, val1, rank, (uint64_t)count, return_type);
        }
        else {
            success = as_operations_list_remove_by_value_rel_rank_range_to_end(
                ops, bin, ctx_ref, val1, rank, return_type);
        }
        break;
    case OP_STRING_STRLEN:
        success = as_operations_string_strlen(ops, bin, ctx_ref);
        break;
    case OP_STRING_SUBSTR:
        if (!length_found) {
            success = as_operations_string_substr(ops, bin, ctx_ref, start);
        }
        else {
            success = as_operations_string_substr_range(ops, bin, ctx_ref,
                                                        start, length);
        }
        break;
    case OP_STRING_CHAR_AT:
        success = as_operations_string_char_at(ops, bin, ctx_ref, index);
        break;
    case OP_STRING_FIND:
        success = as_operations_string_find_occurrence(
            ops, bin, ctx_ref, str_attr_value1, occurrence);
        break;
    case OP_STRING_CONTAINS:
        success =
            as_operations_string_contains(ops, bin, ctx_ref, str_attr_value1);
        break;
    case OP_STRING_STARTS_WITH:
        success = as_operations_string_starts_with(ops, bin, ctx_ref,
                                                   str_attr_value1);
        break;
    case OP_STRING_ENDS_WITH:
        success =
            as_operations_string_ends_with(ops, bin, ctx_ref, str_attr_value1);
        break;
    case OP_STRING_TO_INTEGER:
        success = as_operations_string_to_integer(ops, bin, ctx_ref);
        break;
    case OP_STRING_TO_DOUBLE:
        success = as_operations_string_to_double(ops, bin, ctx_ref);
        break;
    case OP_STRING_BYTE_LENGTH:
        success = as_operations_string_byte_length(ops, bin, ctx_ref);
        break;
    case OP_STRING_IS_NUMERIC:
        success = as_operations_string_is_numeric_type(ops, bin, ctx_ref,
                                                       numeric_type);
        break;
    case OP_STRING_IS_UPPER:
        success = as_operations_string_is_upper(ops, bin, ctx_ref);
        break;
    case OP_STRING_IS_LOWER:
        success = as_operations_string_is_lower(ops, bin, ctx_ref);
        break;
    case OP_STRING_TO_BLOB:
        success = as_operations_string_to_blob(ops, bin, ctx_ref);
        break;
    case OP_STRING_SPLIT:
        if (str_attr_value1) {
            success = as_operations_string_split_separator(ops, bin, ctx_ref,
                                                           str_attr_value1);
        }
        else {
            success = as_operations_string_split(ops, bin, ctx_ref);
        }
        break;
    case OP_STRING_B64_DECODE:
        success = as_operations_string_b64_decode(ops, bin, ctx_ref);
        break;
    case OP_STRING_REGEX_COMPARE:
        success = as_operations_string_regex_compare_flags(
            ops, bin, ctx_ref, str_attr_value1, regex_flags);
        break;
    case OP_STRING_INSERT:
        success = as_operations_string_insert(ops, bin, ctx_ref, &str_policy,
                                              index, str_attr_value1);
        break;
    case OP_STRING_OVERWRITE:
        success = as_operations_string_overwrite(ops, bin, ctx_ref, &str_policy,
                                                 index, str_attr_value1);
        break;
    case OP_STRING_CONCAT:
        success = as_operations_string_concat(ops, bin, ctx_ref, &str_policy,
                                              str_attr_value1);
        break;
    case OP_STRING_CONCAT_LIST:
        // TODO: test negative test case where a non-str value is in as_list
        success = as_operations_string_concat_list(
            ops, bin, ctx_ref, &str_policy, (as_list *)val1);
        break;
    case OP_STRING_SNIP:
        if (end_found) {
            success = as_operations_string_snip_range(ops, bin, ctx_ref,
                                                      &str_policy, start, end);
        }
        else {
            success = as_operations_string_snip(ops, bin, ctx_ref, &str_policy,
                                                start);
        }
        break;
    case OP_STRING_REPLACE:
        success = as_operations_string_replace(
            ops, bin, ctx_ref, &str_policy, str_attr_value1, str_attr_value2);
        break;
    case OP_STRING_REPLACE_ALL:
        success = as_operations_string_replace_all(
            ops, bin, ctx_ref, &str_policy, str_attr_value1, str_attr_value2);
        break;
    // TODO: thinking of making an array mapping op codes to op methods with the same params
    case OP_STRING_UPPER:
        success = as_operations_string_upper(ops, bin, ctx_ref, &str_policy);
        break;
    case OP_STRING_LOWER:
        success = as_operations_string_lower(ops, bin, ctx_ref, &str_policy);
        break;
    case OP_STRING_CASE_FOLD:
        success =
            as_operations_string_case_fold(ops, bin, ctx_ref, &str_policy);
        break;
    case OP_STRING_NORMALIZE_NFC:
        success =
            as_operations_string_normalize_nfc(ops, bin, ctx_ref, &str_policy);
        break;
    case OP_STRING_TRIM_START:
        success =
            as_operations_string_trim_start(ops, bin, ctx_ref, &str_policy);
        break;
    case OP_STRING_TRIM_END:
        success = as_operations_string_trim_end(ops, bin, ctx_ref, &str_policy);
        break;
    case OP_STRING_TRIM:
        success = as_operations_string_trim(ops, bin, ctx_ref, &str_policy);
        break;
    case OP_STRING_PAD_START:
        success = as_operations_string_pad_start(ops, bin, ctx_ref, &str_policy,
                                                 length, str_attr_value1);
        break;
    case OP_STRING_PAD_END:
        success = as_operations_string_pad_end(ops, bin, ctx_ref, &str_policy,
                                               length, str_attr_value1);
        break;
    case OP_STRING_REPEAT:
        success = as_operations_string_repeat(ops, bin, ctx_ref, &str_policy,
                                              (uint64_t)count);
        break;
    case OP_STRING_REGEX_REPLACE:
        success = as_operations_string_replace(
            ops, bin, ctx_ref, &str_policy, str_attr_value1, str_attr_value2);
        break;
    default:
        // This should never be possible since we only get here if we know that the operation is valid.
        as_error_update(err, AEROSPIKE_ERR_PARAM, "Unknown operation");
        goto CLEANUP_VAL2_ON_ERROR;
    }

    has_as_operations_taken_ownership_of_as_val_objs = true;

    if (!success) {
        as_error_update(err, AEROSPIKE_ERR_CLIENT, "Failed to add %s operation",
                        op_code_to_names[operation_code]);
    }

    if (has_as_operations_taken_ownership_of_as_val_objs == false) {
    CLEANUP_VAL2_ON_ERROR:
        if (val2) {
            as_val_destroy(val2);
        }

    CLEANUP_VAL1_ON_ERROR:
        if (val1) {
            as_val_destroy(val1);
        }
    }

CLEANUP_CTX_ON_ERROR:
    if (ctx_ref) {
        as_cdt_ctx_destroy(ctx_ref);
    }

exit:
    return err->code;
}
