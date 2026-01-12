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

as_status add_new_list_op(AerospikeClient *self, as_error *err,
                          PyObject *op_dict, as_vector *unicodeStrVector,
                          as_static_pool *static_pool, as_operations *ops,
                          long operation_code, long *ret_type,
                          int serializer_type)

{
    char *bin = NULL;

    if (get_bin(err, op_dict, unicodeStrVector, &bin) != AEROSPIKE_OK) {
        goto exit;
    }

    as_list_policy list_policy;
    as_list_policy *list_policy_ref = NULL;
    bool policy_in_use = false;

    switch (operation_code) {
    case OP_LIST_APPEND:
    case OP_LIST_APPEND_ITEMS:
    case OP_LIST_INSERT:
    case OP_LIST_INSERT_ITEMS:
    case OP_LIST_INCREMENT:
    case OP_LIST_SET:
        if (get_list_policy(err, op_dict, &list_policy, &policy_in_use,
                            self->validate_keys) != AEROSPIKE_OK) {
            goto exit;
        }
        list_policy_ref = policy_in_use ? &list_policy : NULL;
        break;
    }

    as_val *val = NULL;
    switch (operation_code) {
    case OP_LIST_APPEND:
    case OP_LIST_INSERT:
    case OP_LIST_POP_RANGE:
    case OP_LIST_SET:
    case OP_LIST_INCREMENT:
    case OP_LIST_GET_BY_VALUE:
    case OP_LIST_REMOVE_BY_VALUE:
    case OP_LIST_REMOVE_BY_VALUE_RANK_RANGE_REL:
    case OP_LIST_GET_BY_VALUE_RANK_RANGE_REL:
    case OP_LIST_APPEND_ITEMS:
        if (get_asval(self, err, AS_PY_VAL_KEY, op_dict, &val, static_pool,
                      serializer_type, true) != AEROSPIKE_OK) {
            goto exit;
        }
        break;
    }

    int64_t count;
    bool range_specified = false;
    switch (operation_code) {
    case OP_LIST_POP_RANGE:
    case OP_LIST_REMOVE_RANGE:
    case OP_LIST_GET_RANGE:
    case OP_LIST_TRIM:
        if (get_int64_t(err, AS_PY_VAL_KEY, op_dict, &count) != AEROSPIKE_OK) {
            goto CLEANUP_ON_ERROR1;
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
            goto CLEANUP_ON_ERROR1;
        }
        break;
    }

    int return_type = AS_LIST_RETURN_VALUE;
    if ((operation_code >= OP_LIST_GET_BY_INDEX &&
         operation_code <= OP_LIST_REMOVE_BY_VALUE_RANGE) ||
        (operation_code >= OP_LIST_REMOVE_BY_VALUE_RANK_RANGE_REL &&
         operation_code <= OP_LIST_GET_BY_VALUE_RANK_RANGE_REL)) {
        if (get_list_return_type(err, op_dict, &return_type) != AEROSPIKE_OK) {
            goto CLEANUP_ON_ERROR1;
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
        if (get_int64_t(err, AS_PY_INDEX_KEY, op_dict, &index) !=
            AEROSPIKE_OK) {
            goto CLEANUP_ON_ERROR1;
        }
    }

    bool ctx_in_use = false;
    as_cdt_ctx ctx;
    if (get_cdt_ctx(self, err, &ctx, op_dict, &ctx_in_use, static_pool,
                    serializer_type) != AEROSPIKE_OK) {
        goto CLEANUP_ON_ERROR1;
    }
    as_cdt_ctx *ctx_ref = (ctx_in_use ? &ctx : NULL);

    int64_t rank;
    switch (operation_code) {
    case OP_LIST_GET_BY_RANK:
    case OP_LIST_GET_BY_RANK_RANGE:
    case OP_LIST_REMOVE_BY_RANK:
    case OP_LIST_REMOVE_BY_RANK_RANGE:
    case OP_LIST_GET_BY_VALUE_RANK_RANGE_REL:
    case OP_LIST_REMOVE_BY_VALUE_RANK_RANGE_REL:
        if (get_int64_t(err, AS_PY_RANK_KEY, op_dict, &rank) != AEROSPIKE_OK) {
            goto CLEANUP_ON_ERROR2;
        }
        break;
    }

    const char *list_values_key = NULL;
    switch (operation_code) {
    case OP_LIST_GET_BY_VALUE_LIST:
    case OP_LIST_REMOVE_BY_VALUE_LIST:
        list_values_key = AS_PY_VALUES_KEY;
        break;
    case OP_LIST_APPEND_ITEMS:
    case OP_LIST_INSERT_ITEMS:
        list_values_key = AS_PY_VAL_KEY;
        break;
    }

    as_list *value_list = NULL;
    switch (operation_code) {
    case OP_LIST_GET_BY_VALUE_LIST:
    case OP_LIST_REMOVE_BY_VALUE_LIST:
    case OP_LIST_APPEND_ITEMS:
    case OP_LIST_INSERT_ITEMS:
        if (get_val_list(self, err, list_values_key, op_dict, &value_list,
                         static_pool, serializer_type) != AEROSPIKE_OK) {
            goto CLEANUP_ON_ERROR2;
        }
        break;
    }

    as_val *val_begin = NULL;
    as_val *val_end = NULL;
    switch (operation_code) {
    case OP_LIST_GET_BY_VALUE_RANGE:
    case OP_LIST_REMOVE_BY_VALUE_RANGE:
        if (get_asval(self, err, AS_PY_VAL_BEGIN_KEY, op_dict, &val_begin,
                      static_pool, serializer_type, false) != AEROSPIKE_OK) {
            goto CLEANUP_ON_ERROR3;
        }

        if (get_asval(self, err, AS_PY_VAL_END_KEY, op_dict, &val_end,
                      static_pool, serializer_type, false) != AEROSPIKE_OK) {
            goto CLEANUP_ON_ERROR4;
        }
        break;
    }

    int64_t order_type_int;
    switch (operation_code) {
    case OP_LIST_SET_ORDER:
    case OP_LIST_CREATE:
        if (get_int64_t(err, AS_PY_LIST_ORDER, op_dict, &order_type_int) !=
            AEROSPIKE_OK) {
            goto CLEANUP_ON_ERROR5;
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
                                         index, val);
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
    case OP_LIST_GET_BY_INDEX: {
        success = as_operations_list_get_by_index(ops, bin, ctx_ref, index,
                                                  return_type);
    } break;

    case OP_LIST_GET_BY_INDEX_RANGE: {
        if (range_specified) {
            success = as_operations_list_get_by_index_range(
                ops, bin, ctx_ref, index, (uint64_t)count, return_type);
        }
        else {
            success = as_operations_list_get_by_index_range_to_end(
                ops, bin, ctx_ref, index, return_type);
        }
    } break;
    case OP_LIST_GET_BY_RANK: {
        success = as_operations_list_get_by_rank(ops, bin, ctx_ref, rank,
                                                 return_type);
    } break;

    case OP_LIST_GET_BY_RANK_RANGE: {
        if (range_specified) {
            success = as_operations_list_get_by_rank_range(
                ops, bin, ctx_ref, rank, (uint64_t)count, return_type);
        }
        else {
            success = as_operations_list_get_by_rank_range_to_end(
                ops, bin, ctx_ref, rank, return_type);
        }
    } break;
    case OP_LIST_GET_BY_VALUE: {
        success = as_operations_list_get_by_value(ops, bin, ctx_ref, val,
                                                  return_type);
    } break;
    case OP_LIST_GET_BY_VALUE_LIST: {
        success = as_operations_list_get_by_value_list(ops, bin, ctx_ref,
                                                       value_list, return_type);
    } break;

    case OP_LIST_GET_BY_VALUE_RANGE: {
        success = as_operations_list_get_by_value_range(
            ops, bin, ctx_ref, val_begin, val_end, return_type);
    } break;

    case OP_LIST_REMOVE_BY_INDEX: {
        success = as_operations_list_remove_by_index(ops, bin, ctx_ref, index,
                                                     return_type);
        break;
    }

    case OP_LIST_REMOVE_BY_INDEX_RANGE: {
        if (range_specified) {
            success = as_operations_list_remove_by_index_range(
                ops, bin, ctx_ref, index, (uint64_t)count, return_type);
        }
        else {
            success = as_operations_list_remove_by_index_range_to_end(
                ops, bin, ctx_ref, index, return_type);
        }
    } break;

    case OP_LIST_REMOVE_BY_RANK: {
        success = as_operations_list_remove_by_rank(ops, bin, ctx_ref, rank,
                                                    return_type);
        break;
    }

    case OP_LIST_REMOVE_BY_RANK_RANGE: {
        if (range_specified) {
            success = as_operations_list_remove_by_rank_range(
                ops, bin, ctx_ref, rank, (uint64_t)count, return_type);
        }
        else {
            success = as_operations_list_remove_by_rank_range_to_end(
                ops, bin, ctx_ref, rank, return_type);
        }
    } break;

    case OP_LIST_REMOVE_BY_VALUE: {
        success = as_operations_list_remove_by_value(ops, bin, ctx_ref, val,
                                                     return_type);
        break;
    }

    case OP_LIST_REMOVE_BY_VALUE_LIST: {
        success = as_operations_list_remove_by_value_list(
            ops, bin, ctx_ref, value_list, return_type);
    } break;

    case OP_LIST_REMOVE_BY_VALUE_RANGE: {
        success = as_operations_list_remove_by_value_range(
            ops, bin, ctx_ref, val_begin, val_end, return_type);
    } break;

    case OP_LIST_SET_ORDER: {
        success = as_operations_list_set_order(ops, bin, ctx_ref,
                                               (as_list_order)order_type_int);
        break;
    }

    case OP_LIST_SORT: {
        int64_t sort_flags;

        if (get_int64_t(err, AS_PY_LIST_SORT_FLAGS, op_dict, &sort_flags) !=
            AEROSPIKE_OK) {
            goto CLEANUP_ON_ERROR5;
        }
        success = as_operations_list_sort(ops, bin, ctx_ref,
                                          (as_list_sort_flags)sort_flags);
        break;
    }

    case OP_LIST_GET_BY_VALUE_RANK_RANGE_REL: {
        success = as_operations_list_get_by_value_rel_rank_range(
            ops, bin, ctx_ref, val, rank, (uint64_t)count, return_type);
        break;
    }

    case OP_LIST_CREATE: {
        bool pad, persist_index;
        if (get_bool_from_pyargs(err, AS_PY_PAD, op_dict, &pad) !=
            AEROSPIKE_OK) {
            goto CLEANUP_ON_ERROR5;
        }

        if (get_bool_from_pyargs(err, AS_PY_PERSIST_INDEX, op_dict,
                                 &persist_index) != AEROSPIKE_OK) {
            goto CLEANUP_ON_ERROR5;
        }

        success = as_operations_list_create_all(ops, bin, ctx_ref,
                                                (as_list_order)order_type_int,
                                                pad, persist_index);
        break;
    }
    case OP_LIST_APPEND:
        success =
            as_operations_list_append(ops, bin, ctx_ref, list_policy_ref, val);
        break;
    case OP_LIST_APPEND_ITEMS:
        success = as_operations_list_append_items(ops, bin, ctx_ref,
                                                  list_policy_ref, value_list);
        break;
    case OP_LIST_INSERT:
        success = as_operations_list_insert(ops, bin, ctx_ref, list_policy_ref,
                                            index, val);
        break;
    case OP_LIST_INSERT_ITEMS:
        success = as_operations_list_insert_items(
            ops, bin, ctx_ref, list_policy_ref, index, value_list);
        break;
    case OP_LIST_INCREMENT:
        success = as_operations_list_increment(ops, bin, ctx_ref,
                                               list_policy_ref, index, val);
        break;
    case OP_LIST_REMOVE_BY_VALUE_RANK_RANGE_REL:
        if (range_specified) {
            success = as_operations_list_remove_by_value_rel_rank_range(
                ops, bin, ctx_ref, val, rank, (uint64_t)count, return_type);
        }
        else {
            success = as_operations_list_remove_by_value_rel_rank_range_to_end(
                ops, bin, ctx_ref, val, rank, return_type);
        }
        break;
    default:
        // This should never be possible since we only get here if we know that the operation is valid.
        as_error_update(err, AEROSPIKE_ERR_PARAM, "Unknown operation");
        goto CLEANUP_ON_ERROR5;
    }

    if (!success) {
        // TODO: regression in error message
        as_error_update(err, AEROSPIKE_ERR_CLIENT, "Failed to add operation");
    }

CLEANUP_ON_ERROR5:
    if (val_end) {
        as_val_destroy(val_end);
    }
CLEANUP_ON_ERROR4:
    if (val_begin) {
        as_val_destroy(val_begin);
    }
CLEANUP_ON_ERROR3:
    if (value_list) {
        as_list_destroy(value_list);
    }
CLEANUP_ON_ERROR2:
    if (ctx_ref) {
        as_cdt_ctx_destroy(ctx_ref);
    }
CLEANUP_ON_ERROR1:
    if (val) {
        as_val_destroy(val);
    }
exit:
    return err->code;
}
