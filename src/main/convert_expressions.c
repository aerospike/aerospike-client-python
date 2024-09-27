/*******************************************************************************
 * Copyright 2013-2020 Aerospike, Inc.
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

#include <aerospike/aerospike_index.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/as_error.h>
#include <aerospike/as_exp.h>
#include <aerospike/as_vector.h>
#include <aerospike/as_geojson.h>
#include <aerospike/as_msgpack_ext.h>

#include "client.h"
#include "conversions.h"
#include "serializer.h"
#include "exceptions.h"
#include "policy.h"
#include "cdt_operation_utils.h"
#include "geo.h"
#include "cdt_types.h"
#include "key_ordered_dict.h"

// EXPR OPS
enum expr_ops {
    UNKNOWN = 0,
    EQ = 1,
    NE = 2,
    GT = 3,
    GE = 4,
    LT = 5,
    LE = 6,
    CMP_REGEX = 7,
    CMP_GEO = 8,

    AND = 16,
    OR = 17,
    NOT = 18,
    EXCLUSIVE = 19,

    ADD = 20,
    SUB = 21,
    MUL = 22,
    DIV = 23,
    POW = 24,
    LOG = 25,
    MOD = 26,
    ABS = 27,
    FLOOR = 28,
    CEIL = 29,

    TO_INT = 30,
    TO_FLOAT = 31,

    INT_AND = 32,
    INT_OR = 33,
    INT_XOR = 34,
    INT_NOT = 35,
    INT_LSHIFT = 36,
    INT_RSHIFT = 37,
    INT_ARSHIFT = 38,
    INT_COUNT = 39,
    INT_LSCAN = 40,
    INT_RSCAN = 41,

    MIN = 50,
    MAX = 51,

    META_DIGEST_MOD = 64,
    META_DEVICE_SIZE = 65,
    META_LAST_UPDATE_TIME = 66,
    META_VOID_TIME = 67,
    META_TTL = 68,
    META_SET_NAME = 69,
    META_KEY_EXISTS = 70,
    META_SINCE_UPDATE_TIME = 71,
    META_IS_TOMBSTONE = 72,
    META_MEMORY_SIZE = 73,
    META_RECORD_SIZE = 74,

    REC_KEY = 80,
    BIN = 81,
    BIN_TYPE = 82,
    BIN_EXISTS = 83,

    COND = 123,
    VAR = 124,
    LET = 125,
    DEF = 126,

    CALL = 127,
    LIST_MOD = 139,
    VAL = 200
};

// VIRTUAL OPS
enum virtual_ops {
    END_VA_ARGS = 150,
    _TRUE = 151,
    _FALSE = 152,
    _AS_EXP_BIT_FLAGS = 153,
};

// UTILITY CONSTANTS
enum utiity_constants {
    NO_BIT_FLAGS = 0,
    VAL_STRING_P_ACTIVE = 1,
    VAL_LIST_P_ACTIVE = 2,
    VAL_MAP_P_ACTIVE = 3,
};

// FIXED DICTIONARY KEYS
#define LIST_ORDER_KEY "list_order"
#define REGEX_OPTIONS_KEY "regex_options"

// UTILITY MACROS
#define EXP_SZ(_expr) sizeof((as_exp_entry[]){_expr})

#define APPEND_ARRAY(_sub_values, ...)                                         \
    {                                                                          \
        {                                                                      \
            as_exp_entry tmp_expr_array[] = {__VA_ARGS__};                     \
            int expr_array_size =                                              \
                sizeof(tmp_expr_array) / sizeof(as_exp_entry) - _sub_values;   \
            memcpy(&((*expressions)[(*bottom)]), &tmp_expr_array,              \
                   (expr_array_size * sizeof(as_exp_entry)));                  \
            (*bottom) += expr_array_size;                                      \
        }                                                                      \
    }

#define BIN_EXPR()                                                             \
    {.op = _AS_EXP_CODE_BIN, .count = 3}, as_exp_int(temp_expr->result_type),  \
        _AS_EXP_VAL_RAWSTR(bin_name)

#define KEY_EXPR()                                                             \
    {.op = _AS_EXP_CODE_KEY, .count = 2}, as_exp_int(temp_expr->result_type)

#define LIST_MOD_EXP()                                                         \
    {                                                                          \
        .op = temp_expr->op, .v.list_pol = temp_expr->list_policy              \
    }

// STRUCT DEFINITIONS
typedef struct {
    int64_t op;
    int64_t result_type;
    union {
        as_list *val_list_p;
        as_map *val_map_p;
        char *val_string_p;
    } val;
    uint8_t val_flag;
    PyObject *pydict;
    PyObject *pytuple;
    as_cdt_ctx *ctx;

    as_list_policy *list_policy;
    as_map_policy *map_policy;

    int64_t num_children;
} intermediate_expr;

// FUNCTION DEFINITIONS
static as_status get_expr_size(int *size_to_alloc, int *intermediate_exprs_size,
                               as_vector *intermediate_exprs, as_error *err);

static as_status
get_exp_val_from_pyval(AerospikeClient *self, as_static_pool *static_pool,
                       int serializer_type, as_exp_entry *new_entry,
                       PyObject *py_obj, intermediate_expr *tmp_expr,
                       as_error *err);

static as_status
add_expr_macros(AerospikeClient *self, as_static_pool *static_pool,
                int serializer_type, as_vector *unicodeStrVector,
                as_vector *intermediate_expr_vector, as_exp_entry **expressions,
                int *bottom, int *size, as_error *err);

static bool free_temp_expr(intermediate_expr *temp_expr, as_error *err,
                           bool is_ctx_initialized);

// This is to improve readability when many as_exp_nils() are used
#define NIL as_exp_nil()

/*
* get_expr_size 
* Sets `size_to_alloc` to the byte count required to fit the array of as_exp_entry that will be allocated
* when `intermediate_exprs` is converted.
* Note that intermediate_exprs has an entry for every child of every expression but the child values' sizes do not need to be counted
* because their parents' size accounts for them.
*/
static as_status get_expr_size(int *size_to_alloc, int *intermediate_exprs_size,
                               as_vector *intermediate_exprs, as_error *err)
{

    static const int EXPR_SIZES[] = {
        [BIN] = EXP_SZ(as_exp_bin_int(0)),
        [_AS_EXP_CODE_AS_VAL] = EXP_SZ(as_exp_val(NULL)),
        [VAL] = EXP_SZ(as_exp_val(
            NULL)), // NOTE if I don't count vals I don't need to subtract from other ops // MUST count these for expressions with var args.
        [EQ] = EXP_SZ(
            as_exp_cmp_eq(NIL,
                          NIL)), // ^ TODO implement a less wastefull solution.
        [NE] = EXP_SZ(as_exp_cmp_ne(NIL, NIL)),
        [GT] = EXP_SZ(as_exp_cmp_gt(NIL, NIL)),
        [GE] = EXP_SZ(as_exp_cmp_ge(NIL, NIL)),
        [LT] = EXP_SZ(as_exp_cmp_lt(NIL, NIL)),
        [LE] = EXP_SZ(as_exp_cmp_le(NIL, NIL)),
        [CMP_REGEX] = EXP_SZ(as_exp_cmp_regex(0, "", NIL)),
        [CMP_GEO] = EXP_SZ(as_exp_cmp_geo(NIL, NIL)),
        [AND] = EXP_SZ(as_exp_and(NIL)),
        [OR] = EXP_SZ(as_exp_or(NIL)),
        [NOT] = EXP_SZ(as_exp_not(NIL)),
        [END_VA_ARGS] = EXP_SZ({.op = _AS_EXP_CODE_END_OF_VA_ARGS}),
        [META_DIGEST_MOD] = EXP_SZ(as_exp_digest_modulo(0)),
        [META_DEVICE_SIZE] = EXP_SZ(as_exp_device_size()),
        [META_LAST_UPDATE_TIME] = EXP_SZ(as_exp_last_update()),
        [META_VOID_TIME] = EXP_SZ(as_exp_void_time()),
        [META_TTL] = EXP_SZ(as_exp_ttl()),
        [META_SET_NAME] = EXP_SZ(as_exp_set_name()),
        [META_KEY_EXISTS] = EXP_SZ(as_exp_key_exist()),
        [META_MEMORY_SIZE] = EXP_SZ(as_exp_memory_size()),
        [META_RECORD_SIZE] = EXP_SZ(as_exp_record_size()),
        [REC_KEY] = EXP_SZ(
            as_exp_key_int()), // this covers as_exp_key_int() -> as_exp_key_blob
        [BIN_TYPE] = EXP_SZ(as_exp_bin_type("")),
        [BIN_EXISTS] = EXP_SZ(as_exp_bin_exists("")),
        [OP_LIST_GET_BY_INDEX] =
            EXP_SZ(as_exp_list_get_by_index(NULL, 0, 0, NIL, NIL)),
        [OP_LIST_SIZE] = EXP_SZ(as_exp_list_size(NULL, NIL)),
        [OP_LIST_GET_BY_VALUE] =
            EXP_SZ(as_exp_list_get_by_value(NULL, 0, NIL, NIL)),
        [OP_LIST_GET_BY_VALUE_RANGE] =
            EXP_SZ(as_exp_list_get_by_value_range(NULL, 0, NIL, NIL, NIL)),
        [OP_LIST_GET_BY_VALUE_LIST] =
            EXP_SZ(as_exp_list_get_by_value_list(NULL, 0, NIL, NIL)),
        [OP_LIST_GET_BY_VALUE_RANK_RANGE_REL_TO_END] = EXP_SZ(
            as_exp_list_get_by_rel_rank_range_to_end(NULL, 0, NIL, NIL, NIL)),
        [OP_LIST_GET_BY_VALUE_RANK_RANGE_REL] = EXP_SZ(
            as_exp_list_get_by_rel_rank_range(NULL, 0, NIL, NIL, NIL, NIL)),
        [OP_LIST_GET_BY_INDEX_RANGE_TO_END] =
            EXP_SZ(as_exp_list_get_by_index_range_to_end(NULL, 0, NIL, NIL)),
        [OP_LIST_GET_BY_INDEX_RANGE] =
            EXP_SZ(as_exp_list_get_by_index_range(NULL, 0, NIL, NIL, NIL)),
        [OP_LIST_GET_BY_RANK] =
            EXP_SZ(as_exp_list_get_by_rank(NULL, 0, 0, NIL, NIL)),
        [OP_LIST_GET_BY_RANK_RANGE_TO_END] =
            EXP_SZ(as_exp_list_get_by_rank_range_to_end(NULL, 0, NIL, NIL)),
        [OP_LIST_GET_BY_RANK_RANGE] =
            EXP_SZ(as_exp_list_get_by_rank_range(NULL, 0, NIL, NIL, NIL)),
        [OP_LIST_APPEND] = EXP_SZ(as_exp_list_append(NULL, NULL, NIL, NIL)),
        [OP_LIST_APPEND_ITEMS] =
            EXP_SZ(as_exp_list_append_items(NULL, NULL, NIL, NIL)),
        [OP_LIST_INSERT] =
            EXP_SZ(as_exp_list_insert(NULL, NULL, NIL, NIL, NIL)),
        [OP_LIST_INSERT_ITEMS] =
            EXP_SZ(as_exp_list_insert_items(NULL, NULL, NIL, NIL, NIL)),
        [OP_LIST_INCREMENT] =
            EXP_SZ(as_exp_list_increment(NULL, NULL, NIL, NIL, NIL)),
        [OP_LIST_SET] = EXP_SZ(as_exp_list_set(NULL, NULL, NIL, NIL, NIL)),
        [OP_LIST_CLEAR] = EXP_SZ(as_exp_list_clear(NULL, NIL)),
        [OP_LIST_SORT] = EXP_SZ(as_exp_list_sort(NULL, 0, NIL)),
        [OP_LIST_REMOVE_BY_VALUE] =
            EXP_SZ(as_exp_list_remove_by_value(NULL, 0, NIL, NIL)),
        [OP_LIST_REMOVE_BY_VALUE_LIST] =
            EXP_SZ(as_exp_list_remove_by_value_list(NULL, 0, NIL, NIL)),
        [OP_LIST_REMOVE_BY_VALUE_RANGE] =
            EXP_SZ(as_exp_list_remove_by_value_range(NULL, 0, NIL, NIL, NIL)),
        [OP_LIST_REMOVE_BY_REL_RANK_RANGE_TO_END] =
            EXP_SZ(as_exp_list_remove_by_rel_rank_range_to_end(NULL, 0, NIL,
                                                               NIL, NIL)),
        [OP_LIST_REMOVE_BY_REL_RANK_RANGE] = EXP_SZ(
            as_exp_list_remove_by_rel_rank_range(NULL, 0, NIL, NIL, NIL, NIL)),
        [OP_LIST_REMOVE_BY_INDEX] =
            EXP_SZ(as_exp_list_remove_by_index(NULL, NIL, NIL)),
        [OP_LIST_REMOVE_BY_INDEX_RANGE_TO_END] =
            EXP_SZ(as_exp_list_remove_by_index_range_to_end(NULL, 0, NIL, NIL)),
        [OP_LIST_REMOVE_BY_INDEX_RANGE] =
            EXP_SZ(as_exp_list_remove_by_index_range(NULL, 0, NIL, NIL, NIL)),
        [OP_LIST_REMOVE_BY_RANK] =
            EXP_SZ(as_exp_list_remove_by_rank(NULL, NIL, NIL)),
        [OP_LIST_REMOVE_BY_RANK_RANGE_TO_END] =
            EXP_SZ(as_exp_list_remove_by_rank_range_to_end(NULL, 0, NIL, NIL)),
        [OP_LIST_REMOVE_BY_RANK_RANGE] =
            EXP_SZ(as_exp_list_remove_by_rank_range(NULL, 0, NIL, NIL, NIL)),
        [OP_MAP_PUT] = EXP_SZ(as_exp_map_put(NULL, NULL, NIL, NIL, NIL)),
        [OP_MAP_PUT_ITEMS] = EXP_SZ(as_exp_map_put_items(NULL, NULL, NIL, NIL)),
        [OP_MAP_INCREMENT] =
            EXP_SZ(as_exp_map_increment(NULL, NULL, NIL, NIL, NIL)),
        [OP_MAP_CLEAR] = EXP_SZ(as_exp_map_clear(NULL, NIL)),
        [OP_MAP_REMOVE_BY_KEY] =
            EXP_SZ(as_exp_map_remove_by_key(NULL, NIL, NIL)),
        [OP_MAP_REMOVE_BY_KEY_LIST] =
            EXP_SZ(as_exp_map_remove_by_key_list(NULL, 0, NIL, NIL)),
        [OP_MAP_REMOVE_BY_KEY_RANGE] =
            EXP_SZ(as_exp_map_remove_by_key_range(NULL, 0, NIL, NIL, NIL)),
        [OP_MAP_REMOVE_BY_KEY_REL_INDEX_RANGE_TO_END] =
            EXP_SZ(as_exp_map_remove_by_key_rel_index_range_to_end(NULL, 0, NIL,
                                                                   NIL, NIL)),
        [OP_MAP_REMOVE_BY_KEY_REL_INDEX_RANGE] =
            EXP_SZ(as_exp_map_remove_by_key_rel_index_range(NULL, 0, NIL, NIL,
                                                            NIL, NIL)),
        [OP_MAP_REMOVE_BY_VALUE] =
            EXP_SZ(as_exp_map_remove_by_value(NULL, 0, NIL, NIL)),
        [OP_MAP_REMOVE_BY_VALUE_LIST] =
            EXP_SZ(as_exp_map_remove_by_value_list(NULL, 0, NIL, NIL)),
        [OP_MAP_REMOVE_BY_VALUE_RANGE] =
            EXP_SZ(as_exp_map_remove_by_value_range(NULL, 0, NIL, NIL, NIL)),
        [OP_MAP_REMOVE_BY_VALUE_REL_RANK_RANGE_TO_END] =
            EXP_SZ(as_exp_map_remove_by_value_rel_rank_range_to_end(
                NULL, 0, NIL, NIL, NIL)),
        [OP_MAP_REMOVE_BY_VALUE_REL_RANK_RANGE] =
            EXP_SZ(as_exp_map_remove_by_value_rel_rank_range(NULL, 0, NIL, NIL,
                                                             NIL, NIL)),
        [OP_MAP_REMOVE_BY_INDEX] =
            EXP_SZ(as_exp_map_remove_by_index(NULL, NIL, NIL)),
        [OP_MAP_REMOVE_BY_INDEX_RANGE_TO_END] =
            EXP_SZ(as_exp_map_remove_by_index_range_to_end(NULL, 0, NIL, NIL)),
        [OP_MAP_REMOVE_BY_INDEX_RANGE] =
            EXP_SZ(as_exp_map_remove_by_index_range(NULL, 0, NIL, NIL, NIL)),
        [OP_MAP_REMOVE_BY_RANK] =
            EXP_SZ(as_exp_map_remove_by_rank(NULL, NIL, NIL)),
        [OP_MAP_REMOVE_BY_RANK_RANGE_TO_END] =
            EXP_SZ(as_exp_map_remove_by_rank_range_to_end(NULL, 0, NIL, NIL)),
        [OP_MAP_REMOVE_BY_RANK_RANGE] =
            EXP_SZ(as_exp_map_remove_by_rank_range(NULL, 0, NIL, NIL, NIL)),
        [OP_MAP_SIZE] = EXP_SZ(as_exp_map_size(NULL, NIL)),
        [OP_MAP_GET_BY_KEY] =
            EXP_SZ(as_exp_map_get_by_key(NULL, 0, 0, NIL, NIL)),
        [OP_MAP_GET_BY_KEY_RANGE] =
            EXP_SZ(as_exp_map_get_by_key_range(NULL, 0, NIL, NIL, NIL)),
        [OP_MAP_GET_BY_KEY_LIST] =
            EXP_SZ(as_exp_map_get_by_key_list(NULL, 0, NIL, NIL)),
        [OP_MAP_GET_BY_KEY_REL_INDEX_RANGE_TO_END] =
            EXP_SZ(as_exp_map_get_by_key_rel_index_range_to_end(NULL, 0, NIL,
                                                                NIL, NIL)),
        [OP_MAP_GET_BY_KEY_REL_INDEX_RANGE] = EXP_SZ(
            as_exp_map_get_by_key_rel_index_range(NULL, 0, NIL, NIL, NIL, NIL)),
        [OP_MAP_GET_BY_VALUE] =
            EXP_SZ(as_exp_map_get_by_value(NULL, 0, NIL, NIL)),
        [OP_MAP_GET_BY_VALUE_RANGE] =
            EXP_SZ(as_exp_map_get_by_value_range(NULL, 0, NIL, NIL, NIL)),
        [OP_MAP_GET_BY_VALUE_LIST] =
            EXP_SZ(as_exp_map_get_by_value_list(NULL, 0, NIL, NIL)),
        [OP_MAP_GET_BY_VALUE_RANK_RANGE_REL_TO_END] =
            EXP_SZ(as_exp_map_get_by_value_rel_rank_range_to_end(NULL, 0, NIL,
                                                                 NIL, NIL)),
        [OP_MAP_GET_BY_VALUE_RANK_RANGE_REL] =
            EXP_SZ(as_exp_map_get_by_value_rel_rank_range(NULL, 0, NIL, NIL,
                                                          NIL, NIL)),
        [OP_MAP_GET_BY_INDEX] =
            EXP_SZ(as_exp_map_get_by_index(NULL, 0, 0, NIL, NIL)),
        [OP_MAP_GET_BY_INDEX_RANGE_TO_END] =
            EXP_SZ(as_exp_map_get_by_index_range_to_end(NULL, 0, NIL, NIL)),
        [OP_MAP_GET_BY_INDEX_RANGE] =
            EXP_SZ(as_exp_map_get_by_index_range(NULL, 0, NIL, NIL, NIL)),
        [OP_MAP_GET_BY_RANK] =
            EXP_SZ(as_exp_map_get_by_rank(NULL, 0, 0, NIL, NIL)),
        [OP_MAP_GET_BY_RANK_RANGE_TO_END] =
            EXP_SZ(as_exp_map_get_by_rank_range_to_end(NULL, 0, NIL, NIL)),
        [OP_MAP_GET_BY_RANK_RANGE] =
            EXP_SZ(as_exp_map_get_by_rank_range(NULL, 0, NIL, NIL, NIL)),
        [_AS_EXP_BIT_FLAGS] = 0, //EXP_SZ(as_exp_uint(0)),
        [OP_BIT_RESIZE] = EXP_SZ(as_exp_bit_resize(NULL, NIL, 0, NIL)),
        [OP_BIT_INSERT] = EXP_SZ(as_exp_bit_insert(NULL, NIL, NIL, NIL)),
        [OP_BIT_REMOVE] = EXP_SZ(as_exp_bit_remove(NULL, NIL, NIL, NIL)),
        [OP_BIT_SET] = EXP_SZ(as_exp_bit_set(NULL, NIL, NIL, NIL, NIL)),
        [OP_BIT_OR] = EXP_SZ(as_exp_bit_or(NULL, NIL, NIL, NIL, NIL)),
        [OP_BIT_XOR] = EXP_SZ(as_exp_bit_xor(NULL, NIL, NIL, NIL, NIL)),
        [OP_BIT_AND] = EXP_SZ(as_exp_bit_and(NULL, NIL, NIL, NIL, NIL)),
        [OP_BIT_NOT] = EXP_SZ(as_exp_bit_not(NULL, NIL, NIL, NIL)),
        [OP_BIT_LSHIFT] = EXP_SZ(as_exp_bit_lshift(NULL, NIL, NIL, NIL, NIL)),
        [OP_BIT_RSHIFT] = EXP_SZ(as_exp_bit_rshift(NULL, NIL, NIL, NIL, NIL)),
        [OP_BIT_ADD] = EXP_SZ(as_exp_bit_add(NULL, NIL, NIL, NIL, 0, NIL)),
        [OP_BIT_SUBTRACT] =
            EXP_SZ(as_exp_bit_subtract(NULL, NIL, NIL, NIL, 0, NIL)),
        [OP_BIT_SET_INT] = EXP_SZ(as_exp_bit_set_int(NULL, NIL, NIL, NIL, NIL)),
        [OP_BIT_GET] = EXP_SZ(as_exp_bit_get(NIL, NIL, NIL)),
        [OP_BIT_COUNT] = EXP_SZ(as_exp_bit_count(NIL, NIL, NIL)),
        [OP_BIT_LSCAN] = EXP_SZ(as_exp_bit_lscan(NIL, NIL, NIL, NIL)),
        [OP_BIT_RSCAN] = EXP_SZ(as_exp_bit_rscan(NIL, NIL, NIL, NIL)),
        [OP_BIT_GET_INT] = EXP_SZ(as_exp_bit_get_int(NIL, NIL, 0, NIL)),
        [OP_HLL_INIT] = EXP_SZ(as_exp_hll_init_mh(NULL, 0, 0, NIL)),
        [OP_HLL_ADD] = EXP_SZ(as_exp_hll_add_mh(NULL, NIL, 0, 0, NIL)),
        [OP_HLL_GET_COUNT] = EXP_SZ(as_exp_hll_update(NULL, NIL, NIL)),
        [OP_HLL_GET_UNION] = EXP_SZ(as_exp_hll_get_union(NIL, NIL)),
        [OP_HLL_GET_UNION_COUNT] = EXP_SZ(as_exp_hll_get_union_count(NIL, NIL)),
        [OP_HLL_GET_INTERSECT_COUNT] =
            EXP_SZ(as_exp_hll_get_intersect_count(NIL, NIL)),
        [OP_HLL_GET_SIMILARITY] = EXP_SZ(as_exp_hll_get_similarity(NIL, NIL)),
        [OP_HLL_DESCRIBE] = EXP_SZ(as_exp_hll_describe(NIL)),
        [OP_HLL_MAY_CONTAIN] = EXP_SZ(as_exp_hll_may_contain(NIL, NIL)),
        [_AS_EXP_CODE_CDT_LIST_CRMOD] = 0, //EXP_SZ(as_exp_val(NULL)),
        [_AS_EXP_CODE_CDT_LIST_MOD] = 0,   //EXP_SZ(as_exp_val(NULL)),
        [_AS_EXP_CODE_CDT_MAP_CRMOD] = 0,  //EXP_SZ(as_exp_val(NULL)),
        [_AS_EXP_CODE_CDT_MAP_CR] = 0,     //EXP_SZ(as_exp_val(NULL)),
        [_AS_EXP_CODE_CDT_MAP_MOD] = 0,    //EXP_SZ(as_exp_val(NULL)),
        [EXCLUSIVE] = EXP_SZ(as_exp_exclusive(NIL)),
        [ADD] = EXP_SZ(as_exp_add(NIL)),
        [SUB] = EXP_SZ(as_exp_sub(NIL)),
        [MUL] = EXP_SZ(as_exp_mul(NIL)),
        [DIV] = EXP_SZ(as_exp_div(NIL)),
        [POW] = EXP_SZ(as_exp_pow(NIL, NIL)),
        [LOG] = EXP_SZ(as_exp_log(NIL, NIL)),
        [MOD] = EXP_SZ(as_exp_mod(NIL, NIL)),
        [ABS] = EXP_SZ(as_exp_abs(NIL)),
        [FLOOR] = EXP_SZ(as_exp_floor(NIL)),
        [CEIL] = EXP_SZ(as_exp_ceil(NIL)),
        [TO_INT] = EXP_SZ(as_exp_to_int(NIL)),
        [TO_FLOAT] = EXP_SZ(as_exp_to_float(NIL)),
        [INT_AND] = EXP_SZ(as_exp_int_and(NIL)),
        [INT_OR] = EXP_SZ(as_exp_int_or(NIL)),
        [INT_XOR] = EXP_SZ(as_exp_int_xor(NIL)),
        [INT_NOT] = EXP_SZ(as_exp_int_not(NIL)),
        [INT_LSHIFT] = EXP_SZ(as_exp_int_lshift(NIL, NIL)),
        [INT_RSHIFT] = EXP_SZ(as_exp_int_rshift(NIL, NIL)),
        [INT_ARSHIFT] = EXP_SZ(as_exp_int_arshift(NIL, NIL)),
        [INT_COUNT] = EXP_SZ(as_exp_int_count(NIL)),
        [INT_LSCAN] = EXP_SZ(as_exp_int_lscan(NIL, NIL)),
        [INT_RSCAN] = EXP_SZ(as_exp_int_rscan(NIL, NIL)),
        [MIN] = EXP_SZ(as_exp_min(NIL)),
        [MAX] = EXP_SZ(as_exp_max(NIL)),
        [COND] = EXP_SZ(as_exp_cond(NIL)),
        [LET] = EXP_SZ(as_exp_let(NIL)),
        [DEF] = EXP_SZ(as_exp_def("", NIL)),
        [VAR] = EXP_SZ(as_exp_var("")),
        [UNKNOWN] = EXP_SZ(as_exp_unknown())};

    for (int i = 0; i < *intermediate_exprs_size; ++i) {
        intermediate_expr *tmp_expr =
            (intermediate_expr *)as_vector_get(intermediate_exprs, (uint32_t)i);
        (*size_to_alloc) += EXPR_SIZES[tmp_expr->op];
    }

    if (size_to_alloc <= 0) {
        return as_error_update(err, AEROSPIKE_ERR_PARAM, "Invalid expression");
    }

    return AEROSPIKE_OK;
}

/*
* get_exp_val_from_pyval
* Converts a python value into an expression value.
*/
static as_status
get_exp_val_from_pyval(AerospikeClient *self, as_static_pool *static_pool,
                       int serializer_type, as_exp_entry *new_entry,
                       PyObject *py_obj, intermediate_expr *temp_expr,
                       as_error *err)
{
    as_error_reset(err);

    if (!py_obj) {
        return as_error_update(err, AEROSPIKE_ERR_CLIENT,
                               "py_obj value is null");
    }
    else if (PyBool_Check(py_obj)) {
        as_exp_entry tmp_entry = as_exp_bool(PyObject_IsTrue(py_obj));
        *new_entry =
            tmp_entry; //TODO use as_exp_val((as_val *) bytes); here, might need a cast, not blocker
    }
    else if (PyLong_Check(py_obj)) {
        int64_t l = (int64_t)PyLong_AsLongLong(py_obj);
        if (l == -1 && PyErr_Occurred()) {
            if (PyErr_ExceptionMatches(PyExc_OverflowError)) {
                return as_error_update(err, AEROSPIKE_ERR_PARAM,
                                       "integer value exceeds sys.maxsize");
            }
        }

        as_exp_entry tmp_entry = as_exp_int(l);
        *new_entry = tmp_entry;
    }
    else if (PyUnicode_Check(py_obj)) {
        PyObject *py_ustr = PyUnicode_AsUTF8String(py_obj);
        char *str = PyBytes_AsString(py_ustr);
        temp_expr->val.val_string_p = strdup(str);
        temp_expr->val_flag = VAL_STRING_P_ACTIVE;
        as_exp_entry tmp_entry = as_exp_str(temp_expr->val.val_string_p);
        *new_entry = tmp_entry;
        Py_DECREF(py_ustr);
    }
    else if (PyBytes_Check(py_obj)) {
        uint8_t *b = (uint8_t *)PyBytes_AsString(py_obj);
        uint32_t b_len = (uint32_t)PyBytes_Size(py_obj);
        as_exp_entry tmp_entry = as_exp_bytes(b, b_len);
        *new_entry = tmp_entry;
    }
    else if (!strcmp(py_obj->ob_type->tp_name, "aerospike.Geospatial")) {
        PyObject *py_parameter = PyUnicode_FromString("geo_data");
        PyObject *py_data = PyObject_GenericGetAttr(py_obj, py_parameter);
        Py_DECREF(py_parameter);
        char *geo_value =
            (char *)PyUnicode_AsUTF8(AerospikeGeospatial_DoDumps(py_data, err));
        Py_DECREF(py_data);
        as_exp_entry tmp_entry = as_exp_geo(geo_value);
        *new_entry = tmp_entry;
    }
    else if (PyByteArray_Check(py_obj)) {
        as_bytes *bytes;
        GET_BYTES_POOL(bytes, static_pool, err);
        if (err->code == AEROSPIKE_OK) {
            if (serialize_based_on_serializer_policy(self, serializer_type,
                                                     &bytes, py_obj,
                                                     err) != AEROSPIKE_OK) {
                return err->code;
            }
            as_exp_entry tmp_entry = as_exp_val(
                (as_val *)
                    bytes); //TODO can this be simplified to a buffer and as_exp_bytes?
            *new_entry = tmp_entry;
        }
    }
    else if (PyList_Check(py_obj)) {
        as_list *list = NULL;
        pyobject_to_list(self, err, py_obj, &list, static_pool,
                         serializer_type);
        if (err->code == AEROSPIKE_OK) {
            temp_expr->val.val_list_p = list;
            temp_expr->val_flag = VAL_LIST_P_ACTIVE;
            as_exp_entry tmp_entry = as_exp_val(list);
            *new_entry = tmp_entry;
        }
    }
    else if (PyDict_Check(py_obj)) {
        as_map *map = NULL;
        pyobject_to_map(self, err, py_obj, &map, static_pool, serializer_type);
        if (err->code == AEROSPIKE_OK) {
            temp_expr->val.val_map_p = map;
            temp_expr->val_flag = VAL_MAP_P_ACTIVE;
            as_exp_entry tmp_entry = as_exp_val(map);
            *new_entry = tmp_entry;
        }
    }
    else if (Py_None == py_obj) {
        as_exp_entry tmp_entry = as_exp_nil();
        *new_entry = tmp_entry;
    }
    else if (!strcmp(py_obj->ob_type->tp_name, "aerospike.null")) {
        as_exp_entry tmp_entry = as_exp_nil();
        *new_entry = tmp_entry;
    }
    else if (AS_Matches_Classname(py_obj, AS_CDT_WILDCARD_NAME)) {
        as_exp_entry tmp_entry =
            as_exp_val((as_val *)as_val_reserve(&as_cmp_wildcard));
        *new_entry = tmp_entry;
    }
    else if (AS_Matches_Classname(py_obj, AS_CDT_INFINITE_NAME)) {
        as_exp_entry tmp_entry =
            as_exp_val((as_val *)as_val_reserve(&as_cmp_inf));
        *new_entry = tmp_entry;
    }
    else {
        if (PyFloat_Check(py_obj)) {
            double d = PyFloat_AsDouble(py_obj);
            as_exp_entry tmp_entry = as_exp_float(d);
            *new_entry = tmp_entry;
        }
        else {
            as_bytes *bytes;
            GET_BYTES_POOL(bytes, static_pool, err);
            if (err->code == AEROSPIKE_OK) {
                if (serialize_based_on_serializer_policy(self, serializer_type,
                                                         &bytes, py_obj,
                                                         err) != AEROSPIKE_OK) {
                    return err->code;
                }

                as_exp_entry tmp_entry = as_exp_val((as_val *)bytes);
                *new_entry = tmp_entry;
            }
        }
    }

    return err->code;
}

/*
* add_expr_macros
* Converts each intermediate_expr struct in intermediate_expr_vector to as_exp_entries and copies them to expressions.
* Note that a count of as_exp_entries to leave out of the copy is passed to the `APPEND_ARRAY` macro.
* Since this function uses the C expressions macros directly, we don't want to copy the useless junk generated by the 
* empty arguments used. Each expression child/value has a intermediate_expr struct in intermediate_expr_vector so the missing values will be copied later.
* These counts need to be updated if the C client macro changes.
*/
static as_status
add_expr_macros(AerospikeClient *self, as_static_pool *static_pool,
                int serializer_type, as_vector *unicodeStrVector,
                as_vector *intermediate_expr_vector, as_exp_entry **expressions,
                int *bottom, int *size, as_error *err)
{

    for (int i = 0; i < *size; ++i) {
        intermediate_expr *temp_expr = (intermediate_expr *)as_vector_get(
            intermediate_expr_vector, (uint32_t)i);

        int64_t lval1 = 0;
        int64_t lval2 = 0;
        char *bin_name = NULL;
        PyObject *py_val_from_dict = NULL;

        if (temp_expr->op >= _AS_EXP_CODE_CDT_LIST_CRMOD &&
            temp_expr->op <= _AS_EXP_CODE_CDT_MAP_MOD) {

            if (temp_expr->op == _AS_EXP_CODE_CDT_LIST_CRMOD ||
                temp_expr->op == _AS_EXP_CODE_CDT_LIST_MOD) {
                APPEND_ARRAY(0, LIST_MOD_EXP());
            }
            else if (temp_expr->op >= _AS_EXP_CODE_CDT_MAP_CRMOD &&
                     temp_expr->op <= _AS_EXP_CODE_CDT_MAP_MOD) {
                APPEND_ARRAY(0, {.op = temp_expr->op,
                                 .v.map_pol = temp_expr->map_policy});
            }

            continue;
        }

        switch (temp_expr->op) {
        case BIN:
            if (get_bin(err, temp_expr->pydict, unicodeStrVector, &bin_name) !=
                AEROSPIKE_OK) {
                return err->code;
            }

            APPEND_ARRAY(0, BIN_EXPR());
            break;
        case VAL:
        case _AS_EXP_CODE_AS_VAL:;
            as_exp_entry tmp_expr;
            if (get_exp_val_from_pyval(
                    self, static_pool, serializer_type, &tmp_expr,
                    PyDict_GetItemString(temp_expr->pydict, AS_PY_VAL_KEY),
                    temp_expr, err) != AEROSPIKE_OK) {
                return err->code;
            }

            APPEND_ARRAY(0, tmp_expr);
            break;
        case EQ:
            APPEND_ARRAY(2, as_exp_cmp_eq(NIL, NIL));
            break;
        case NE:
            APPEND_ARRAY(2, as_exp_cmp_ne(NIL, NIL));
            break;
        case GT:
            APPEND_ARRAY(2, as_exp_cmp_gt(NIL, NIL));
            break;
        case GE:
            APPEND_ARRAY(2, as_exp_cmp_ge(NIL, NIL));
            break;
        case LT:
            APPEND_ARRAY(2, as_exp_cmp_lt(NIL, NIL));
            break;
        case LE:
            APPEND_ARRAY(2, as_exp_cmp_le(NIL, NIL));
            break;
        case CMP_REGEX:
            if (get_int64_t(err, REGEX_OPTIONS_KEY, temp_expr->pydict,
                            &lval1) != AEROSPIKE_OK) {
                return err->code;
            }

            py_val_from_dict =
                PyDict_GetItemString(temp_expr->pydict, AS_PY_VAL_KEY);
            char *regex_str = NULL;
            if (PyUnicode_Check(py_val_from_dict)) {
                const char *utf8_encoding = PyUnicode_AsUTF8(py_val_from_dict);
                regex_str = strdup(utf8_encoding);
                temp_expr->val.val_string_p = regex_str;
                temp_expr->val_flag = VAL_STRING_P_ACTIVE;
            }
            else {
                return as_error_update(err, AEROSPIKE_ERR_PARAM,
                                       "regex_str must be a string.");
            }

            APPEND_ARRAY(1, as_exp_cmp_regex(lval1, regex_str, NIL));
            break;
        case CMP_GEO:
            APPEND_ARRAY(2, as_exp_cmp_geo(NIL, NIL));
            break;
        case AND:
            APPEND_ARRAY(2, as_exp_and(NIL));
            break;
        case OR:
            APPEND_ARRAY(2, as_exp_or(NIL));
            break;
        case NOT:
            APPEND_ARRAY(1, as_exp_not(NIL));
            break;
        case END_VA_ARGS:
            APPEND_ARRAY(
                0,
                {.op =
                     _AS_EXP_CODE_END_OF_VA_ARGS}); //NOTE: this case handles the end of arguments to an AND/OR expression.
            break;
        case META_DIGEST_MOD:
            if (get_int64_t(err, AS_PY_VAL_KEY, temp_expr->pydict, &lval1) !=
                AEROSPIKE_OK) {
                return err->code;
            }

            APPEND_ARRAY(0, as_exp_digest_modulo(lval1));
            break;
        case META_DEVICE_SIZE:
            APPEND_ARRAY(0, as_exp_device_size());
            break;
        case META_LAST_UPDATE_TIME:
            APPEND_ARRAY(0, as_exp_last_update());
            break;
        case META_SINCE_UPDATE_TIME:
            APPEND_ARRAY(0, as_exp_since_update());
            break;
        case META_IS_TOMBSTONE:
            APPEND_ARRAY(0, as_exp_is_tombstone());
            break;
        case META_VOID_TIME:
            APPEND_ARRAY(0, as_exp_void_time());
            break;
        case META_TTL:
            APPEND_ARRAY(0, as_exp_ttl());
            break;
        case META_SET_NAME:
            APPEND_ARRAY(0, as_exp_set_name());
            break;
        case META_KEY_EXISTS:
            APPEND_ARRAY(0, as_exp_key_exist());
            break;
        case META_MEMORY_SIZE:
            APPEND_ARRAY(0, as_exp_memory_size());
            break;
        case META_RECORD_SIZE:
            APPEND_ARRAY(0, as_exp_record_size());
            break;
        case REC_KEY:
            APPEND_ARRAY(0, KEY_EXPR());
            break;
        case BIN_TYPE:
            if (get_bin(err, temp_expr->pydict, unicodeStrVector, &bin_name) !=
                AEROSPIKE_OK) {
                return err->code;
            }

            APPEND_ARRAY(0, as_exp_bin_type(bin_name));
            break;
        case BIN_EXISTS:
            if (get_bin(err, temp_expr->pydict, unicodeStrVector, &bin_name) !=
                AEROSPIKE_OK) {
                return err->code;
            }

            APPEND_ARRAY(0, as_exp_bin_exists(bin_name));
            break;
        case OP_LIST_GET_BY_INDEX:
            if (get_int64_t(err, AS_PY_VALUE_TYPE_KEY, temp_expr->pydict,
                            &lval2) != AEROSPIKE_OK) {
                return err->code;
            }

            if (get_int64_t(err, AS_PY_LIST_RETURN_KEY, temp_expr->pydict,
                            &lval1) != AEROSPIKE_OK) {
                return err->code;
            }

            APPEND_ARRAY(
                2, as_exp_list_get_by_index(temp_expr->ctx, lval1, lval2, NIL,
                                            NIL)); // - 2 for index, bin
            break;
        case OP_LIST_SIZE:
            APPEND_ARRAY(1, as_exp_list_size(temp_expr->ctx, NIL));
            break;
        case OP_LIST_GET_BY_VALUE:
            if (get_int64_t(err, AS_PY_LIST_RETURN_KEY, temp_expr->pydict,
                            &lval1) != AEROSPIKE_OK) {
                return err->code;
            }

            APPEND_ARRAY(2,
                         as_exp_list_get_by_value(temp_expr->ctx, lval1, NIL,
                                                  NIL)); // - 2 for value, bin
            break;
        case OP_LIST_GET_BY_VALUE_RANGE:
            if (get_int64_t(err, AS_PY_LIST_RETURN_KEY, temp_expr->pydict,
                            &lval1) != AEROSPIKE_OK) {
                return err->code;
            }

            APPEND_ARRAY(3, as_exp_list_get_by_value_range(
                                temp_expr->ctx, lval1, NIL, NIL,
                                NIL)); // - 3 for begin, end, bin
            break;
        case OP_LIST_GET_BY_VALUE_LIST:
            if (get_int64_t(err, AS_PY_LIST_RETURN_KEY, temp_expr->pydict,
                            &lval1) != AEROSPIKE_OK) {
                return err->code;
            }

            APPEND_ARRAY(
                2, as_exp_list_get_by_value_list(temp_expr->ctx, lval1, NIL,
                                                 NIL)); // - 2 for value, bin
            break;
        case OP_LIST_GET_BY_VALUE_RANK_RANGE_REL_TO_END:
            if (get_int64_t(err, AS_PY_LIST_RETURN_KEY, temp_expr->pydict,
                            &lval1) != AEROSPIKE_OK) {
                return err->code;
            }

            APPEND_ARRAY(3, as_exp_list_get_by_rel_rank_range_to_end(
                                temp_expr->ctx, lval1, NIL, NIL,
                                NIL)); // - 3 for value, rank, bin
            break;
        case OP_LIST_GET_BY_VALUE_RANK_RANGE_REL:
            if (get_int64_t(err, AS_PY_LIST_RETURN_KEY, temp_expr->pydict,
                            &lval1) != AEROSPIKE_OK) {
                return err->code;
            }

            APPEND_ARRAY(4, as_exp_list_get_by_rel_rank_range(
                                temp_expr->ctx, lval1, NIL, NIL, NIL,
                                NIL)); // - 4 for value, rank, count, bin
            break;
        case OP_LIST_GET_BY_INDEX_RANGE_TO_END:
            if (get_int64_t(err, AS_PY_LIST_RETURN_KEY, temp_expr->pydict,
                            &lval1) != AEROSPIKE_OK) {
                return err->code;
            }

            APPEND_ARRAY(2, as_exp_list_get_by_index_range_to_end(
                                temp_expr->ctx, lval1, NIL,
                                NIL)); // - 2 for index, bin
            break;
        case OP_LIST_GET_BY_INDEX_RANGE:
            if (get_int64_t(err, AS_PY_LIST_RETURN_KEY, temp_expr->pydict,
                            &lval1) != AEROSPIKE_OK) {
                return err->code;
            }

            APPEND_ARRAY(3, as_exp_list_get_by_index_range(
                                temp_expr->ctx, lval1, NIL, NIL,
                                NIL)); // - 3 for index, count, bin
            break;
        case OP_LIST_GET_BY_RANK:
            if (get_int64_t(err, AS_PY_LIST_RETURN_KEY, temp_expr->pydict,
                            &lval1) != AEROSPIKE_OK) {
                return err->code;
            }

            if (get_int64_t(err, AS_PY_VALUE_TYPE_KEY, temp_expr->pydict,
                            &lval2) != AEROSPIKE_OK) {
                return err->code;
            }

            APPEND_ARRAY(2, as_exp_list_get_by_rank(temp_expr->ctx, lval1,
                                                    lval2, NIL,
                                                    NIL)); // - 2 for rank, bin
            break;
        case OP_LIST_GET_BY_RANK_RANGE_TO_END:
            if (get_int64_t(err, AS_PY_LIST_RETURN_KEY, temp_expr->pydict,
                            &lval1) != AEROSPIKE_OK) {
                return err->code;
            }

            APPEND_ARRAY(2, as_exp_list_get_by_rank_range_to_end(
                                temp_expr->ctx, lval1, NIL,
                                NIL)); // - 2 for rank, bin
            break;
        case OP_LIST_GET_BY_RANK_RANGE:
            if (get_int64_t(err, AS_PY_LIST_RETURN_KEY, temp_expr->pydict,
                            &lval1) != AEROSPIKE_OK) {
                return err->code;
            }

            APPEND_ARRAY(3, as_exp_list_get_by_rank_range(
                                temp_expr->ctx, lval1, NIL, NIL,
                                NIL)); // - 3 for rank, count, bin
            break;
        case OP_LIST_APPEND:
            APPEND_ARRAY(
                3, as_exp_list_append(
                       temp_expr->ctx, temp_expr->list_policy, NIL,
                       NIL)); // -3 for val, _AS_EXP_CODE_CDT_LIST_CRMOD, bin
            break;
        case OP_LIST_APPEND_ITEMS:
            APPEND_ARRAY(3, as_exp_list_append_items(temp_expr->ctx,
                                                     temp_expr->list_policy,
                                                     NIL, NIL));
            break;
        case OP_LIST_INSERT:
            APPEND_ARRAY(4, as_exp_list_insert(temp_expr->ctx,
                                               temp_expr->list_policy, NIL, NIL,
                                               NIL));
            break;
        case OP_LIST_INSERT_ITEMS:
            APPEND_ARRAY(4, as_exp_list_insert_items(temp_expr->ctx,
                                                     temp_expr->list_policy,
                                                     NIL, NIL, NIL));
            break;
        case OP_LIST_INCREMENT:
            APPEND_ARRAY(4, as_exp_list_increment(temp_expr->ctx,
                                                  temp_expr->list_policy, NIL,
                                                  NIL, NIL));
            break;
        case OP_LIST_SET:
            APPEND_ARRAY(4,
                         as_exp_list_set(temp_expr->ctx, temp_expr->list_policy,
                                         NIL, NIL, NIL));
            break;
        case OP_LIST_CLEAR:
            APPEND_ARRAY(1, as_exp_list_clear(temp_expr->ctx,
                                              NIL)); // -1 for bin
            break;
        case OP_LIST_SORT:
            if (get_int64_t(err, LIST_ORDER_KEY, temp_expr->pydict, &lval1) !=
                AEROSPIKE_OK) {
                return err->code;
            }

            APPEND_ARRAY(1, as_exp_list_sort(temp_expr->ctx, lval1,
                                             NIL)); // -1 for bin
            break;
        case OP_LIST_REMOVE_BY_VALUE:
            if (get_int64_t(err, AS_PY_LIST_RETURN_KEY, temp_expr->pydict,
                            &lval1) != AEROSPIKE_OK) {
                return err->code;
            }
            APPEND_ARRAY(
                2, as_exp_list_remove_by_value(temp_expr->ctx, lval1, NIL,
                                               NIL)); // -2 for bin and val
            break;
        case OP_LIST_REMOVE_BY_VALUE_LIST:
            if (get_int64_t(err, AS_PY_LIST_RETURN_KEY, temp_expr->pydict,
                            &lval1) != AEROSPIKE_OK) {
                return err->code;
            }
            APPEND_ARRAY(
                2, as_exp_list_remove_by_value_list(temp_expr->ctx, lval1, NIL,
                                                    NIL)); // -2 for bin and val
            break;
        case OP_LIST_REMOVE_BY_VALUE_RANGE:
            if (get_int64_t(err, AS_PY_LIST_RETURN_KEY, temp_expr->pydict,
                            &lval1) != AEROSPIKE_OK) {
                return err->code;
            }
            APPEND_ARRAY(3, as_exp_list_remove_by_value_range(
                                temp_expr->ctx, lval1, NIL, NIL,
                                NIL)); // - 3 for begin, end, val
            break;
        case OP_LIST_REMOVE_BY_REL_RANK_RANGE_TO_END:
            if (get_int64_t(err, AS_PY_LIST_RETURN_KEY, temp_expr->pydict,
                            &lval1) != AEROSPIKE_OK) {
                return err->code;
            }
            APPEND_ARRAY(3, as_exp_list_remove_by_rel_rank_range_to_end(
                                temp_expr->ctx, lval1, NIL, NIL,
                                NIL)); // -3 for value, rank, bin
            break;
        case OP_LIST_REMOVE_BY_REL_RANK_RANGE:
            if (get_int64_t(err, AS_PY_LIST_RETURN_KEY, temp_expr->pydict,
                            &lval1) != AEROSPIKE_OK) {
                return err->code;
            }
            APPEND_ARRAY(4, as_exp_list_remove_by_rel_rank_range(
                                temp_expr->ctx, lval1, NIL, NIL, NIL,
                                NIL)); // -4 for value, rank, count, bin
            break;
        case OP_LIST_REMOVE_BY_INDEX:
            APPEND_ARRAY(2,
                         as_exp_list_remove_by_index(temp_expr->ctx, NIL,
                                                     NIL)); // -2 for index, bin
            break;
        case OP_LIST_REMOVE_BY_INDEX_RANGE_TO_END:
            if (get_int64_t(err, AS_PY_LIST_RETURN_KEY, temp_expr->pydict,
                            &lval1) != AEROSPIKE_OK) {
                return err->code;
            }
            APPEND_ARRAY(2, as_exp_list_remove_by_index_range_to_end(
                                temp_expr->ctx, lval1, NIL,
                                NIL)); // -2 for index, bin
            break;
        case OP_LIST_REMOVE_BY_INDEX_RANGE:
            if (get_int64_t(err, AS_PY_LIST_RETURN_KEY, temp_expr->pydict,
                            &lval1) != AEROSPIKE_OK) {
                return err->code;
            }
            APPEND_ARRAY(3, as_exp_list_remove_by_index_range(
                                temp_expr->ctx, lval1, NIL, NIL,
                                NIL)); // - 3 for index, count, bin
            break;
        case OP_LIST_REMOVE_BY_RANK:
            APPEND_ARRAY(2,
                         as_exp_list_remove_by_rank(temp_expr->ctx, NIL,
                                                    NIL)); // -2 for rank, bin
            break;
        case OP_LIST_REMOVE_BY_RANK_RANGE_TO_END:
            if (get_int64_t(err, AS_PY_LIST_RETURN_KEY, temp_expr->pydict,
                            &lval1) != AEROSPIKE_OK) {
                return err->code;
            }
            APPEND_ARRAY(2, as_exp_list_remove_by_rank_range_to_end(
                                temp_expr->ctx, lval1, NIL,
                                NIL)); // - 2 for rank, bin
            break;
        case OP_LIST_REMOVE_BY_RANK_RANGE:
            if (get_int64_t(err, AS_PY_LIST_RETURN_KEY, temp_expr->pydict,
                            &lval1) != AEROSPIKE_OK) {
                return err->code;
            }
            APPEND_ARRAY(3, as_exp_list_remove_by_rank_range(
                                temp_expr->ctx, lval1, NIL, NIL,
                                NIL)); // - 3 for rank, count, bin
            break;
        case OP_MAP_PUT:
            APPEND_ARRAY(4,
                         as_exp_map_put(temp_expr->ctx, temp_expr->map_policy,
                                        NIL, NIL, NIL));
            break;
        case OP_MAP_PUT_ITEMS:
            APPEND_ARRAY(3,
                         as_exp_map_put_items(temp_expr->ctx,
                                              temp_expr->map_policy, NIL, NIL));
            break;
        case OP_MAP_INCREMENT:
            APPEND_ARRAY(4, as_exp_map_increment(temp_expr->ctx,
                                                 temp_expr->map_policy, NIL,
                                                 NIL, NIL));
            break;
        case OP_MAP_CLEAR:
            APPEND_ARRAY(1, as_exp_map_clear(temp_expr->ctx,
                                             NIL)); // - 1 for bin
            break;
        case OP_MAP_REMOVE_BY_KEY:
            APPEND_ARRAY(2, as_exp_map_remove_by_key(temp_expr->ctx, NIL,
                                                     NIL)); // - 2 for key, bin
            break;
        case OP_MAP_REMOVE_BY_KEY_LIST:
            if (get_int64_t(err, AS_PY_MAP_RETURN_KEY, temp_expr->pydict,
                            &lval1) != AEROSPIKE_OK) {
                return err->code;
            }
            APPEND_ARRAY(
                2, as_exp_map_remove_by_key_list(temp_expr->ctx, lval1, NIL,
                                                 NIL)); // - 2 for key, bin
            break;
        case OP_MAP_REMOVE_BY_KEY_RANGE:
            if (get_int64_t(err, AS_PY_MAP_RETURN_KEY, temp_expr->pydict,
                            &lval1) != AEROSPIKE_OK) {
                return err->code;
            }
            APPEND_ARRAY(3, as_exp_map_remove_by_key_range(
                                temp_expr->ctx, lval1, NIL, NIL,
                                NIL)); // - 3 for begin, end, bin
            break;
        case OP_MAP_REMOVE_BY_KEY_REL_INDEX_RANGE_TO_END:
            if (get_int64_t(err, AS_PY_MAP_RETURN_KEY, temp_expr->pydict,
                            &lval1) != AEROSPIKE_OK) {
                return err->code;
            }
            APPEND_ARRAY(3, as_exp_map_remove_by_key_rel_index_range_to_end(
                                temp_expr->ctx, lval1, NIL, NIL,
                                NIL)); // - 3 for key, index, bin
            break;
        case OP_MAP_REMOVE_BY_KEY_REL_INDEX_RANGE:
            if (get_int64_t(err, AS_PY_MAP_RETURN_KEY, temp_expr->pydict,
                            &lval1) != AEROSPIKE_OK) {
                return err->code;
            }
            APPEND_ARRAY(4, as_exp_map_remove_by_key_rel_index_range(
                                temp_expr->ctx, lval1, NIL, NIL, NIL,
                                NIL)); // - 4 for key, index, count, bin
            break;
        case OP_MAP_REMOVE_BY_VALUE:
            if (get_int64_t(err, AS_PY_MAP_RETURN_KEY, temp_expr->pydict,
                            &lval1) != AEROSPIKE_OK) {
                return err->code;
            }
            APPEND_ARRAY(2,
                         as_exp_map_remove_by_value(temp_expr->ctx, lval1, NIL,
                                                    NIL)); // - 2 for val, bin
            break;
        case OP_MAP_REMOVE_BY_VALUE_LIST:
            if (get_int64_t(err, AS_PY_MAP_RETURN_KEY, temp_expr->pydict,
                            &lval1) != AEROSPIKE_OK) {
                return err->code;
            }
            APPEND_ARRAY(
                2, as_exp_map_remove_by_value_list(temp_expr->ctx, lval1, NIL,
                                                   NIL)); // - 2 for values, bin
            break;
        case OP_MAP_REMOVE_BY_VALUE_RANGE:
            if (get_int64_t(err, AS_PY_MAP_RETURN_KEY, temp_expr->pydict,
                            &lval1) != AEROSPIKE_OK) {
                return err->code;
            }
            APPEND_ARRAY(3, as_exp_map_remove_by_value_range(
                                temp_expr->ctx, lval1, NIL, NIL,
                                NIL)); // - 3 for begin, end, bin
            break;
        case OP_MAP_REMOVE_BY_VALUE_REL_RANK_RANGE_TO_END:
            if (get_int64_t(err, AS_PY_MAP_RETURN_KEY, temp_expr->pydict,
                            &lval1) != AEROSPIKE_OK) {
                return err->code;
            }
            APPEND_ARRAY(3, as_exp_map_remove_by_value_rel_rank_range_to_end(
                                temp_expr->ctx, lval1, NIL, NIL,
                                NIL)); // - 3 for val, rank, bin
            break;
        case OP_MAP_REMOVE_BY_VALUE_REL_RANK_RANGE:
            if (get_int64_t(err, AS_PY_MAP_RETURN_KEY, temp_expr->pydict,
                            &lval1) != AEROSPIKE_OK) {
                return err->code;
            }
            APPEND_ARRAY(4, as_exp_map_remove_by_value_rel_rank_range(
                                temp_expr->ctx, lval1, NIL, NIL, NIL,
                                NIL)); // - 4 for val, rank, count, bin
            break;
        case OP_MAP_REMOVE_BY_INDEX:
            APPEND_ARRAY(2,
                         as_exp_map_remove_by_index(temp_expr->ctx, NIL,
                                                    NIL)); // - 2 for index, bin
            break;
        case OP_MAP_REMOVE_BY_INDEX_RANGE_TO_END:
            if (get_int64_t(err, AS_PY_MAP_RETURN_KEY, temp_expr->pydict,
                            &lval1) != AEROSPIKE_OK) {
                return err->code;
            }
            APPEND_ARRAY(2, as_exp_map_remove_by_index_range_to_end(
                                temp_expr->ctx, lval1, NIL,
                                NIL)); // - 2 for index, bin
            break;
        case OP_MAP_REMOVE_BY_INDEX_RANGE:
            if (get_int64_t(err, AS_PY_MAP_RETURN_KEY, temp_expr->pydict,
                            &lval1) != AEROSPIKE_OK) {
                return err->code;
            }
            APPEND_ARRAY(3, as_exp_map_remove_by_index_range(
                                temp_expr->ctx, lval1, NIL, NIL,
                                NIL)); // - 3 for index, count, bin
            break;
        case OP_MAP_REMOVE_BY_RANK:
            APPEND_ARRAY(2,
                         as_exp_map_remove_by_rank(temp_expr->ctx, NIL,
                                                   NIL)); // - 2 for rank, bin
            break;
        case OP_MAP_REMOVE_BY_RANK_RANGE_TO_END:
            if (get_int64_t(err, AS_PY_MAP_RETURN_KEY, temp_expr->pydict,
                            &lval1) != AEROSPIKE_OK) {
                return err->code;
            }
            APPEND_ARRAY(2, as_exp_map_remove_by_rank_range_to_end(
                                temp_expr->ctx, lval1, NIL,
                                NIL)); // - 2 for rank, bin
            break;
        case OP_MAP_REMOVE_BY_RANK_RANGE:
            if (get_int64_t(err, AS_PY_MAP_RETURN_KEY, temp_expr->pydict,
                            &lval1) != AEROSPIKE_OK) {
                return err->code;
            }
            APPEND_ARRAY(3, as_exp_map_remove_by_rank_range(
                                temp_expr->ctx, lval1, NIL, NIL,
                                NIL)); // - 3 for rank, count, bin
            break;
        case OP_MAP_SIZE:
            APPEND_ARRAY(1, as_exp_map_size(temp_expr->ctx,
                                            NIL)); // - 1 for bin
            break;
        case OP_MAP_GET_BY_KEY:
            if (get_int64_t(err, AS_PY_MAP_RETURN_KEY, temp_expr->pydict,
                            &lval1) != AEROSPIKE_OK) {
                return err->code;
            }

            if (get_int64_t(err, AS_PY_VALUE_TYPE_KEY, temp_expr->pydict,
                            &lval2) != AEROSPIKE_OK) {
                return err->code;
            }

            APPEND_ARRAY(2, as_exp_map_get_by_key(temp_expr->ctx, lval1, lval2,
                                                  NIL,
                                                  NIL)); // - 2 for key, bin
            break;
        case OP_MAP_GET_BY_KEY_RANGE:
            if (get_int64_t(err, AS_PY_MAP_RETURN_KEY, temp_expr->pydict,
                            &lval1) != AEROSPIKE_OK) {
                return err->code;
            }

            APPEND_ARRAY(
                3, as_exp_map_get_by_key_range(temp_expr->ctx, lval1, NIL, NIL,
                                               NIL)); // - 3 for begin, end, bin
            break;
        case OP_MAP_GET_BY_KEY_LIST:
            if (get_int64_t(err, AS_PY_MAP_RETURN_KEY, temp_expr->pydict,
                            &lval1) != AEROSPIKE_OK) {
                return err->code;
            }

            APPEND_ARRAY(2,
                         as_exp_map_get_by_key_list(temp_expr->ctx, lval1, NIL,
                                                    NIL)); // - 2 for keys, bin
            break;
        case OP_MAP_GET_BY_KEY_REL_INDEX_RANGE_TO_END:
            if (get_int64_t(err, AS_PY_MAP_RETURN_KEY, temp_expr->pydict,
                            &lval1) != AEROSPIKE_OK) {
                return err->code;
            }

            APPEND_ARRAY(3, as_exp_map_get_by_key_rel_index_range_to_end(
                                temp_expr->ctx, lval1, NIL, NIL,
                                NIL)); // - 3 for key, index, bin
            break;
        case OP_MAP_GET_BY_KEY_REL_INDEX_RANGE:
            if (get_int64_t(err, AS_PY_MAP_RETURN_KEY, temp_expr->pydict,
                            &lval1) != AEROSPIKE_OK) {
                return err->code;
            }

            APPEND_ARRAY(4, as_exp_map_get_by_key_rel_index_range(
                                temp_expr->ctx, lval1, NIL, NIL, NIL,
                                NIL)); // - 4 for key, index, count, bin
            break;
        case OP_MAP_GET_BY_VALUE:
            if (get_int64_t(err, AS_PY_MAP_RETURN_KEY, temp_expr->pydict,
                            &lval1) != AEROSPIKE_OK) {
                return err->code;
            }

            APPEND_ARRAY(2, as_exp_map_get_by_value(temp_expr->ctx, lval1, NIL,
                                                    NIL)); // - 2 for value, bin
            break;
        case OP_MAP_GET_BY_VALUE_RANGE:
            if (get_int64_t(err, AS_PY_MAP_RETURN_KEY, temp_expr->pydict,
                            &lval1) != AEROSPIKE_OK) {
                return err->code;
            }

            APPEND_ARRAY(3, as_exp_map_get_by_value_range(
                                temp_expr->ctx, lval1, NIL, NIL,
                                NIL)); // - 3 for begin, end, bin
            break;
        case OP_MAP_GET_BY_VALUE_LIST:
            if (get_int64_t(err, AS_PY_MAP_RETURN_KEY, temp_expr->pydict,
                            &lval1) != AEROSPIKE_OK) {
                return err->code;
            }

            APPEND_ARRAY(
                2, as_exp_map_get_by_value_list(temp_expr->ctx, lval1, NIL,
                                                NIL)); // - 2 for value, bin
            break;
        case OP_MAP_GET_BY_VALUE_RANK_RANGE_REL_TO_END:
            if (get_int64_t(err, AS_PY_MAP_RETURN_KEY, temp_expr->pydict,
                            &lval1) != AEROSPIKE_OK) {
                return err->code;
            }

            APPEND_ARRAY(3, as_exp_map_get_by_value_rel_rank_range_to_end(
                                temp_expr->ctx, lval1, NIL, NIL,
                                NIL)); // - 3 for value, rank, bin
            break;
        case OP_MAP_GET_BY_VALUE_RANK_RANGE_REL:
            if (get_int64_t(err, AS_PY_MAP_RETURN_KEY, temp_expr->pydict,
                            &lval1) != AEROSPIKE_OK) {
                return err->code;
            }

            APPEND_ARRAY(4, as_exp_map_get_by_value_rel_rank_range(
                                temp_expr->ctx, lval1, NIL, NIL, NIL,
                                NIL)); // - 4 for value, rank, count, bin
            break;
        case OP_MAP_GET_BY_INDEX:
            if (get_int64_t(err, AS_PY_MAP_RETURN_KEY, temp_expr->pydict,
                            &lval1) != AEROSPIKE_OK) {
                return err->code;
            }

            if (get_int64_t(err, AS_PY_VALUE_TYPE_KEY, temp_expr->pydict,
                            &lval2) != AEROSPIKE_OK) {
                return err->code;
            }

            APPEND_ARRAY(2, as_exp_map_get_by_index(temp_expr->ctx, lval1,
                                                    lval2, NIL,
                                                    NIL)); // - 2 for index, bin
            break;
        case OP_MAP_GET_BY_INDEX_RANGE_TO_END:
            if (get_int64_t(err, AS_PY_MAP_RETURN_KEY, temp_expr->pydict,
                            &lval1) != AEROSPIKE_OK) {
                return err->code;
            }

            APPEND_ARRAY(2, as_exp_map_get_by_index_range_to_end(
                                temp_expr->ctx, lval1, NIL,
                                NIL)); // - 2 for index, bin
            break;
        case OP_MAP_GET_BY_INDEX_RANGE:
            if (get_int64_t(err, AS_PY_MAP_RETURN_KEY, temp_expr->pydict,
                            &lval1) != AEROSPIKE_OK) {
                return err->code;
            }

            APPEND_ARRAY(3, as_exp_map_get_by_index_range(
                                temp_expr->ctx, lval1, NIL, NIL,
                                NIL)); // - 3 for index, count, bin
            break;
        case OP_MAP_GET_BY_RANK:
            if (get_int64_t(err, AS_PY_MAP_RETURN_KEY, temp_expr->pydict,
                            &lval1) != AEROSPIKE_OK) {
                return err->code;
            }

            if (get_int64_t(err, AS_PY_VALUE_TYPE_KEY, temp_expr->pydict,
                            &lval2) != AEROSPIKE_OK) {
                return err->code;
            }

            APPEND_ARRAY(2, as_exp_map_get_by_rank(temp_expr->ctx, lval1, lval2,
                                                   NIL,
                                                   NIL)); // - 2 for rank, bin
            break;
        case OP_MAP_GET_BY_RANK_RANGE_TO_END:
            if (get_int64_t(err, AS_PY_MAP_RETURN_KEY, temp_expr->pydict,
                            &lval1) != AEROSPIKE_OK) {
                return err->code;
            }

            APPEND_ARRAY(2, as_exp_map_get_by_rank_range_to_end(
                                temp_expr->ctx, lval1, NIL,
                                NIL)); // - 2 for rank, bin
            break;
        case OP_MAP_GET_BY_RANK_RANGE:
            if (get_int64_t(err, AS_PY_MAP_RETURN_KEY, temp_expr->pydict,
                            &lval1) != AEROSPIKE_OK) {
                return err->code;
            }

            APPEND_ARRAY(3, as_exp_map_get_by_rank_range(
                                temp_expr->ctx, lval1, NIL, NIL,
                                NIL)); // - 3 for rank, count, bin
            break;
        case _AS_EXP_BIT_FLAGS:
            if (get_int64_t(err, AS_PY_VAL_KEY, temp_expr->pydict, &lval1) !=
                AEROSPIKE_OK) {
                return err->code;
            }

            APPEND_ARRAY(0, as_exp_uint((uint64_t)lval1));
            break;
        case OP_BIT_RESIZE:
            APPEND_ARRAY(4, as_exp_bit_resize(
                                NULL, NIL, NO_BIT_FLAGS,
                                NIL)); // - 4 for byte_size, policy, flags, bin
            break;
        case OP_BIT_INSERT:
            APPEND_ARRAY(4, as_exp_bit_insert(NULL, NIL, NIL, NIL));
            break;
        case OP_BIT_REMOVE:
            APPEND_ARRAY(4, as_exp_bit_remove(NULL, NIL, NIL, NIL));
            break;
        case OP_BIT_SET:
            APPEND_ARRAY(5, as_exp_bit_set(NULL, NIL, NIL, NIL, NIL));
            break;
        case OP_BIT_OR:
            APPEND_ARRAY(5, as_exp_bit_or(NULL, NIL, NIL, NIL, NIL));
            break;
        case OP_BIT_XOR:
            APPEND_ARRAY(5, as_exp_bit_xor(NULL, NIL, NIL, NIL, NIL));
            break;
        case OP_BIT_AND:
            APPEND_ARRAY(5, as_exp_bit_and(NULL, NIL, NIL, NIL, NIL));
            break;
        case OP_BIT_NOT:
            APPEND_ARRAY(4, as_exp_bit_not(NULL, NIL, NIL, NIL));
            break;
        case OP_BIT_LSHIFT:
            APPEND_ARRAY(5, as_exp_bit_lshift(NULL, NIL, NIL, NIL, NIL));
            break;
        case OP_BIT_RSHIFT:
            APPEND_ARRAY(5, as_exp_bit_rshift(NULL, NIL, NIL, NIL, NIL));
            break;
        case OP_BIT_ADD:
            APPEND_ARRAY(
                6, as_exp_bit_add(NULL, NIL, NIL, NIL, NO_BIT_FLAGS, NIL));
            break;
        case OP_BIT_SUBTRACT:
            APPEND_ARRAY(
                6, as_exp_bit_subtract(NULL, NIL, NIL, NIL, NO_BIT_FLAGS, NIL));
            break;
        case OP_BIT_SET_INT:
            APPEND_ARRAY(5, as_exp_bit_set_int(NULL, NIL, NIL, NIL, NIL));
            break;
        case OP_BIT_GET:
            APPEND_ARRAY(
                3, as_exp_bit_get(NIL, NIL,
                                  NIL)); // - 3 for bit_offset, bit_size, bin
            break;
        case OP_BIT_COUNT:
            APPEND_ARRAY(
                3, as_exp_bit_count(NIL, NIL,
                                    NIL)); // - 3 for bit_offset, bit_size, bin
            break;
        case OP_BIT_LSCAN:
            APPEND_ARRAY(4, as_exp_bit_lscan(NIL, NIL, NIL, NIL));
            break;
        case OP_BIT_RSCAN:
            APPEND_ARRAY(4, as_exp_bit_rscan(NIL, NIL, NIL, NIL));
            break;
        case OP_BIT_GET_INT:
            APPEND_ARRAY(4, as_exp_bit_get_int(NIL, NIL, 0, NIL));
            break;
        case OP_HLL_INIT: // NOTE: this case covers HLLInit and HLLInitMH.
            APPEND_ARRAY(
                4,
                as_exp_hll_init_mh(
                    NULL, 0, 0,
                    NIL)); // - 4 for index_bit_count, mh_bit_count, policy, bin
            break;
        case OP_HLL_ADD: // NOTE: this case covers HLLAddMH, HLLAdd, and HLLUpdate
            APPEND_ARRAY(
                5, as_exp_hll_add_mh(
                       NULL, NIL, 0, 0,
                       NIL)); // - 5 for list, index_bit_count, -1, policy, bin
            break;
        case OP_HLL_GET_COUNT:
            APPEND_ARRAY(1, as_exp_hll_get_count(NIL)); // - 1 for bin
            break;
        case OP_HLL_GET_UNION:
            APPEND_ARRAY(2, as_exp_hll_get_union(NIL,
                                                 NIL)); // - 2 for list, bin
            break;
        case OP_HLL_GET_UNION_COUNT:
            APPEND_ARRAY(
                2, as_exp_hll_get_union_count(NIL, NIL)); // - 2 for list, bin
            break;
        case OP_HLL_GET_INTERSECT_COUNT:
            APPEND_ARRAY(2, as_exp_hll_get_intersect_count(
                                NIL, NIL)); // - 2 for list, bin
            break;
        case OP_HLL_GET_SIMILARITY:
            APPEND_ARRAY(
                2, as_exp_hll_get_similarity(NIL, NIL)); // - 2 for list, bin
            break;
        case OP_HLL_DESCRIBE:
            APPEND_ARRAY(1, as_exp_hll_describe(NIL)); // - 1 for bin
            break;
        case OP_HLL_MAY_CONTAIN:
            APPEND_ARRAY(2, as_exp_hll_may_contain(NIL,
                                                   NIL)); // - 2 for list, bin
            break;
        case EXCLUSIVE:
            APPEND_ARRAY(
                2,
                as_exp_exclusive(
                    NIL)); // - 2 for va_args, AS_EXP_CODE_END_OF_VA_ARGS
            break;
        case ADD:
            APPEND_ARRAY(
                2,
                as_exp_add(NIL)); // - 2 for va_args, AS_EXP_CODE_END_OF_VA_ARGS
            break;
        case SUB:
            APPEND_ARRAY(
                2,
                as_exp_sub(NIL)); // - 2 for va_args, AS_EXP_CODE_END_OF_VA_ARGS
            break;
        case MUL:
            APPEND_ARRAY(
                2,
                as_exp_mul(NIL)); // - 2 for va_args, AS_EXP_CODE_END_OF_VA_ARGS
            break;
        case DIV:
            APPEND_ARRAY(
                2,
                as_exp_div(NIL)); // - 2 for va_args, AS_EXP_CODE_END_OF_VA_ARGS
            break;
        case POW:
            APPEND_ARRAY(2, as_exp_pow(NIL,
                                       NIL)); // - 2 for __base, __exponent
            break;
        case LOG:
            APPEND_ARRAY(2, as_exp_log(NIL,
                                       NIL)); // - 2 for __base, __base
            break;
        case MOD:
            APPEND_ARRAY(2,
                         as_exp_mod(NIL,
                                    NIL)); // - 2 for __numerator, __denominator
            break;
        case ABS:
            APPEND_ARRAY(1, as_exp_abs(NIL)); // - 1 for __value
            break;
        case FLOOR:
            APPEND_ARRAY(1, as_exp_floor(NIL)); // - 1 for __num
            break;
        case CEIL:
            APPEND_ARRAY(1, as_exp_ceil(NIL)); // - 1 for __num
            break;
        case TO_INT:
            APPEND_ARRAY(1, as_exp_to_int(NIL)); // - 1 for __num
            break;
        case TO_FLOAT:
            APPEND_ARRAY(1, as_exp_to_float(NIL)); // - 1 for __num
            break;
        case INT_AND:
            APPEND_ARRAY(
                2,
                as_exp_int_and(
                    NIL)); // - 2 for va_args, AS_EXP_CODE_END_OF_VA_ARGS
            break;
        case INT_OR:
            APPEND_ARRAY(
                2,
                as_exp_int_or(
                    NIL)); // - 2 for va_args, AS_EXP_CODE_END_OF_VA_ARGS
            break;
        case INT_XOR:
            APPEND_ARRAY(
                2,
                as_exp_int_xor(
                    NIL)); // - 2 for va_args, AS_EXP_CODE_END_OF_VA_ARGS
            break;
        case INT_NOT:
            APPEND_ARRAY(1, as_exp_int_not(NIL)); // - 1 for __expr
            break;
        case INT_LSHIFT:
            APPEND_ARRAY(2, as_exp_int_lshift(NIL,
                                              NIL)); // - 2 for __value, __shift
            break;
        case INT_RSHIFT:
            APPEND_ARRAY(2, as_exp_int_rshift(NIL,
                                              NIL)); // - 2 for __value, __shift
            break;
        case INT_ARSHIFT:
            APPEND_ARRAY(
                2, as_exp_int_arshift(NIL, NIL)); // - 2 for __value, __shift
            break;
        case INT_COUNT:
            APPEND_ARRAY(1, as_exp_int_count(NIL)); // - 1 for __expr
            break;
        case INT_LSCAN:
            APPEND_ARRAY(2, as_exp_int_lscan(NIL,
                                             NIL)); // - 2 for __value, __search
            break;
        case INT_RSCAN:
            APPEND_ARRAY(2, as_exp_int_rscan(NIL,
                                             NIL)); // - 2 for __value, __search
            break;
        case MIN:
            APPEND_ARRAY(
                2,
                as_exp_min(NIL)); // - 2 for va_args, AS_EXP_CODE_END_OF_VA_ARGS
            break;
        case MAX:
            APPEND_ARRAY(
                2,
                as_exp_max(NIL)); // - 2 for va_args, AS_EXP_CODE_END_OF_VA_ARGS
            break;
        case COND:
            APPEND_ARRAY(
                2,
                as_exp_cond(
                    NIL)); // - 2 for va_args, AS_EXP_CODE_END_OF_VA_ARGS
            break;
        case LET:
            APPEND_ARRAY(
                2,
                as_exp_let(NIL)); // - 2 for va_args, AS_EXP_CODE_END_OF_VA_ARGS
            break;
        case DEF:;
            py_val_from_dict =
                PyDict_GetItemString(temp_expr->pydict, AS_PY_VAL_KEY);
            const char *def_var_name = NULL;
            if (PyUnicode_Check(py_val_from_dict)) {
                def_var_name = PyUnicode_AsUTF8(py_val_from_dict);
            }
            else {
                return as_error_update(err, AEROSPIKE_ERR_PARAM,
                                       "regex_str must be a string.");
            }

            APPEND_ARRAY(1, as_exp_def(def_var_name, NIL)); // - 1 for __expr
            break;
        case VAR:;
            py_val_from_dict =
                PyDict_GetItemString(temp_expr->pydict, AS_PY_VAL_KEY);
            const char *var_name = NULL;
            if (PyUnicode_Check(py_val_from_dict)) {
                var_name = PyUnicode_AsUTF8(py_val_from_dict);
            }
            else {
                return as_error_update(err, AEROSPIKE_ERR_PARAM,
                                       "regex_str must be a string.");
            }

            APPEND_ARRAY(0, as_exp_var(var_name));
            break;
        case UNKNOWN:
            APPEND_ARRAY(0, as_exp_unknown());
            break;
        default:
            return as_error_update(err, AEROSPIKE_ERR_PARAM,
                                   "Unrecognised expression op type.");
            break;
        }
    }

    return err->code;
}

/*
* convert_exp_list
* Converts expressions from python into intermediate_expr structs.
* Initiates the conversion from intermediate_expr structs to expressions.
* builds the expressions.
*/
as_status convert_exp_list(AerospikeClient *self, PyObject *py_exp_list,
                           as_exp **exp_list, as_error *err)
{
    int bottom = 0;

    if (py_exp_list == NULL || !PyList_Check(py_exp_list)) {
        as_error_update(err, AEROSPIKE_ERR_PARAM,
                        "Expressions must be a non empty list of 4 element "
                        "tuples, generated by a compiled aerospike expression");
        return err->code;
    }

    Py_ssize_t size = PyList_Size(py_exp_list);
    if (size <= 0) {
        as_error_update(err, AEROSPIKE_ERR_PARAM,
                        "Expressions must be a non empty list of 4 element "
                        "tuples, generated by a compiled aerospike expression");
        return err->code;
    }

    int processed_exp_count = 0;
    int size_to_alloc = 0;
    bool ctx_in_use = false;
    PyObject *py_expr_tuple = NULL;
    PyObject *py_list_policy_p = NULL;
    PyObject *py_map_policy_p = NULL;
    PyObject *py_ctx_list_p = NULL;
    as_vector intermediate_expr_queue;
    intermediate_expr temp_expr;
    as_exp_entry *c_expr_entries = NULL;

    as_vector *unicodeStrVector = as_vector_create(sizeof(char *), 128);
    as_vector_inita(&intermediate_expr_queue, sizeof(intermediate_expr), size);

    as_static_pool static_pool;
    memset(&static_pool, 0, sizeof(static_pool));

    // Flags in case we need to deallocate temp expr while it is being built
    bool is_building_temp_expr = true;
    bool is_ctx_initialized = false;

    for (int i = 0; i < size; ++i) {
        memset(&temp_expr, 0, sizeof(intermediate_expr));
        ctx_in_use = false;
        // Reset flag for next temp expr being built
        is_ctx_initialized = false;

        py_expr_tuple = PyList_GetItem(py_exp_list, (Py_ssize_t)i);
        if (!PyTuple_Check(py_expr_tuple) || PyTuple_Size(py_expr_tuple) != 4) {
            as_error_update(
                err, AEROSPIKE_ERR_PARAM,
                "Expressions must be a non empty list of 4 element tuples, "
                "generated by a compiled aerospike expression");
            goto CLEANUP;
        }

        temp_expr.pytuple = py_expr_tuple;
        temp_expr.op = PyLong_AsLong(PyTuple_GetItem(py_expr_tuple, 0));
        if (temp_expr.op == -1 && PyErr_Occurred()) {
            as_error_update(
                err, AEROSPIKE_ERR_PARAM,
                "Failed to get op from expression tuple, op must be an int.");
            goto CLEANUP;
        }

        PyObject *rt_tmp = PyTuple_GetItem(py_expr_tuple, 1);
        if (rt_tmp != Py_None) {
            temp_expr.result_type = PyLong_AsLong(rt_tmp);
            if (temp_expr.result_type == -1 && PyErr_Occurred()) {
                as_error_update(err, AEROSPIKE_ERR_PARAM,
                                "Failed to get result_type from expression "
                                "tuple, rt must be an int.");
                goto CLEANUP;
            }
        }

        temp_expr.pydict = PyTuple_GetItem(py_expr_tuple, 2);
        if (temp_expr.pydict != Py_None) {
            if (!PyDict_Check(temp_expr.pydict)) {
                as_error_update(err, AEROSPIKE_ERR_PARAM,
                                "Failed to get fixed dictionary from "
                                "expression tuple, fixed must be a dict.");
                goto CLEANUP;
            }
        }

        //TODO Is ctx/list_policy/map_policy allocation and parsing necessary here?
        //TODO Could it be moved somewhere else?
        py_ctx_list_p = PyDict_GetItemString(temp_expr.pydict, CTX_KEY);
        if (py_ctx_list_p != NULL) {
            temp_expr.ctx = malloc(sizeof(as_cdt_ctx));
            if (temp_expr.ctx == NULL) {
                as_error_update(err, AEROSPIKE_ERR,
                                "Could not malloc mem for temp_expr.ctx.");
                goto CLEANUP;
            }

            if (get_cdt_ctx(self, err, temp_expr.ctx, temp_expr.pydict,
                            &ctx_in_use, &static_pool,
                            SERIALIZER_PYTHON) != AEROSPIKE_OK) {
                goto CLEANUP;
            }
        }
        is_ctx_initialized = true;

        py_list_policy_p =
            PyDict_GetItemString(temp_expr.pydict, AS_PY_LIST_POLICY);
        if (py_list_policy_p != NULL) {
            if (PyDict_Check(py_list_policy_p) &&
                PyDict_Size(py_list_policy_p) > 0) {
                temp_expr.list_policy = malloc(sizeof(as_list_policy));
                if (temp_expr.list_policy == NULL) {
                    as_error_update(
                        err, AEROSPIKE_ERR,
                        "Could not malloc mem for temp_expr.list_policy.");
                    goto CLEANUP;
                }

                bool policy_in_use = false;
                if (get_list_policy(err, temp_expr.pydict,
                                    temp_expr.list_policy,
                                    &policy_in_use) != AEROSPIKE_OK) {
                    temp_expr.list_policy = NULL;
                    goto CLEANUP;
                }
            }
        }

        py_map_policy_p =
            PyDict_GetItemString(temp_expr.pydict, AS_PY_MAP_POLICY);
        if (py_map_policy_p != NULL) {
            if (PyDict_Check(py_map_policy_p) &&
                PyDict_Size(py_map_policy_p) > 0) {
                temp_expr.map_policy = malloc(sizeof(as_map_policy));
                if (temp_expr.map_policy == NULL) {
                    as_error_update(
                        err, AEROSPIKE_ERR,
                        "Could not malloc mem for temp_expr.map_policy.");
                    goto CLEANUP;
                }

                if (pyobject_to_map_policy(err, py_map_policy_p,
                                           temp_expr.map_policy) !=
                    AEROSPIKE_OK) {
                    temp_expr.map_policy = NULL;
                    goto CLEANUP;
                }
            }
        }

        temp_expr.num_children =
            PyLong_AsLong(PyTuple_GetItem(py_expr_tuple, 3));
        if (temp_expr.num_children == -1 && PyErr_Occurred()) {
            as_error_update(err, AEROSPIKE_ERR_PARAM,
                            "Failed to get num_children from expression tuple, "
                            "num_children must be an int.");
            goto CLEANUP;
        }

        as_vector_append(&intermediate_expr_queue, (void *)&temp_expr);
        processed_exp_count++;
    }

    is_building_temp_expr = false;

    if (get_expr_size(&size_to_alloc, (int *)&size, &intermediate_expr_queue,
                      err) != AEROSPIKE_OK) {
        goto CLEANUP;
    }

    c_expr_entries = (as_exp_entry *)malloc(size_to_alloc);
    if (c_expr_entries == NULL) {
        as_error_update(err, AEROSPIKE_ERR,
                        "Could not malloc mem for c_expr_entries.");
        goto CLEANUP;
    }

    if (add_expr_macros(self, &static_pool, SERIALIZER_PYTHON, unicodeStrVector,
                        &intermediate_expr_queue, &c_expr_entries, &bottom,
                        (int *)&size, err) != AEROSPIKE_OK) {
        goto CLEANUP;
    }

    *exp_list = as_exp_compile(c_expr_entries, bottom);
CLEANUP:
    if (is_building_temp_expr) {
        bool success = free_temp_expr(&temp_expr, err, is_ctx_initialized);
        if (!success) {
            return err->code;
        }
    }

    for (int i = 0; i < processed_exp_count; ++i) {
        intermediate_expr *temp_expr = (intermediate_expr *)as_vector_get(
            &intermediate_expr_queue, (uint32_t)i);
        bool success = free_temp_expr(temp_expr, err, true);
        if (!success) {
            return err->code;
        }
    }

    as_vector_destroy(&intermediate_expr_queue);
    if (c_expr_entries != NULL) {
        free(c_expr_entries);
    }

    POOL_DESTROY(&static_pool);
    as_vector_destroy(unicodeStrVector);
    return err->code;
}

// Returns true if successful, false if not
static bool free_temp_expr(intermediate_expr *temp_expr, as_error *err,
                           bool is_ctx_initialized)
{
    if (temp_expr == NULL) {
        return true;
    }

    if (temp_expr->list_policy != NULL) {
        free(temp_expr->list_policy);
    }

    if (temp_expr->map_policy != NULL) {
        free(temp_expr->map_policy);
    }

    if (temp_expr->ctx != NULL) {
        if (is_ctx_initialized) {
            as_cdt_ctx_destroy(temp_expr->ctx);
        }
        free(temp_expr->ctx);
    }

    switch (temp_expr->val_flag) {
    case 0:
        break;
    case VAL_STRING_P_ACTIVE:
        free(temp_expr->val.val_string_p);
        break;
    case VAL_LIST_P_ACTIVE:
        as_list_destroy(temp_expr->val.val_list_p);
        break;
    case VAL_MAP_P_ACTIVE:
        as_map_destroy(temp_expr->val.val_map_p);
        break;
    default:
        as_error_update(err, AEROSPIKE_ERR, "Unexpected val_flag %u.",
                        temp_expr->val_flag);
        return false;
    }
    return true;
}
