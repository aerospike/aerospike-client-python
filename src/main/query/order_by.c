/*******************************************************************************
 * Copyright 2013-2026 Aerospike, Inc.
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

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <limits.h>
#include <stdbool.h>
#include <string.h>

#include <aerospike/as_bin.h>
#include <aerospike/as_error.h>
#include <aerospike/as_operations.h>
#include <aerospike/as_query.h>

#include "client.h"
#include "exceptions.h"
#include "query.h"

static const char BIN_NAME_ERROR[] =
    "Top-K order-by bin name must be 1-15 UTF-8 bytes without NUL";
static const char TYPE_REQUIRED_ERROR[] =
    "orderBy requires a type declaration (INTEGER, DOUBLE, STRING, or BYTES); "
    "Aerospike has no schema";
static const char DIRECTION_ERROR[] = "orderBy direction must be ASC or DESC";
static const char FLAGS_ERROR[] =
    "orderBy flags are only valid with type STRING";
static const char TOP_K_RANGE_ERROR[] = "topK must be in [1, 1000]; got %u";
static const char MAX_RECORDS_ERROR[] = "Top-K is incompatible with maxRecords";
static const char BACKGROUND_ERROR[] =
    "orderBy/topK is only valid on foreground (SELECT-style) queries; "
    "background (UPDATE-style) queries do not produce a result stream";
static const char AGGREGATE_ERROR[] =
    "orderBy is incompatible with aggregate UDFs; use the UDF's own reduce "
    "stage";
static const char RESUME_ERROR[] =
    "Top-K can't resume a partition filter from an earlier query";
static const char PROJECTION_MISSING_ERROR[] =
    "orderBy bin '%s' is not in projection; add it to setOperations or remove "
    "projection";
static const char PROJECTION_DUPLICATE_ERROR[] =
    "orderBy bin '%s' is produced by more than one operation";
static const char ORDER_KEY_COUNT_ERROR[] = "orderBy supports at most two keys";

typedef void (*order_key_setter)(as_query *query, const char *bin_name,
                                 as_query_order_by_type type,
                                 as_order direction,
                                 as_query_order_by_flags flags);

static bool is_order_by_type(unsigned int type_value)
{
    if (type_value == AS_QUERY_ORDER_BY_INTEGER) {
        return true;
    }
    if (type_value == AS_QUERY_ORDER_BY_DOUBLE) {
        return true;
    }
    if (type_value == AS_QUERY_ORDER_BY_STRING) {
        return true;
    }
    if (type_value == AS_QUERY_ORDER_BY_BYTES) {
        return true;
    }
    return false;
}

static bool partition_status_has_resume(const as_partitions_status *parts)
{
    if (parts == NULL) {
        return false;
    }

    uint16_t part_count = parts->part_count;
    for (uint16_t i = 0; i < part_count; i++) {
        const as_partition_status *part = &parts->parts[i];
        if (part->digest.init || part->bval != 0) {
            return true;
        }
    }
    return false;
}

static as_status
validate_order_key_projection(const as_query *query,
                              const as_query_order_by_field *key, as_error *err)
{
    const char *bin_name = key->bin;

    if (as_operations_defined(query->ops)) {
        uint16_t matches = 0;
        as_operations *ops = query->ops;
        uint16_t op_count = ops->binops.size;
        for (uint16_t i = 0; i < op_count; i++) {
            const char *op_bin = ops->binops.entries[i].bin.name;
            if (strcmp(op_bin, bin_name) == 0) {
                matches++;
            }
        }

        if (matches > 1) {
            as_error_update(err, AEROSPIKE_ERR_PARAM,
                            PROJECTION_DUPLICATE_ERROR, bin_name);
            return err->code;
        }
        if (matches == 0) {
            as_error_update(err, AEROSPIKE_ERR_PARAM, PROJECTION_MISSING_ERROR,
                            bin_name);
            return err->code;
        }
        return AEROSPIKE_OK;
    }

    if (query->select.size == 0) {
        return AEROSPIKE_OK;
    }

    bool found = false;
    uint16_t select_size = query->select.size;
    for (uint16_t i = 0; i < select_size; i++) {
        if (strcmp(query->select.entries[i], bin_name) == 0) {
            found = true;
            break;
        }
    }
    if (!found) {
        as_error_update(err, AEROSPIKE_ERR_PARAM, PROJECTION_MISSING_ERROR,
                        bin_name);
        return err->code;
    }
    return AEROSPIKE_OK;
}

as_status
AerospikeQuery_ValidateTopK(AerospikeQuery *self, as_error *err,
                            bool is_background,
                            const as_partition_filter *partition_filter,
                            const as_partitions_status *partition_status)
{
    bool order_by_set = self->query.order_by.defined;
    uint32_t top_k = self->query.top_k;
    if (!order_by_set && top_k == 0) {
        return AEROSPIKE_OK;
    }

    if (is_background) {
        as_error_update(err, AEROSPIKE_ERR_PARAM, "%s", BACKGROUND_ERROR);
        return err->code;
    }

    // as_query_then_order_by() sets this instead of storing a third key.
    if (self->query.order_by_overflow) {
        as_error_update(err, AEROSPIKE_ERR_PARAM, "%s", ORDER_KEY_COUNT_ERROR);
        return err->code;
    }

    // top_k == 0 means k is unset. Like the C client, an order key without k is
    // sent to the server, which rejects it.
    if (top_k > 1000) {
        as_error_update(err, AEROSPIKE_ERR_PARAM, TOP_K_RANGE_ERROR, top_k);
        return err->code;
    }

    if (self->query.max_records > 0) {
        as_error_update(err, AEROSPIKE_ERR_PARAM, "%s", MAX_RECORDS_ERROR);
        return err->code;
    }

    bool has_aggregate = self->query.apply.function[0] != '\0';
    if (has_aggregate) {
        as_error_update(err, AEROSPIKE_ERR_PARAM, "%s", AGGREGATE_ERROR);
        return err->code;
    }

    bool has_resume = partition_status_has_resume(self->query.parts_all);
    if (!has_resume && partition_filter != NULL) {
        has_resume = partition_filter->digest.init;
    }
    if (!has_resume) {
        has_resume = partition_status_has_resume(partition_status);
    }
    if (has_resume) {
        as_error_update(err, AEROSPIKE_ERR_PARAM, "%s", RESUME_ERROR);
        return err->code;
    }

    if (!order_by_set) {
        return AEROSPIKE_OK;
    }

    as_status projection_status =
        validate_order_key_projection(&self->query, &self->query.order_by, err);
    if (projection_status != AEROSPIKE_OK) {
        return projection_status;
    }

    if (!self->query.then_order_by.defined) {
        return AEROSPIKE_OK;
    }

    as_status then_projection_status = validate_order_key_projection(
        &self->query, &self->query.then_order_by, err);
    return then_projection_status;
}

// Shared by order_by() and then_order_by(). format is the
// PyArg_ParseTupleAndKeywords format, which names the method in TypeErrors.
static AerospikeQuery *set_order_key(AerospikeQuery *self, PyObject *args,
                                     PyObject *kwds, const char *format,
                                     order_key_setter setter)
{
    // type and direction are parsed as optional so that omitting them raises
    // the ParamError messages from the Top-K API instead of a TypeError.
    const unsigned int unset = UINT_MAX;
    char *bin_name = NULL;
    Py_ssize_t bin_size = 0;
    unsigned int type = unset;
    unsigned int direction = unset;
    unsigned int flags = AS_QUERY_ORDER_BY_FLAGS_DEFAULT;

    static char *kwlist[] = {"bin", "type", "direction", "flags", NULL};

    as_error err;
    as_error_init(&err);

    if (!self || (self->client && !self->client->as)) {
        as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
        goto CLEANUP;
    }

    if (!PyArg_ParseTupleAndKeywords(args, kwds, format, kwlist, &bin_name,
                                     &bin_size, &type, &direction, &flags)) {
        return NULL;
    }

    size_t bin_len = strlen(bin_name);
    bool bin_ok = bin_len == (size_t)bin_size && bin_len >= 1 &&
                  bin_len <= AS_BIN_NAME_MAX_LEN;
    if (!bin_ok) {
        as_error_update(&err, AEROSPIKE_ERR_PARAM, "%s", BIN_NAME_ERROR);
        goto CLEANUP;
    }

    if (type == unset) {
        as_error_update(&err, AEROSPIKE_ERR_PARAM, "%s", TYPE_REQUIRED_ERROR);
        goto CLEANUP;
    }

    if (!is_order_by_type(type)) {
        as_error_update(&err, AEROSPIKE_ERR_PARAM,
                        "orderBy type must be one of INTEGER, DOUBLE, STRING, "
                        "BYTES; got %u",
                        type);
        goto CLEANUP;
    }

    bool direction_ok =
        direction == AS_ORDER_ASCENDING || direction == AS_ORDER_DESCENDING;
    if (!direction_ok) {
        as_error_update(&err, AEROSPIKE_ERR_PARAM, "%s", DIRECTION_ERROR);
        goto CLEANUP;
    }

    if (flags != 0 && type != AS_QUERY_ORDER_BY_STRING) {
        as_error_update(&err, AEROSPIKE_ERR_PARAM, "%s", FLAGS_ERROR);
        goto CLEANUP;
    }

    setter(&self->query, bin_name, (as_query_order_by_type)type,
           (as_order)direction, (as_query_order_by_flags)flags);

CLEANUP:
    if (err.code != AEROSPIKE_OK) {
        raise_exception(&err);
        return NULL;
    }

    Py_INCREF(self);
    return self;
}

AerospikeQuery *AerospikeQuery_OrderBy(AerospikeQuery *self, PyObject *args,
                                       PyObject *kwds)
{
    return set_order_key(self, args, kwds, "s#|III:order_by",
                         as_query_order_by);
}

AerospikeQuery *AerospikeQuery_ThenOrderBy(AerospikeQuery *self, PyObject *args,
                                           PyObject *kwds)
{
    return set_order_key(self, args, kwds, "s#|III:then_order_by",
                         as_query_then_order_by);
}
