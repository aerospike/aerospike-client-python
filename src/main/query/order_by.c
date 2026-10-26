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
#include <aerospike/as_query.h>

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
