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

#include <Python.h>
#include <stdbool.h>
#include <string.h>

#include <aerospike/as_bin.h>
#include <aerospike/as_error.h>
#include <aerospike/as_query.h>

#include "exceptions.h"
#include "pythoncapi_compat.h"
#include "query.h"

static const char BIN_NAME_ERROR[] =
    "Top-K order-by bin name must be 1-15 UTF-8 bytes without NUL";
static const char TYPE_REQUIRED_ERROR[] =
    "orderBy requires a type declaration (INTEGER, DOUBLE, STRING, or BYTES); "
    "Aerospike has no schema";
static const char DIRECTION_ERROR[] = "orderBy direction must be ASC or DESC";

typedef void (*order_key_setter)(as_query *query, const char *bin_name,
                                 as_query_order_by_type type,
                                 as_order direction,
                                 as_query_order_by_flags flags);

// Shared by order_by() and then_order_by(). format is the
// PyArg_ParseTupleAndKeywords format, which names the method in TypeErrors.
static AerospikeQuery *set_order_key(AerospikeQuery *self, PyObject *py_args,
                                     PyObject *py_kwds, const char *format,
                                     order_key_setter setter)
{
    // type and direction are parsed as optional so that omitting them raises
    // the ParamError messages from the Top-K API instead of a TypeError. They
    // are parsed as objects because the I format wraps out-of-range values.
    PyObject *py_bin = NULL;
    PyObject *py_type = NULL;
    PyObject *py_direction = NULL;
    int flags = AS_QUERY_ORDER_BY_FLAGS_DEFAULT;

    static char *kwlist[] = {"bin", "type", "direction", "flags", NULL};

    as_error err;
    as_error_init(&err);

    if (!self || (self->client && !self->client->as)) {
        as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
        goto CLEANUP;
    }

    if (!PyArg_ParseTupleAndKeywords(py_args, py_kwds, format, kwlist, &py_bin,
                                     &PyLong_Type, &py_type, &PyLong_Type,
                                     &py_direction, &flags)) {
        return NULL;
    }

    Py_ssize_t bin_size = 0;
    const char *bin_name = PyUnicode_AsUTF8AndSize(py_bin, &bin_size);
    if (!bin_name) {
        return NULL;
    }

    // The C setters copy only up to the first NUL.
    bool has_nul = strlen(bin_name) != (size_t)bin_size;
    bool size_ok = bin_size >= 1 && bin_size <= AS_BIN_NAME_MAX_LEN;
    if (has_nul || !size_ok) {
        as_error_update(&err, AEROSPIKE_ERR_PARAM, "%s", BIN_NAME_ERROR);
        goto CLEANUP;
    }

    if (!py_type) {
        as_error_update(&err, AEROSPIKE_ERR_PARAM, "%s", TYPE_REQUIRED_ERROR);
        goto CLEANUP;
    }

    // The C client checks that type is valid when the query executes.
    int type = PyLong_AsInt(py_type);
    if (type == -1 && PyErr_Occurred()) {
        return NULL;
    }

    if (!py_direction) {
        as_error_update(&err, AEROSPIKE_ERR_PARAM, "%s", DIRECTION_ERROR);
        goto CLEANUP;
    }

    long long direction = PyLong_AsLongLong(py_direction);
    if (direction == -1 && PyErr_Occurred()) {
        return NULL;
    }

    bool direction_ok =
        direction == AS_ORDER_ASCENDING || direction == AS_ORDER_DESCENDING;
    if (!direction_ok) {
        as_error_update(&err, AEROSPIKE_ERR_PARAM, "%s", DIRECTION_ERROR);
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

AerospikeQuery *AerospikeQuery_OrderBy(AerospikeQuery *self, PyObject *py_args,
                                       PyObject *py_kwds)
{
    return set_order_key(self, py_args, py_kwds, "U|O!O!i:order_by",
                         as_query_order_by);
}

AerospikeQuery *AerospikeQuery_ThenOrderBy(AerospikeQuery *self,
                                           PyObject *py_args, PyObject *py_kwds)
{
    return set_order_key(self, py_args, py_kwds, "U|O!O!i:then_order_by",
                         as_query_then_order_by);
}
