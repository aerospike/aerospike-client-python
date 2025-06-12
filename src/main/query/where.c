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

#include <aerospike/as_query.h>
#include <aerospike/aerospike_index.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/as_error.h>

#include "client.h"
#include "query.h"
#include "conversions.h"
#include "exceptions.h"
#include "policy.h"

#undef TRACE
#define TRACE()

// TODO: replace with helper function from conversions.c
int64_t pyobject_to_int64(PyObject *py_obj)
{
    if (PyLong_Check(py_obj)) {
        return PyLong_AsLongLong(py_obj);
    }
    else {
        return 0;
    }
}

// py_bin, py_val1, pyval2 are guaranteed to be non-NULL
static int AerospikeQuery_Where_Add(AerospikeQuery *self, PyObject *py_ctx,
                                    as_predicate_type predicate,
                                    as_index_datatype in_datatype,
                                    PyObject *py_bin, PyObject *py_val1,
                                    PyObject *py_val2, int index_type)
{
    as_error err;
    as_cdt_ctx *pctx = NULL;
    bool ctx_in_use = false;
    int rc = 1;

    if (py_ctx) {
        // TODO: does static pool go out of scope?
        as_static_pool static_pool;
        memset(&static_pool, 0, sizeof(static_pool));
        pctx = cf_malloc(sizeof(as_cdt_ctx));
        memset(pctx, 0, sizeof(as_cdt_ctx));
        if (get_cdt_ctx(self->client, &err, pctx, py_ctx, &ctx_in_use,
                        &static_pool, SERIALIZER_PYTHON) != AEROSPIKE_OK) {
            return err.code;
        }
    }

    const char *bin = NULL;
    if (PyUnicode_Check(py_bin)) {
        bin = PyUnicode_AsUTF8(py_bin);
        if (!bin) {
            goto CLEANUP1;
        }
    }
    else if (PyByteArray_Check(py_bin)) {
        bin = PyByteArray_AsString(py_bin);
        if (!bin) {
            goto CLEANUP1;
        }
    }
    else {
        // Bins are required for all where() calls
        goto CLEANUP1;
    }

    int64_t val1_int = 0;
    int64_t val2_int = 0;
    char *val1_str = NULL;
    uint8_t *val1_bytes = NULL;

    // Can point to either val1_int or val1_str. We use this so we can pass in the same optional argument
    // for both ints or strs to as_query_where_with_ctx().
    void *val1 = NULL;

    Py_ssize_t bytes_size = 0;

    if (in_datatype == AS_INDEX_STRING || in_datatype == AS_INDEX_GEO2DSPHERE) {
        if (!PyUnicode_Check(py_val1)) {
            goto CLEANUP1;
        }
        const char *buffer = PyUnicode_AsUTF8(py_val1);
        if (!buffer) {
            goto CLEANUP1;
        }
        val1_str = strdup(buffer);
        val1 = (void *)val1_str;
    }
    else if (in_datatype == AS_INDEX_NUMERIC) {
        val1_int = pyobject_to_int64(py_val1);
        if (PyErr_Occurred()) {
            PyErr_Clear();
            val1 = 0;
        }
        val1 = (void *)val1_int;

        if (PyLong_Check(py_val2)) {
            val2_int = pyobject_to_int64(py_val2);
            if (PyErr_Occurred()) {
                PyErr_Clear();
                val2_int = 0;
            }
        }
    }
    else if (in_datatype == AS_INDEX_BLOB) {
        char *bytes_buffer = NULL;
        if (PyBytes_Check(py_val1)) {
            bytes_buffer = PyBytes_AsString(py_val1);
            if (!bytes_buffer) {
                goto CLEANUP1;
            }
            bytes_size = PyBytes_Size(py_val1);
            if (PyErr_Occurred()) {
                goto CLEANUP1;
            }
        }
        else if (PyByteArray_Check(py_val1)) {
            bytes_buffer = PyByteArray_AsString(py_val1);
            if (!val1_bytes) {
                goto CLEANUP1;
            }
            bytes_size = PyByteArray_Size(py_val1);
            if (PyErr_Occurred()) {
                goto CLEANUP1;
            }
        }
        else {
            goto CLEANUP1;
        }

        uint8_t *val1_bytes_cpy =
            (uint8_t *)malloc(sizeof(uint8_t) * bytes_size);
        memcpy(val1_bytes_cpy, bytes_buffer, sizeof(uint8_t) * bytes_size);
        val1_bytes = val1_bytes_cpy;
        // Blobs are handled separately below, so we don't need to use the void* pointer
    }

    as_query_where_init(&self->query, 1);

    if (predicate == AS_PREDICATE_EQUAL && in_datatype == AS_INDEX_BLOB) {
        // We don't call as_blob_contains() directly because we can't pass in index_type as a parameter
        as_query_where_with_ctx(&self->query, bin, pctx, predicate, index_type,
                                AS_INDEX_BLOB, val1_bytes, bytes_size, true);
    }
    else if (in_datatype == AS_INDEX_NUMERIC ||
             in_datatype == AS_INDEX_STRING ||
             in_datatype == AS_INDEX_GEO2DSPHERE) {
        if (predicate == AS_PREDICATE_RANGE &&
            in_datatype == AS_INDEX_NUMERIC) {
            as_query_where_with_ctx(&self->query, bin, pctx, predicate,
                                    index_type, in_datatype, val1_int,
                                    val2_int);
        }
        else {
            as_query_where_with_ctx(&self->query, bin, pctx, predicate,
                                    index_type, in_datatype, val1);
        }

        if (in_datatype == AS_INDEX_STRING ||
            in_datatype == AS_INDEX_GEO2DSPHERE) {
            self->query.where.entries[0].value.string_val._free = true;
        }
    }
    else {
        // If it ain't supported, raise and error
        as_error_update(&err, AEROSPIKE_ERR_PARAM, "unknown predicate type");
        PyObject *py_err = NULL;
        error_to_pyobject(&err, &py_err);
        PyErr_SetObject(PyExc_Exception, py_err);
        goto CLEANUP2;
    }

    if (ctx_in_use) {
        self->query.where.entries[0].ctx_free = true;
    }
    rc = 0;

CLEANUP2:

    if (rc) {
        // The values end up not being used by as_query
        if (val1_str) {
            free(val1_str);
        }
        if (val1_bytes) {
            free(val1_bytes);
        }
    }

CLEANUP1:

    if (rc) {
        // The ctx ends up not being used by as_query
        if (ctx_in_use) {
            as_cdt_ctx_destroy(pctx);
        }
        if (pctx) {
            cf_free(pctx);
        }
    }

    return rc;
}

#define PREDICATE_INVALID_ERROR_MSG1 "predicate is invalid."
#define PREDICATE_INVALID_ERROR_MSG2 "Failed to fetch predicate information"

AerospikeQuery *AerospikeQuery_Where_Invoke(AerospikeQuery *self,
                                            PyObject *py_ctx,
                                            PyObject *py_predicate)
{

    as_error err;
    as_error_init(&err);

    // Parse predicate tuple

    if (!PyTuple_Check(py_predicate)) {
        as_error_update(&err, AEROSPIKE_ERR_PARAM,
                        PREDICATE_INVALID_ERROR_MSG1);
        goto CLEANUP;
    }
    Py_ssize_t size = PyTuple_Size(py_predicate);
    if (size <= 1 || size > 6) {
        as_error_update(&err, AEROSPIKE_ERR_PARAM,
                        PREDICATE_INVALID_ERROR_MSG1);
        goto CLEANUP;
    }

    PyObject *py_predicate_type = PyTuple_GetItem(py_predicate, 0);
    if (!py_predicate_type) {
        as_error_update(&err, AEROSPIKE_ERR_CLIENT,
                        PREDICATE_INVALID_ERROR_MSG2);
        goto CLEANUP;
    }
    PyObject *py_index_datatype = PyTuple_GetItem(py_predicate, 1);
    if (!py_index_datatype) {
        as_error_update(&err, AEROSPIKE_ERR_CLIENT,
                        PREDICATE_INVALID_ERROR_MSG2);
        goto CLEANUP;
    }

    if (!PyLong_Check(py_predicate_type) || !PyLong_Check(py_index_datatype)) {
        as_error_update(&err, AEROSPIKE_ERR_PARAM,
                        PREDICATE_INVALID_ERROR_MSG1);
        goto CLEANUP;
    }

    as_predicate_type predicate_type =
        (as_predicate_type)PyLong_AsLong(py_predicate_type);
    if (PyErr_Occurred()) {
        goto CLEANUP;
    }

    as_index_datatype index_datatype =
        (as_index_datatype)PyLong_AsLong(py_index_datatype);
    if (PyErr_Occurred()) {
        goto CLEANUP;
    }

    PyObject *py_bin = NULL;
    PyObject *py_val1 = NULL;
    PyObject *py_val2 = NULL;
    PyObject **py_optional_tuple_items[] = {&py_bin, &py_val1, &py_val2};

    // Read optional tuple items
    const Py_ssize_t FIRST_OPTIONAL_IDX = 2;
    for (Py_ssize_t i = FIRST_OPTIONAL_IDX; i <= 4; i++) {
        PyObject *py_tuple_item;
        if (i <= size - 1) {
            py_tuple_item = PyTuple_GetItem(py_predicate, i);
            if (!py_tuple_item) {
                goto CLEANUP;
            }
        }
        else {
            py_tuple_item = Py_None;
        }
        *(py_optional_tuple_items[i - FIRST_OPTIONAL_IDX]) = py_tuple_item;
    }

    as_index_type index_type;
    if (size == 6) {
        PyObject *py_index_type = PyTuple_GetItem(py_predicate, 5);
        if (!py_index_type) {
            goto CLEANUP;
        }
        if (!PyLong_Check(py_index_type)) {
            as_error_update(&err, AEROSPIKE_ERR_PARAM,
                            PREDICATE_INVALID_ERROR_MSG1);
            goto CLEANUP;
        }
        index_type = PyLong_AsLong(py_index_type);
        if (PyErr_Occurred()) {
            goto CLEANUP;
        }
    }
    else {
        index_type = AS_INDEX_TYPE_DEFAULT;
    }

    int rc =
        AerospikeQuery_Where_Add(self, py_ctx, predicate_type, index_datatype,
                                 py_bin, py_val1, py_val2, index_type);
    /* Failed to add the predicate for some reason */
    if (rc != 0) {
        as_error_update(&err, AEROSPIKE_ERR_PARAM, "Failed to add predicate");
        goto CLEANUP;
    }
CLEANUP:

    if (err.code != AEROSPIKE_OK) {
        raise_exception(&err);
        return NULL;
    }

    Py_INCREF(self);
    return self;
}

AerospikeQuery *AerospikeQuery_Where(AerospikeQuery *self, PyObject *args)
{
    as_error err;

    PyObject *py_pred = NULL;
    PyObject *py_cdt_ctx = NULL;

    if (PyArg_ParseTuple(args, "O|O:where", &py_pred, &py_cdt_ctx) == false) {
        return NULL;
    }

    as_error_init(&err);

    if (!self || !self->client->as) {
        as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
        goto CLEANUP;
    }

    if (!self->client->is_conn_16) {
        as_error_update(&err, AEROSPIKE_ERR_CLUSTER,
                        "No connection to aerospike cluster");
        goto CLEANUP;
    }

    return AerospikeQuery_Where_Invoke(self, py_cdt_ctx, py_pred);

CLEANUP:
    raise_exception(&err);
    return NULL;
}
