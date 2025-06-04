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

static int AerospikeQuery_Where_Add(AerospikeQuery *self, PyObject *py_ctx,
                                    as_predicate_type predicate,
                                    as_index_datatype in_datatype,
                                    PyObject *py_bin, PyObject *py_val1,
                                    PyObject *py_val2, int index_type)
{
    as_error err;
    as_cdt_ctx *pctx = NULL;
    bool ctx_in_use = false;
    int rc = 0;

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

    as_exp *exp_list = NULL;
    if (py_expr) {
        as_status status =
            convert_exp_list(self->client, py_expr, &exp_list, &err);
        if (status != AEROSPIKE_OK) {
            return err.code;
        }
    }

    char *bin = NULL;
    if (py_bin) {
        if (PyUnicode_Check(py_bin)) {
            bin = PyUnicode_AsUTF8(py_bin);
        }
        else if (PyByteArray_Check(py_bin)) {
            bin = PyByteArray_AsString(py_bin);
        }
        else {
            rc = 1;
        }

        Py_DECREF(py_bin);
        py_bin = NULL;
    }

    int64_t val1 = 0;
    int64_t val2 = 0;
    char *val1_str = NULL;
    Py_ssize_t bytes_size = 0;
    if (in_datatype == AS_INDEX_STRING) {
        if (PyUnicode_Check(py_val1)) {
            val1_str = PyUnicode_AsUTF8(py_val1);
        }
        else {
            rc = 1;
        }
    }
    else if (in_datatype == AS_INDEX_NUMERIC) {
        val1 = pyobject_to_int64(py_val1);
        if (py_val2) {
            val2 = pyobject_to_int64(py_val2);
        }
    }
    else if (in_datatype == AS_INDEX_BLOB) {
        uint8_t *val = NULL;
        ;

        if (PyBytes_Check(py_val1)) {
            val = (uint8_t *)PyBytes_AsString(py_val1);
            bytes_size = PyBytes_Size(py_val1);
        }
        else if (PyByteArray_Check(py_val1)) {
            val = (uint8_t *)PyByteArray_AsString(py_val1);
            bytes_size = PyByteArray_Size(py_val1);
        }
        else {
            rc = 1;
        }

        uint8_t *bytes_buffer = (uint8_t *)malloc(sizeof(uint8_t) * bytes_size);
        memcpy(bytes_buffer, val, sizeof(uint8_t) * bytes_size);
        val = bytes_buffer;
    }

    as_query_where_init(&self->query, 1);

    if (in_datatype == AS_INDEX_BLOB) {
        if (exp_list) {
            as_query_where_with_exp(&self->query, NULL, exp_list, predicate,
                                    index_type, in_datatype, val1_str,
                                    bytes_size, true);
        }
        else {
            as_query_where_with_ctx(&self->query, bin, pctx, predicate,
                                    index_type, in_datatype, val1_str,
                                    bytes_size, true);
        }
        // Cleanup
        free(bin);
    }
    else if (in_datatype == AS_INDEX_BLOB ||
             in_datatype == AS_INDEX_GEO2DSPHERE ||
             in_datatype == AS_INDEX_NUMERIC ||
             in_datatype == AS_INDEX_STRING) {
        if (predicate == AS_PREDICATE_RANGE) {
            if (exp_list) {
                as_query_where_with_exp(&self->query, NULL, exp_list, predicate,
                                        index_type, in_datatype, val1, val2);
            }
            else {
                as_query_where_with_ctx(&self->query, bin, pctx, predicate,
                                        index_type, in_datatype, val1, val2);
            }
            if (in_datatype == AS_INDEX_GEO2DSPHERE) {
                self->query.where.entries[0].value.string_val._free = true;
            }
        }
        else if (predicate == AS_PREDICATE_EQUAL) {
            if (exp_list) {
                as_query_where_with_exp(&self->query, NULL, exp_list, predicate,
                                        index_type, in_datatype, val1);
            }
            else {
                as_query_where_with_ctx(&self->query, bin, pctx, predicate,
                                        index_type, in_datatype, val1);
            }
        }
    }
    else {
        // If it ain't supported, raise and error
        as_error_update(&err, AEROSPIKE_ERR_PARAM, "unknown predicate type");
        PyObject *py_err = NULL;
        error_to_pyobject(&err, &py_err);
        PyErr_SetObject(PyExc_Exception, py_err);
        rc = 1;
    }

    if (rc) {
        assert(false);
        if (ctx_in_use) {
            as_cdt_ctx_destroy(pctx);
        }
        if (pctx) {
            cf_free(pctx);
        }
    }
    else if (ctx_in_use) {
        self->query.where.entries[0].ctx_free = true;
    }
    return rc;
}

AerospikeQuery *AerospikeQuery_Where_Invoke(AerospikeQuery *self,
                                            PyObject *py_arg1,
                                            PyObject *py_arg2)
{
    as_error err;
    int rc = 0;

    as_error_init(&err);

    if (PyTuple_Check(py_arg2) && PyTuple_Size(py_arg2) > 1 &&
        PyTuple_Size(py_arg2) <= 6) {

        Py_ssize_t size = PyTuple_Size(py_arg2);

        PyObject *py_op = PyTuple_GetItem(py_arg2, 0);
        PyObject *py_op_data = PyTuple_GetItem(py_arg2, 1);
        if (!py_op || !py_op_data) {
            as_error_update(&err, AEROSPIKE_ERR_CLIENT,
                            "Failed to fetch predicate information");
            goto CLEANUP;
        }
        if (PyLong_Check(py_op) && PyLong_Check(py_op_data)) {
            as_predicate_type op = (as_predicate_type)PyLong_AsLong(py_op);
            as_index_datatype op_data =
                (as_index_datatype)PyLong_AsLong(py_op_data);
            rc = AerospikeQuery_Where_Add(
                self, py_arg1, op, op_data,
                size > 2 ? PyTuple_GetItem(py_arg2, 2) : Py_None,
                size > 3 ? PyTuple_GetItem(py_arg2, 3) : Py_None,
                size > 4 ? PyTuple_GetItem(py_arg2, 4) : Py_None,
                size > 5 ? PyLong_AsLong(PyTuple_GetItem(py_arg2, 5)) : 0);
            /* Failed to add the predicate for some reason */
            if (rc != 0) {
                as_error_update(&err, AEROSPIKE_ERR_PARAM,
                                "Failed to add predicate");
                goto CLEANUP;
            }
            /* Incorrect predicate or index type */
        }
        else {
            as_error_update(&err, AEROSPIKE_ERR_PARAM, "predicate is invalid.");
            goto CLEANUP;
        }
        /* Predicate not a tuple, or too short or too long */
    }
    else {
        as_error_update(&err, AEROSPIKE_ERR_PARAM, "predicate is invalid.");
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
