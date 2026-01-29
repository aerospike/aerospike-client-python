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

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_index.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/as_bin.h>
#include <aerospike/as_config.h>
#include <aerospike/as_error.h>
#include <aerospike/as_policy.h>

#include "client.h"
#include "conversions.h"
#include "exceptions.h"
#include "policy.h"

static bool getTypeFromPyObject(PyObject *py_datatype, int *idx_datatype,
                                as_error *err);

static PyObject *createIndexWithCollectionType(
    AerospikeClient *self, PyObject *py_policy, PyObject *py_ns,
    PyObject *py_set, PyObject *py_bin, PyObject *py_name,
    PyObject *py_datatype, as_index_type index_type, PyObject *py_ctx);

static PyObject *createIndexWithDataAndCollectionType(
    AerospikeClient *self, PyObject *py_policy, PyObject *py_ns,
    PyObject *py_set, PyObject *py_bin, PyObject *py_name,
    as_index_type index_type, as_index_datatype data_type, PyObject *py_ctx,
    as_exp *exp);

#define DEPRECATION_NOTICE_TO_USE_INDEX_SINGLE_VALUE_CREATE                    \
    "%s() is deprecated. Please use index_single_value_create() instead"

// This allows people to see the function calling the Python client API that issues a warning
#define STACK_LEVEL 2

/**
 *******************************************************************************************************
 * Creates an integer index for a bin in the Aerospike DB.
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns an integer status. 0(Zero) is success value.
 * In case of error,appropriate exceptions will be raised.
 *******************************************************************************************************
 */
PyObject *AerospikeClient_Index_Integer_Create(AerospikeClient *self,
                                               PyObject *args, PyObject *kwds)
{
    PyErr_WarnFormat(PyExc_DeprecationWarning, STACK_LEVEL,
                     DEPRECATION_NOTICE_TO_USE_INDEX_SINGLE_VALUE_CREATE,
                     "index_integer_create");

    // Initialize error
    as_error err;
    as_error_init(&err);

    // Python Function Arguments
    PyObject *py_policy = NULL;
    PyObject *py_ns = NULL;
    PyObject *py_set = NULL;
    PyObject *py_bin = NULL;
    PyObject *py_name = NULL;

    // Python Function Keyword Arguments
    static char *kwlist[] = {"ns", "set", "bin", "name", "policy", NULL};

    // Python Function Argument Parsing
    if (PyArg_ParseTupleAndKeywords(args, kwds, "OOOO|O:index_integer_create",
                                    kwlist, &py_ns, &py_set, &py_bin, &py_name,
                                    &py_policy) == false) {
        return NULL;
    }

    return createIndexWithDataAndCollectionType(
        self, py_policy, py_ns, py_set, py_bin, py_name, AS_INDEX_TYPE_DEFAULT,
        AS_INDEX_NUMERIC, NULL, NULL);
}

/**
 *******************************************************************************************************
 * Creates a string index for a bin in the Aerospike DB.
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns an integer status. 0(Zero) is success value.
 * In case of error,appropriate exceptions will be raised.
 *******************************************************************************************************
 */
PyObject *AerospikeClient_Index_String_Create(AerospikeClient *self,
                                              PyObject *args, PyObject *kwds)
{
    PyErr_WarnFormat(PyExc_DeprecationWarning, STACK_LEVEL,
                     DEPRECATION_NOTICE_TO_USE_INDEX_SINGLE_VALUE_CREATE,
                     "index_string_create");

    // Initialize error
    as_error err;
    as_error_init(&err);

    // Python Function Arguments
    PyObject *py_policy = NULL;
    PyObject *py_ns = NULL;
    PyObject *py_set = NULL;
    PyObject *py_bin = NULL;
    PyObject *py_name = NULL;

    // Python Function Keyword Arguments
    static char *kwlist[] = {"ns", "set", "bin", "name", "policy", NULL};

    // Python Function Argument Parsing
    if (PyArg_ParseTupleAndKeywords(args, kwds, "OOOO|O:index_string_create",
                                    kwlist, &py_ns, &py_set, &py_bin, &py_name,
                                    &py_policy) == false) {
        return NULL;
    }

    return createIndexWithDataAndCollectionType(
        self, py_policy, py_ns, py_set, py_bin, py_name, AS_INDEX_TYPE_DEFAULT,
        AS_INDEX_STRING, NULL, NULL);
}

PyObject *AerospikeClient_Index_Blob_Create(AerospikeClient *self,
                                            PyObject *args, PyObject *kwds)
{
    PyErr_WarnFormat(PyExc_DeprecationWarning, STACK_LEVEL,
                     DEPRECATION_NOTICE_TO_USE_INDEX_SINGLE_VALUE_CREATE,
                     "index_blob_create");

    // Python Function Arguments
    PyObject *py_policy = NULL;
    PyObject *py_ns = NULL;
    PyObject *py_set = NULL;
    PyObject *py_bin = NULL;
    PyObject *py_name = NULL;

    // Python Function Keyword Arguments
    static char *kwlist[] = {"ns", "set", "bin", "name", "policy", NULL};

    // Python Function Argument Parsing
    if (PyArg_ParseTupleAndKeywords(args, kwds, "OOOO|O:index_blob_create",
                                    kwlist, &py_ns, &py_set, &py_bin, &py_name,
                                    &py_policy) == false) {
        return NULL;
    }

    return createIndexWithDataAndCollectionType(
        self, py_policy, py_ns, py_set, py_bin, py_name, AS_INDEX_TYPE_DEFAULT,
        AS_INDEX_BLOB, NULL, NULL);
}

PyObject *AerospikeClient_Index_Expr_Create(AerospikeClient *self,
                                            PyObject *args, PyObject *kwds)
{
    // Python Function Arguments
    PyObject *py_ns = NULL;
    PyObject *py_set = NULL;
    PyObject *py_expr = NULL;
    as_index_type index_type;
    as_index_datatype data_type;
    as_exp *expr = NULL;
    PyObject *py_name = NULL;
    PyObject *py_policy = NULL;

    // Python Function Keyword Arguments
    static char *kwlist[] = {
        "ns",          "set",  "index_type", "index_datatype",
        "expressions", "name", "policy",     NULL};

    // Python Function Argument Parsing
    if (PyArg_ParseTupleAndKeywords(
            args, kwds, "OOiiOO|O:index_expr_create", kwlist, &py_ns, &py_set,
            &index_type, &data_type, &py_expr, &py_name, &py_policy) == false) {
        return NULL;
    }

    as_error err;
    as_error_init(&err);
    if (as_exp_new_from_pyobject(self, py_expr, &expr, &err, false) !=
        AEROSPIKE_OK) {
        raise_exception(&err);
        return NULL;
    }

    return createIndexWithDataAndCollectionType(self, py_policy, py_ns, py_set,
                                                NULL, py_name, index_type,
                                                data_type, NULL, expr);
}

#define CTX_PARSE_ERROR_MESSAGE "Unable to parse ctx"

/**
 *******************************************************************************************************
 * Creates a cdt index for a bin in the Aerospike DB.
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns an integer status. 0(Zero) is success value.
 * In case of error,appropriate exceptions will be raised.
 *******************************************************************************************************
 */
PyObject *AerospikeClient_Index_Cdt_Create(AerospikeClient *self,
                                           PyObject *args, PyObject *kwds)
{
    PyErr_WarnEx(PyExc_DeprecationWarning,
                 "index_cdt_create() is deprecated. Please use one of the "
                 "other non-deprecated index_*_create() methods instead",
                 2);

    // Initialize error
    as_error err;
    as_error_init(&err);

    // Python Function Arguments
    PyObject *py_policy = NULL;
    PyObject *py_ns = NULL;
    PyObject *py_set = NULL;
    PyObject *py_bin = NULL;
    PyObject *py_indextype = NULL;
    PyObject *py_datatype = NULL;
    PyObject *py_name = NULL;

    PyObject *py_ctx = NULL;

    PyObject *py_obj = NULL;
    as_index_datatype data_type;
    as_index_type index_type;

    // Python Function Keyword Arguments
    static char *kwlist[] = {
        "ns",   "set", "bin",    "index_type", "index_datatype",
        "name", "ctx", "policy", NULL};

    // Python Function Argument Parsing
    if (PyArg_ParseTupleAndKeywords(args, kwds, "OOOOOOO|O:index_list_create",
                                    kwlist, &py_ns, &py_set, &py_bin,
                                    &py_indextype, &py_datatype, &py_name,
                                    &py_ctx, &py_policy) == false) {
        return NULL;
    }

    if (!getTypeFromPyObject(py_indextype, (int *)&index_type, &err)) {
        goto CLEANUP;
    }

    if (!getTypeFromPyObject(py_datatype, (int *)&data_type, &err)) {
        goto CLEANUP;
    }

    // Even if this call fails, it will raise its own exception
    // and the err object here will not be set. We don't raise an exception twice
    py_obj = createIndexWithDataAndCollectionType(
        self, py_policy, py_ns, py_set, py_bin, py_name, index_type, data_type,
        py_ctx, NULL);

    // as_cdt_ctx_destroy(&ctx);

CLEANUP:
    if (err.code != AEROSPIKE_OK) {
        raise_exception_base(&err, Py_None, Py_None, Py_None, Py_None, py_name);
        return NULL;
    }
    return py_obj;
}

/**
 *******************************************************************************************************
 * Removes an index in the Aerospike database.
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns an integer status. 0(Zero) is success value.
 * In case of error,appropriate exceptions will be raised.
 *******************************************************************************************************
 */
PyObject *AerospikeClient_Index_Remove(AerospikeClient *self, PyObject *args,
                                       PyObject *kwds)
{
    // Initialize error
    as_error err;
    as_error_init(&err);

    // Python Function Arguments
    PyObject *py_policy = NULL;
    PyObject *py_ns = NULL;
    PyObject *py_name = NULL;
    PyObject *py_ustr_name = NULL;

    as_policy_info info_policy;
    as_policy_info *info_policy_p = NULL;

    // Python Function Keyword Arguments
    static char *kwlist[] = {"ns", "name", "policy", NULL};

    // Python Function Argument Parsing
    if (PyArg_ParseTupleAndKeywords(args, kwds, "OO|O:index_remove", kwlist,
                                    &py_ns, &py_name, &py_policy) == false) {
        return NULL;
    }

    if (!self || !self->as) {
        as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
        goto CLEANUP;
    }

    if (!self->is_conn_16) {
        as_error_update(&err, AEROSPIKE_ERR_CLUSTER,
                        "No connection to aerospike cluster");
        goto CLEANUP;
    }

    // Convert python object to policy_info
    pyobject_to_policy_info(&err, py_policy, &info_policy, &info_policy_p,
                            &self->as->config.policies.info,
                            self->validate_keys, SECOND_AS_POLICY_NONE);
    if (err.code != AEROSPIKE_OK) {
        goto CLEANUP;
    }

    // Convert python object into namespace string
    if (!PyUnicode_Check(py_ns)) {
        as_error_update(&err, AEROSPIKE_ERR_PARAM,
                        "Namespace should be a string");
        goto CLEANUP;
    }
    char *namespace = (char *)PyUnicode_AsUTF8(py_ns);

    // Convert PyObject into the name of the index
    char *name = NULL;
    if (PyUnicode_Check(py_name)) {
        py_ustr_name = PyUnicode_AsUTF8String(py_name);
        name = PyBytes_AsString(py_ustr_name);
    }
    else {
        as_error_update(&err, AEROSPIKE_ERR_PARAM,
                        "Index name should be string or unicode");
        goto CLEANUP;
    }

    // Invoke operation
    Py_BEGIN_ALLOW_THREADS
    aerospike_index_remove(self->as, &err, info_policy_p, namespace, name);
    Py_END_ALLOW_THREADS

CLEANUP:

    if (py_ustr_name) {
        Py_DECREF(py_ustr_name);
    }
    if (err.code != AEROSPIKE_OK) {
        raise_exception_base(&err, Py_None, Py_None, Py_None, Py_None, py_name);
        return NULL;
    }

    return PyLong_FromLong(0);
}

// TODO: way to get method name dynamically for error message?
static inline PyObject *AerospikeClient_Index_Create(AerospikeClient *self,
                                                     PyObject *args,
                                                     PyObject *kwds,
                                                     as_index_type index_type,
                                                     const char *ml_name)
{
    // Initialize error
    as_error err;
    as_error_init(&err);

    // Python Function Arguments
    PyObject *py_policy = NULL;
    PyObject *py_ns = NULL;
    PyObject *py_set = NULL;
    PyObject *py_bin = NULL;
    PyObject *py_name = NULL;
    PyObject *py_datatype = NULL;
    PyObject *py_ctx = NULL;

    static char *kwlist[] = {"ns",   "set",    "bin", "index_datatype",
                             "name", "policy", "ctx", NULL};

    char format[256];
    snprintf(format, 256, "OOOOO|OO:%s", ml_name);
    if (PyArg_ParseTupleAndKeywords(args, kwds, format, kwlist, &py_ns, &py_set,
                                    &py_bin, &py_datatype, &py_name, &py_policy,
                                    &py_ctx) == false) {
        return NULL;
    }

    return createIndexWithCollectionType(self, py_policy, py_ns, py_set, py_bin,
                                         py_name, py_datatype, index_type,
                                         py_ctx);
}

PyObject *AerospikeClient_Index_Single_Value_Create(AerospikeClient *self,
                                                    PyObject *args,
                                                    PyObject *kwds)
{
    return AerospikeClient_Index_Create(self, args, kwds, AS_INDEX_TYPE_DEFAULT,
                                        "index_single_value_create");
}

PyObject *AerospikeClient_Index_List_Create(AerospikeClient *self,
                                            PyObject *args, PyObject *kwds)
{
    return AerospikeClient_Index_Create(self, args, kwds, AS_INDEX_TYPE_LIST,
                                        "index_list_create");
}

PyObject *AerospikeClient_Index_Map_Keys_Create(AerospikeClient *self,
                                                PyObject *args, PyObject *kwds)
{
    return AerospikeClient_Index_Create(self, args, kwds, AS_INDEX_TYPE_MAPKEYS,
                                        "index_map_keys_create");
}

PyObject *AerospikeClient_Index_Map_Values_Create(AerospikeClient *self,
                                                  PyObject *args,
                                                  PyObject *kwds)
{
    return AerospikeClient_Index_Create(
        self, args, kwds, AS_INDEX_TYPE_MAPVALUES, "index_map_values_create");
}

PyObject *AerospikeClient_Index_2dsphere_Create(AerospikeClient *self,
                                                PyObject *args, PyObject *kwds)
{
    PyErr_WarnFormat(PyExc_DeprecationWarning, STACK_LEVEL,
                     DEPRECATION_NOTICE_TO_USE_INDEX_SINGLE_VALUE_CREATE,
                     "index_geo2dsphere_create");

    // Initialize error
    as_error err;
    as_error_init(&err);

    // Python Function Arguments
    PyObject *py_policy = NULL;
    PyObject *py_ns = NULL;
    PyObject *py_set = NULL;
    PyObject *py_bin = NULL;
    PyObject *py_name = NULL;

    // Python Function Keyword Arguments
    static char *kwlist[] = {"ns", "set", "bin", "name", "policy", NULL};

    // Python Function Argument Parsing
    if (PyArg_ParseTupleAndKeywords(
            args, kwds, "OOOO|O:index_geo2dsphere_create", kwlist, &py_ns,
            &py_set, &py_bin, &py_name, &py_policy) == false) {
        return NULL;
    }

    return createIndexWithDataAndCollectionType(
        self, py_policy, py_ns, py_set, py_bin, py_name, AS_INDEX_TYPE_DEFAULT,
        AS_INDEX_GEO2DSPHERE, NULL, NULL);
}

/*
 * Convert a PyObject into an as_index_datatype, return False if the conversion fails for any reason.
 */
static bool getTypeFromPyObject(PyObject *py_datatype, int *idx_datatype,
                                as_error *err)
{

    long type = 0;
    if (PyLong_Check(py_datatype)) {
        type = PyLong_AsLong(py_datatype);
        if (type == -1 && PyErr_Occurred()) {
            if (PyErr_ExceptionMatches(PyExc_OverflowError)) {
                as_error_update(err, AEROSPIKE_ERR_PARAM,
                                "integer value exceeds sys.maxsize");
                goto CLEANUP;
            }
        }
    }
    else {
        as_error_update(err, AEROSPIKE_ERR_PARAM,
                        "Index type must be an integer");
        goto CLEANUP;
    }

    *idx_datatype = type;

CLEANUP:
    if (err->code != AEROSPIKE_OK) {
        raise_exception(err);
        return false;
    }
    return true;
}

/*
 * Figure out the data_type from a PyObject and call createIndexWithDataAndCollectionType.
 */
static PyObject *createIndexWithCollectionType(
    AerospikeClient *self, PyObject *py_policy, PyObject *py_ns,
    PyObject *py_set, PyObject *py_bin, PyObject *py_name,
    PyObject *py_datatype, as_index_type index_type, PyObject *py_ctx)
{

    as_index_datatype data_type = AS_INDEX_STRING;

    as_error err;
    as_error_init(&err);

    if (!getTypeFromPyObject(py_datatype, (int *)&data_type, &err)) {
        return NULL;
    }

    return createIndexWithDataAndCollectionType(self, py_policy, py_ns, py_set,
                                                py_bin, py_name, index_type,
                                                data_type, py_ctx, NULL);
}

/*
 * Create a complex index on the specified ns/set/bin with the given name and index and data_type. Return PyObject(0) on success
 * else return NULL with an error raised.
 */

// exp is optional and can be NULL.
// If exp is non-NULL (i.e we are indexing an expression), py_bin should be NULL.
static PyObject *createIndexWithDataAndCollectionType(
    AerospikeClient *self, PyObject *py_policy, PyObject *py_ns,
    PyObject *py_set, PyObject *py_bin, PyObject *py_name,
    as_index_type index_type, as_index_datatype data_type, PyObject *py_ctx,
    as_exp *exp)
{

    // Initialize error
    as_error err;
    as_error_init(&err);

    PyObject *py_ustr_set = NULL;
    PyObject *py_ustr_bin = NULL;
    PyObject *py_ustr_name = NULL;

    as_policy_info info_policy;
    as_policy_info *info_policy_p = NULL;
    as_index_task task;

    if (!self || !self->as) {
        as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
        //raise_exception(&err, -2, "Invalid aerospike object");
        goto CLEANUP;
    }

    if (!self->is_conn_16) {
        as_error_update(&err, AEROSPIKE_ERR_CLUSTER,
                        "No connection to aerospike cluster");
        goto CLEANUP;
    }

    // Convert python object to policy_info
    pyobject_to_policy_info(&err, py_policy, &info_policy, &info_policy_p,
                            &self->as->config.policies.info,
                            self->validate_keys, SECOND_AS_POLICY_NONE);
    if (err.code != AEROSPIKE_OK) {
        goto CLEANUP;
    }

    // Convert python object into namespace string
    if (!PyUnicode_Check(py_ns)) {
        as_error_update(&err, AEROSPIKE_ERR_PARAM,
                        "Namespace should be a string");
        goto CLEANUP;
    }
    char *namespace = (char *)PyUnicode_AsUTF8(py_ns);

    // Convert python object into set string
    char *set_ptr = NULL;
    if (PyUnicode_Check(py_set)) {
        py_ustr_set = PyUnicode_AsUTF8String(py_set);
        set_ptr = PyBytes_AsString(py_ustr_set);
    }
    else if (py_set != Py_None) {
        as_error_update(&err, AEROSPIKE_ERR_PARAM,
                        "Set should be string, unicode or None");
        goto CLEANUP;
    }

    // Convert python object into bin string
    char *bin_ptr = NULL;
    if (py_bin) {
        if (PyUnicode_Check(py_bin)) {
            py_ustr_bin = PyUnicode_AsUTF8String(py_bin);
            bin_ptr = PyBytes_AsString(py_ustr_bin);
        }
        else if (PyByteArray_Check(py_bin)) {
            bin_ptr = PyByteArray_AsString(py_bin);
        }
        else {
            as_error_update(&err, AEROSPIKE_ERR_PARAM,
                            "Bin should be a string");
            goto CLEANUP;
        }
    }

    // Convert PyObject into the name of the index
    char *name = NULL;
    if (PyUnicode_Check(py_name)) {
        py_ustr_name = PyUnicode_AsUTF8String(py_name);
        name = PyBytes_AsString(py_ustr_name);
    }
    else {
        as_error_update(&err, AEROSPIKE_ERR_PARAM,
                        "Index name should be string or unicode");
        goto CLEANUP;
    }

    // TODO: this should be refactored by using a new helper function to parse a ctx list instead of get_cdt_ctx()
    // which only parses a dictionary containing a ctx list
    as_cdt_ctx ctx;
    bool ctx_in_use = false;
    PyObject *py_ctx_dict = NULL;
    if (py_ctx) {
        py_ctx_dict = PyDict_New();
        if (!py_ctx_dict) {
            as_error_update(&err, AEROSPIKE_ERR_CLIENT,
                            CTX_PARSE_ERROR_MESSAGE);
            goto CLEANUP;
        }
        int retval = PyDict_SetItemString(py_ctx_dict, "ctx", py_ctx);
        if (retval == -1) {
            as_error_update(&err, AEROSPIKE_ERR_CLIENT,
                            CTX_PARSE_ERROR_MESSAGE);
            goto CLEANUP2;
        }

        as_static_pool static_pool;
        memset(&static_pool, 0, sizeof(static_pool));

        if (get_cdt_ctx(self, &err, &ctx, py_ctx_dict, &ctx_in_use,
                        &static_pool, SERIALIZER_PYTHON) != AEROSPIKE_OK) {
            goto CLEANUP2;
        }
    }

    as_cdt_ctx *ctx_ref = ctx_in_use ? &ctx : NULL;

    // Invoke operation
    Py_BEGIN_ALLOW_THREADS
    if (exp) {
        aerospike_index_create_exp(self->as, &err, &task, info_policy_p,
                                   namespace, set_ptr, name, index_type,
                                   data_type, exp);
    }
    else {
        aerospike_index_create_ctx(self->as, &err, &task, info_policy_p,
                                   namespace, set_ptr, bin_ptr, name,
                                   index_type, data_type, ctx_ref);
    }
    Py_END_ALLOW_THREADS
    if (err.code == AEROSPIKE_OK) {
        Py_BEGIN_ALLOW_THREADS
        aerospike_index_create_wait(&err, &task, 2000);
        Py_END_ALLOW_THREADS
    }

    if (ctx_ref) {
        as_cdt_ctx_destroy(ctx_ref);
    }

CLEANUP2:
    Py_XDECREF(py_ctx_dict);

CLEANUP:
    if (py_ustr_set) {
        Py_DECREF(py_ustr_set);
    }
    if (py_ustr_bin) {
        Py_DECREF(py_ustr_bin);
    }
    if (py_ustr_name) {
        Py_DECREF(py_ustr_name);
    }
    if (exp) {
        as_exp_destroy(exp);
    }
    if (err.code != AEROSPIKE_OK) {
        raise_exception(&err);
        return NULL;
    }

    return PyLong_FromLong(0);
}
