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

#define CTX_PARSE_ERROR_MESSAGE "Unable to parse ctx"

/*
 * Create a complex index on the specified ns/set/bin with the given name and index and data_type. Return PyObject(0) on success
 * else return NULL with an error raised.
 */
// expr is optional and can be NULL.
// If expr is non-NULL (i.e we are indexing an expression), py_bin should be NULL.
// This is permissive and allows py_ctx to be None or NULL
//
// NOTE: data_type and index_type are integers because some index creation methods i.e index_expr_create
// take in an C integer directly as an argument, but the rest of the index creation methods take in a PyObject
// that needs to be converted to a C integer on our end.
// To handle both cases, we just have each individual index creation method convert those parameters to C integers
// if needed.
static PyObject *convert_python_args_to_c_and_create_index(
    AerospikeClient *self, PyObject *py_policy, PyObject *py_ns,
    PyObject *py_set, PyObject *py_bin, PyObject *py_name,
    as_index_type index_type, as_index_datatype data_type, PyObject *py_ctx,
    PyObject *py_expr)
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
    if (py_ctx && !Py_IsNone(py_ctx)) {
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

    as_exp *expr = NULL;
    if (py_expr && as_exp_new_from_pyobject(self, py_expr, &expr, &err,
                                            false) != AEROSPIKE_OK) {
        goto CLEANUP3;
    }

    // Invoke operation
    Py_BEGIN_ALLOW_THREADS
    if (expr) {
        aerospike_index_create_exp(self->as, &err, &task, info_policy_p,
                                   namespace, set_ptr, name, index_type,
                                   data_type, expr);
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

    if (expr) {
        as_exp_destroy(expr);
    }

CLEANUP3:
    if (ctx_ref) {
        as_cdt_ctx_destroy(ctx_ref);
    }

CLEANUP2:
    Py_XDECREF(py_ctx_dict);

CLEANUP:
    Py_XDECREF(py_ustr_set);
    Py_XDECREF(py_ustr_bin);
    Py_XDECREF(py_ustr_name);

    if (err.code != AEROSPIKE_OK) {
        raise_exception(&err);
        return NULL;
    }

    return PyLong_FromLong(0);
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

    return convert_python_args_to_c_and_create_index(
        self, py_policy, py_ns, py_set, NULL, py_name, index_type, data_type,
        NULL, py_expr);
}

// TODO: way to get method name dynamically for error message?
static inline PyObject *
AerospikeClient_Index_Create_Helper(AerospikeClient *self, PyObject *args,
                                    PyObject *kwds, as_index_type index_type,
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

    as_index_datatype index_datatype = AS_INDEX_STRING;
    if (get_int_from_py_int(&err, py_datatype, (int *)&index_datatype,
                            "index_datatype") != AEROSPIKE_OK) {
        goto CLEANUP_ON_ERROR;
    }

    return convert_python_args_to_c_and_create_index(
        self, py_policy, py_ns, py_set, py_bin, py_name, index_type,
        index_datatype, py_ctx, NULL);

CLEANUP_ON_ERROR:
    raise_exception_base(&err, Py_None, Py_None, Py_None, Py_None, py_name);
    return NULL;
}

PyObject *AerospikeClient_Index_Single_Value_Create(AerospikeClient *self,
                                                    PyObject *args,
                                                    PyObject *kwds)
{
    return AerospikeClient_Index_Create_Helper(
        self, args, kwds, AS_INDEX_TYPE_DEFAULT, "index_single_value_create");
}

PyObject *AerospikeClient_Index_List_Create(AerospikeClient *self,
                                            PyObject *args, PyObject *kwds)
{
    return AerospikeClient_Index_Create_Helper(
        self, args, kwds, AS_INDEX_TYPE_LIST, "index_list_create");
}

PyObject *AerospikeClient_Index_Map_Keys_Create(AerospikeClient *self,
                                                PyObject *args, PyObject *kwds)
{
    return AerospikeClient_Index_Create_Helper(
        self, args, kwds, AS_INDEX_TYPE_MAPKEYS, "index_map_keys_create");
}

PyObject *AerospikeClient_Index_Map_Values_Create(AerospikeClient *self,
                                                  PyObject *args,
                                                  PyObject *kwds)
{
    return AerospikeClient_Index_Create_Helper(
        self, args, kwds, AS_INDEX_TYPE_MAPVALUES, "index_map_values_create");
}

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

// Deprecated API's

#define DEPRECATION_NOTICE_TO_USE_INDEX_SINGLE_VALUE_CREATE                    \
    "%s() is deprecated. Please use index_single_value_create() instead"

static PyObject *AerospikeClient_Index_Create_Deprecated_Helper(
    AerospikeClient *self, PyObject *args, PyObject *kwds, const char *ml_name,
    as_index_datatype index_datatype)
{
    PyErr_WarnFormat(PyExc_DeprecationWarning, STACK_LEVEL,
                     DEPRECATION_NOTICE_TO_USE_INDEX_SINGLE_VALUE_CREATE,
                     ml_name);

    as_error err;
    as_error_init(&err);

    // Python Function Arguments
    PyObject *py_ns = NULL;
    PyObject *py_set = NULL;
    PyObject *py_bin = NULL;
    PyObject *py_name = NULL;
    PyObject *py_policy = NULL;

    static char *kwlist[] = {"ns", "set", "bin", "name", "policy", NULL};

    // Python Function Argument Parsing
    if (PyArg_ParseTupleAndKeywords(args, kwds, "OOOO|O:index_integer_create",
                                    kwlist, &py_ns, &py_set, &py_bin, &py_name,
                                    &py_policy) == false) {
        return NULL;
    }

    return convert_python_args_to_c_and_create_index(
        self, py_policy, py_ns, py_set, py_bin, py_name, AS_INDEX_TYPE_DEFAULT,
        index_datatype, NULL, NULL);
}

PyObject *AerospikeClient_Index_Integer_Create(AerospikeClient *self,
                                               PyObject *args, PyObject *kwds)
{
    return AerospikeClient_Index_Create_Deprecated_Helper(
        self, args, kwds, "index_integer_create", AS_INDEX_NUMERIC);
}

PyObject *AerospikeClient_Index_String_Create(AerospikeClient *self,
                                              PyObject *args, PyObject *kwds)
{
    return AerospikeClient_Index_Create_Deprecated_Helper(
        self, args, kwds, "index_string_create", AS_INDEX_STRING);
}

PyObject *AerospikeClient_Index_Blob_Create(AerospikeClient *self,
                                            PyObject *args, PyObject *kwds)
{
    return AerospikeClient_Index_Create_Deprecated_Helper(
        self, args, kwds, "index_blob_create", AS_INDEX_BLOB);
}

PyObject *AerospikeClient_Index_2dsphere_Create(AerospikeClient *self,
                                                PyObject *args, PyObject *kwds)
{
    return AerospikeClient_Index_Create_Deprecated_Helper(
        self, args, kwds, "index_geo2dsphere_create", AS_INDEX_GEO2DSPHERE);
}

PyObject *AerospikeClient_Index_Cdt_Create(AerospikeClient *self,
                                           PyObject *args, PyObject *kwds)
{
    int retval =
        PyErr_WarnEx(PyExc_DeprecationWarning,
                     "index_cdt_create() is deprecated. Please use one of the "
                     "other non-deprecated index_*_create() methods instead",
                     STACK_LEVEL);
    if (retval == -1) {
        return NULL;
    }

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

    if (get_int_from_py_int(&err, py_indextype, (int *)&index_type,
                            "index_type") != AEROSPIKE_OK) {
        goto CLEANUP_ON_ERROR;
    }

    if (get_int_from_py_int(&err, py_datatype, (int *)&data_type,
                            "index_datatype") != AEROSPIKE_OK) {
        goto CLEANUP_ON_ERROR;
    }

    // convert_python_args_to_c_and_create_index, which is called by the new index create method API's,
    // accepts an optional value of None for ctx
    // This API call is the only exception where a list of ctx's is required
    if (Py_IsNone(py_ctx)) {
        as_error_update(&err, AEROSPIKE_ERR_PARAM, "ctx cannot be None");
        goto CLEANUP_ON_ERROR;
    }

    // Even if this call fails, it will raise its own exception
    // and the err object here will not be set. We don't raise an exception twice
    return convert_python_args_to_c_and_create_index(
        self, py_policy, py_ns, py_set, py_bin, py_name, index_type, data_type,
        py_ctx, NULL);

CLEANUP_ON_ERROR:
    raise_exception_base(&err, Py_None, Py_None, Py_None, Py_None, py_name);
    return NULL;
}
