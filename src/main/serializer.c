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

#include <aerospike/aerospike_key.h>
#include <aerospike/as_key.h>
#include <aerospike/as_error.h>
#include <aerospike/as_record.h>

#include "client.h"
#include "conversions.h"
#include "exceptions.h"
#include "policy.h"
#include "serializer.h"

uint32_t is_user_serializer_registered = 0;
uint32_t is_user_deserializer_registered = 0;

user_serializer_callback user_serializer_call_info, user_deserializer_call_info;

/**
 ******************************************************************************************************
 * Set a serializer in the aerospike database
 *
 * @param self                  Aerospike object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns  integer handle for the serializer being set.
 *******************************************************************************************************
 */
PyObject *AerospikeClient_Set_Serializer(AerospikeClient *self, PyObject *args,
                                         PyObject *kwds)
{
    // Python Function Arguments
    PyObject *py_func = NULL;

    // Python Function Keyword Arguments
    static char *kwlist[] = {"function", NULL};
    as_error err;
    // Initialize error
    as_error_init(&err);

    // Python Function Argument Parsing
    if (PyArg_ParseTupleAndKeywords(args, kwds, "O:set_serializer", kwlist,
                                    &py_func) == false) {
        return NULL;
    }

    if (!is_user_serializer_registered) {
        memset(&user_serializer_call_info, 0,
               sizeof(user_serializer_call_info));
    }

    if (user_serializer_call_info.callback == py_func) {
        return PyLong_FromLong(0);
    }
    if (!PyCallable_Check(py_func)) {
        as_error_update(&err, AEROSPIKE_ERR_PARAM,
                        "Parameter must be a callable");
        goto CLEANUP;
    }

    if (user_serializer_call_info.callback) {
        Py_DECREF(user_serializer_call_info.callback);
    }
    is_user_serializer_registered = 1;
    user_serializer_call_info.callback = py_func;
    Py_INCREF(py_func);

CLEANUP:
    if (err.code != AEROSPIKE_OK) {
        raise_exception(&err);
        return NULL;
    }

    return PyLong_FromLong(0);
}

/**
 ******************************************************************************************************
 * Set a deserializer in the aerospike database
 *
 * @param self                  Aerospike object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns  integer handle for the deserializer being set
 *******************************************************************************************************
 */
PyObject *AerospikeClient_Set_Deserializer(AerospikeClient *self,
                                           PyObject *args, PyObject *kwds)
{
    // Python Function Arguments
    PyObject *py_func = NULL;

    // Python Function Keyword Arguments
    static char *kwlist[] = {"function", NULL};
    as_error err;
    as_error_init(&err);

    // Python Function Argument Parsing
    if (PyArg_ParseTupleAndKeywords(args, kwds, "O:set_deserializer", kwlist,
                                    &py_func) == false) {
        return NULL;
    }

    if (!is_user_deserializer_registered) {
        memset(&user_deserializer_call_info, 0,
               sizeof(user_deserializer_call_info));
    }

    if (user_deserializer_call_info.callback == py_func) {
        return PyLong_FromLong(0);
    }

    if (!PyCallable_Check(py_func)) {
        as_error_update(&err, AEROSPIKE_ERR_PARAM,
                        "Parameter must be a callable");
        goto CLEANUP;
    }
    is_user_deserializer_registered = 1;
    if (user_deserializer_call_info.callback) {
        Py_DECREF(user_deserializer_call_info.callback);
    }
    user_deserializer_call_info.callback = py_func;
    Py_INCREF(py_func);

CLEANUP:
    if (err.code != AEROSPIKE_OK) {
        raise_exception(&err);
        return NULL;
    }

    return PyLong_FromLong(0);
}

/*
 *******************************************************************************************************
 * If dynamic_pool != NULL, executes the passed user_serializer_callback,
 * by creating as_bytes (bytes) from the passed Py_Object (value).
 * Else executes the passed user_deserializer_callback,
 * by passing the as_bytes (bytes) to the deserializer and getting back
 * the corresponding Py_Object (value).
 *
 * @param user_callback_info            The user_serializer_callback for the user
 *                                      callback to be executed.
 * @param bytes                         The as_bytes to be stored/retrieved.
 * @param value                         The value to be retrieved/stored.
 * @param dynamic_pool                The dynamic pool which indicates
 *                                      serialize/deserialize.
 * @param error_p                       The as_error to be populated by the
 *                                      function with encountered error if any.
 *******************************************************************************************************
 */
void execute_user_callback(user_serializer_callback *user_callback_info,
                           as_bytes **bytes, PyObject **value,
                           as_dynamic_pool *dynamic_pool, as_error *error_p)
{
    PyObject *py_return = NULL;
    PyObject *py_value = NULL;
    PyObject *py_arglist = PyTuple_New(1);
    // The buffer must be destroyed by the object (expression, operation, cdt).
    // If not, DESTROY_DYNAMIC_POOL must be called with free_buffer = true to free the serialized value.
    bool destroy_buffers = true;
    bool serialize_data = false;
    if (dynamic_pool) {
        serialize_data = true;
    }

    if (serialize_data) {
        Py_XINCREF(*value);
        if (PyTuple_SetItem(py_arglist, 0, *value) != 0) {
            Py_DECREF(py_arglist);
            goto CLEANUP;
        }
    }
    else {
        as_bytes *bytes_pointer = *bytes;
        char *bytes_val_p = (char *)bytes_pointer->value;
        py_value =
            PyUnicode_FromStringAndSize(bytes_val_p, as_bytes_size(*bytes));
        if (PyTuple_SetItem(py_arglist, 0, py_value) != 0) {
            Py_DECREF(py_arglist);
            goto CLEANUP;
        }
    }

    Py_INCREF(user_callback_info->callback);
    py_return = PyObject_Call(user_callback_info->callback, py_arglist, NULL);
    Py_DECREF(user_callback_info->callback);
    Py_DECREF(py_arglist);

    if (py_return) {
        if (serialize_data) {
            char *py_val;
            Py_ssize_t len;

            py_val = (char *)PyUnicode_AsUTF8AndSize(py_return, &len);
            uint8_t *heap_b =
                (uint8_t *)cf_calloc((uint32_t)len, sizeof(uint8_t));
            memcpy(heap_b, py_val, (uint32_t)len);
            *bytes = GET_BYTES_POOL(dynamic_pool, error_p);
            if (error_p->code == AEROSPIKE_OK) {
                as_bytes_init_wrap(*bytes, heap_b, (int32_t)len,
                                   destroy_buffers);
                Py_DECREF(py_return);
            }
            else {
                Py_DECREF(py_return);
                goto CLEANUP;
            }
        }
        else {
            *value = py_return;
        }
    }
    else {
        if (serialize_data) {
            as_error_update(
                error_p, AEROSPIKE_ERR,
                "Unable to call user's registered serializer callback");
            goto CLEANUP;
        }
        else {
            as_error_update(
                error_p, AEROSPIKE_ERR,
                "Unable to call user's registered deserializer callback");
            goto CLEANUP;
        }
    }

CLEANUP:
    if (error_p->code != AEROSPIKE_OK) {
        raise_exception(error_p);
    }
}

/*
 *******************************************************************************************************
 * Checks serializer_policy.
 * Serializes Py_Object (value) into as_bytes using serialization logic
 * based on serializer_policy.
 *
 * @param serializer_policy         The serializer_policy to be used to handle
 *                                  the serialization. The serializer_policy will be ignored if 
 *                                  user_serilizer_call_info has been set (unless is_client_put_serializer is set).
 * @param bytes                     The as_bytes to be set.
 * @param value                     The value to be serialized.
 * @param error_p                   The as_error to be populated by the function
 *                                  with encountered error if any.
 *******************************************************************************************************
 */
extern as_status serialize_based_on_serializer_policy(
    AerospikeClient *self, int32_t serializer_policy, as_bytes **bytes,
    as_dynamic_pool *dynamic_pool, PyObject *value, as_error *error_p)
{
    uint8_t use_client_serializer = true;
    PyObject *initresult = NULL;

    if (self->is_client_put_serializer) {
        if (serializer_policy == SERIALIZER_USER) {
            if (!self->user_serializer_call_info.callback) {
                use_client_serializer = false;
            }
        }
    }
    else if (self->user_serializer_call_info.callback) {
        serializer_policy = SERIALIZER_USER;
    }

    switch (serializer_policy) {
    case SERIALIZER_NONE:
        as_error_update(error_p, AEROSPIKE_ERR_PARAM,
                        "Cannot serialize: SERIALIZER_NONE selected");
        goto CLEANUP;
    case SERIALIZER_JSON:
        /*
			 *   TODO:
			 *     Handle JSON serialization after support for AS_BYTES_JSON
			 *     is added in aerospike-client-c
			 */
        as_error_update(error_p, AEROSPIKE_ERR,
                        "Unable to serialize using standard json serializer");
        goto CLEANUP;

    case SERIALIZER_USER:
        if (use_client_serializer) {
            execute_user_callback(&self->user_serializer_call_info, bytes,
                                  &value, dynamic_pool, error_p);
            if (AEROSPIKE_OK != (error_p->code)) {
                goto CLEANUP;
            }
        }
        else {
            if (is_user_serializer_registered) {
                execute_user_callback(&user_serializer_call_info, bytes, &value,
                                      dynamic_pool, error_p);
                if (AEROSPIKE_OK != (error_p->code)) {
                    goto CLEANUP;
                }
            }
            else {
                as_error_update(error_p, AEROSPIKE_ERR,
                                "No serializer callback registered");
                goto CLEANUP;
            }
        }
        break;
    default:
        as_error_update(error_p, AEROSPIKE_ERR, "Unsupported serializer");
        goto CLEANUP;
    }

CLEANUP:

    Py_XDECREF(initresult);
    if (error_p->code != AEROSPIKE_OK) {
        raise_exception(error_p);
    }

    return error_p->code;
}

/*
 *******************************************************************************************************
 * Checks as_bytes->type.
 * Deserializes as_bytes into Py_Object (retval) using deserialization logic
 * based on as_bytes->type.
 *
 * @param bytes                 The as_bytes to be deserialized.
 * @param retval                The return zval to be populated with the
 *                              deserialized value of the input as_bytes.
 * @param error_p               The as_error to be populated by the function
 *                              with encountered error if any.
 *******************************************************************************************************
 */
extern as_status deserialize_based_on_as_bytes_type(AerospikeClient *self,
                                                    as_bytes *bytes,
                                                    PyObject **retval,
                                                    as_error *error_p)
{
    switch (as_bytes_get_type(bytes)) {
    case AS_BYTES_PYTHON:;
        // Automatically convert AS_BYTES_PYTHON server types to bytearrays.
        // This prevents the client from throwing an exception and
        // breaking applications that don't handle the exception
        // in case it still fetches AS_BYTES_PYTHON types stored in the server.
        // Applications using this client must deserialize the bytearrays
        // manually with cPickle.
        uint32_t bval_size = as_bytes_size(bytes);
        PyObject *py_val = PyByteArray_FromStringAndSize(
            (char *)as_bytes_get(bytes), bval_size);
        if (!py_val) {
            as_error_update(error_p, AEROSPIKE_ERR_CLIENT,
                            "Unable to deserialize AS_BYTES_PYTHON bytes");
            goto CLEANUP;
        }
        *retval = py_val;
        as_error_update(error_p, AEROSPIKE_OK, NULL);
    case AS_BYTES_BLOB: {
        if (self->user_deserializer_call_info.callback) {
            execute_user_callback(&self->user_deserializer_call_info, &bytes,
                                  retval, NULL, error_p);
            if (AEROSPIKE_OK != (error_p->code)) {
                uint32_t bval_size = as_bytes_size(bytes);
                PyObject *py_val = PyBytes_FromStringAndSize(
                    (char *)as_bytes_get(bytes), bval_size);
                if (!py_val) {
                    as_error_update(error_p, AEROSPIKE_ERR_CLIENT,
                                    "Unable to deserialize bytes");
                    goto CLEANUP;
                }
                *retval = py_val;
                as_error_update(error_p, AEROSPIKE_OK, NULL);
            }
        }
        else {
            if (is_user_deserializer_registered) {
                execute_user_callback(&user_deserializer_call_info, &bytes,
                                      retval, NULL, error_p);
                if (AEROSPIKE_OK != (error_p->code)) {
                    uint32_t bval_size = as_bytes_size(bytes);
                    PyObject *py_val = PyBytes_FromStringAndSize(
                        (char *)as_bytes_get(bytes), bval_size);
                    if (!py_val) {
                        as_error_update(error_p, AEROSPIKE_ERR_CLIENT,
                                        "Unable to deserialize bytes");
                        goto CLEANUP;
                    }
                    as_error_update(error_p, AEROSPIKE_OK, NULL);
                    *retval = py_val;
                }
            }
            else {
                uint32_t bval_size = as_bytes_size(bytes);
                PyObject *py_val = PyBytes_FromStringAndSize(
                    (char *)as_bytes_get(bytes), bval_size);
                if (!py_val) {
                    as_error_update(error_p, AEROSPIKE_ERR_CLIENT,
                                    "Unable to deserialize bytes");
                    goto CLEANUP;
                }
                *retval = py_val;
            }
        }
    } break;
    case AS_BYTES_HLL: {
        // Convert bytes to Python bytes object
        PyObject *py_bytes = PyBytes_FromStringAndSize(
            (const char *)bytes->value, (Py_ssize_t)bytes->size);
        if (py_bytes == NULL) {
            as_error_update(
                error_p, AEROSPIKE_ERR_CLIENT,
                "Unable to convert C client's as_bytes to Python bytes");
            goto CLEANUP;
        }
        PyObject *py_hll = create_class_instance_from_module(
            error_p, "aerospike_helpers", "HyperLogLog", py_bytes);
        Py_DECREF(py_bytes);
        if (!py_hll) {
            goto CLEANUP;
        }
        *retval = py_hll;
        break;
    }
    default: {
        // First try to return a raw byte array, if that fails raise an error
        uint32_t bval_size = as_bytes_size(bytes);
        PyObject *py_val =
            PyBytes_FromStringAndSize((char *)as_bytes_get(bytes), bval_size);
        if (py_val) {
            *retval = py_val;
        }
        else {
            as_error_update(error_p, AEROSPIKE_ERR,
                            "Unable to deserialize bytes");
            goto CLEANUP;
        }
    }
    }

CLEANUP:

    if (error_p->code != AEROSPIKE_OK) {
        raise_exception(error_p);
    }
    // If one of the deserializers failed and the fallback to byte array conversion
    // was successful, we clear any error state left by Python
    PyErr_Clear();
    return error_p->code;
}
PyObject *AerospikeClient_Unset_Serializers(AerospikeClient *self,
                                            PyObject *args, PyObject *kwds)
{
    // Python Function Keyword Arguments
    static char *kwlist[] = {NULL};
    as_error err;
    // Initialize error
    as_error_init(&err);

    // Python Function Argument Parsing
    if (PyArg_ParseTupleAndKeywords(args, kwds, ":unset_serializers", kwlist) ==
        false) {
        return NULL;
    }
    is_user_serializer_registered = 0;
    is_user_deserializer_registered = 0;
    memset(&user_deserializer_call_info, 0,
           sizeof(user_deserializer_call_info));
    memset(&user_serializer_call_info, 0, sizeof(user_serializer_call_info));

    return PyLong_FromLong(0);
}
