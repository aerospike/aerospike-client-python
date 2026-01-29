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

#include <aerospike/as_error.h>
#include <aerospike/as_log.h>

#include "client.h"
#include "conversions.h"
#include "exceptions.h"
#include "log.h"

bool is_current_log_level_off = true;
PyObject *py_current_custom_callback = NULL;

#ifdef _WIN32
    #define __sync_fetch_and_add InterlockedExchangeAdd64
#endif

volatile int log_counter = 0;

bool default_log_handler(as_log_level level, const char *func, const char *file,
                         uint32_t line, const char *fmt, ...)
{

    char msg[1024];
    va_list ap;

    int counter = __sync_fetch_and_add(&log_counter, 1);

    va_start(ap, fmt);
    vsnprintf(msg, 1024, fmt, ap);
    va_end(ap);

    printf("%d:%d %s\n", getpid(), counter, msg);

    return true;
}

static bool call_custom_py_log_handler(as_log_level level, const char *func,
                                       const char *file, uint32_t line,
                                       const char *fmt, ...)
{

    char msg[1024];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(msg, 1024, fmt, ap);
    va_end(ap);

    // User callback's argument list
    PyObject *py_arglist = NULL;

    // Lock python state
    PyGILState_STATE gstate;
    gstate = PyGILState_Ensure();

    // Create a tuple of argument list
    py_arglist = PyTuple_New(5);

    // Initialise argument variables
    PyObject *log_level = PyLong_FromLong((long)level);
    PyObject *func_name = PyUnicode_FromString(func);
    PyObject *file_name = PyUnicode_FromString(file);
    PyObject *line_no = PyLong_FromUnsignedLong(line);
    PyObject *message = PyUnicode_FromString(msg);

    // Set argument list
    PyTuple_SetItem(py_arglist, 0, log_level);
    PyTuple_SetItem(py_arglist, 1, func_name);
    PyTuple_SetItem(py_arglist, 2, file_name);
    PyTuple_SetItem(py_arglist, 3, line_no);
    PyTuple_SetItem(py_arglist, 4, message);

    // Invoke user callback, passing in argument's list
    PyObject_Call(py_current_custom_callback, py_arglist, NULL);

    Py_DECREF(py_arglist);

    // Release python state
    PyGILState_Release(gstate);

    return true;
}

PyObject *Aerospike_Set_Log_Level(PyObject *parent, PyObject *args,
                                  PyObject *kwds)
{
    as_status status = AEROSPIKE_OK;

    // Python Function Argument Parsing
    static char *kwlist[] = {"loglevel", NULL};
    PyObject *py_log_level = NULL;

    if (PyArg_ParseTupleAndKeywords(args, kwds, "O|:setLogLevel", kwlist,
                                    &py_log_level) == false) {
        return NULL;
    }

    as_error err;
    as_error_init(&err);

    // Type check for incoming parameters
    if (!PyLong_Check(py_log_level)) {
        as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid log level");
        goto CLEANUP;
    }

    long log_level = PyLong_AsLong(py_log_level);
    if (log_level == -1 && PyErr_Occurred()) {
        if (PyErr_ExceptionMatches(PyExc_OverflowError)) {
            as_error_update(&err, AEROSPIKE_ERR_PARAM,
                            "integer value exceeds sys.maxsize");
            goto CLEANUP;
        }
    }

    is_current_log_level_off = log_level == LOG_LEVEL_OFF;

    if (log_level == LOG_LEVEL_OFF) {
        as_log_set_callback(NULL);
    }
    else {
        as_log_set_level((as_log_level)log_level);

        // Re-enable log handler
        if (py_current_custom_callback != NULL) {
            as_log_set_callback((as_log_callback)call_custom_py_log_handler);
        }
        else {
            as_log_set_callback((as_log_callback)default_log_handler);
        }
    }

CLEANUP:

    if (err.code != AEROSPIKE_OK) {
        raise_exception(&err);
        return NULL;
    }

    return PyLong_FromLong(status);
}

PyObject *Aerospike_Set_Log_Handler(PyObject *parent, PyObject *args,
                                    PyObject *kwds)
{
    // Python variables
    PyObject *py_callback = NULL;
    // Python function keyword arguments
    static char *kwlist[] = {"log_handler", NULL};

    // Python function arguments parsing
    if (PyArg_ParseTupleAndKeywords(args, kwds, "|O:setLogHandler", kwlist,
                                    &py_callback) == false) {
        return NULL;
    }

    // Clean up existing log handler
    Py_CLEAR(py_current_custom_callback);

    // 3 cases (when args are passed):
    if (py_callback == NULL) {
        // 1. No args -> enable Python client's default log handler IF log level is not OFF
        // IF log level is OFF, don't enable the default log handler.
        if (!is_current_log_level_off) {
            as_log_set_callback((as_log_callback)default_log_handler);
        }
    }
    else if (Py_IsNone(py_callback)) {
        // Disable log handler altogether
        as_log_set_callback(NULL);
    }
    else if (PyCallable_Check(py_callback)) {
        // Register custom log handler
        py_current_custom_callback = Py_NewRef(py_callback);
        if (!is_current_log_level_off) {
            as_log_set_callback((as_log_callback)call_custom_py_log_handler);
        }
    }

    return PyLong_FromLong(0);
}

void Aerospike_Enable_Default_Logging()
{
    // Invoke C API to set log level
    as_log_set_level((as_log_level)AS_LOG_LEVEL_ERROR);
    // Register callback to C-SDK
    as_log_set_callback((as_log_callback)default_log_handler);

    return;
}
