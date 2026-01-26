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

#define LOG_LEVEL_OFF -1

static AerospikeLogData user_callback = {
    .py_callback = NULL, .level = AS_LOG_LEVEL_ERROR, .logToConsole = true};

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

static bool call_custom_py_callback(as_log_level level, const char *func,
                                    const char *file, uint32_t line,
                                    const char *fmt, ...)
{

    char msg[1024];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(msg, 1024, fmt, ap);
    va_end(ap);

    // Extract pyhton user callback
    PyObject *py_callback = user_callback.py_callback;
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
    PyObject_Call(py_callback, py_arglist, NULL);

    Py_DECREF(py_arglist);

    // Release python state
    PyGILState_Release(gstate);

    return true;
}

PyObject *Aerospike_Set_Log_Level(PyObject *parent, PyObject *args,
                                  PyObject *kwds)
{
    // Aerospike vaiables
    as_status status = AEROSPIKE_OK;

    // Python Function Keyword Arguments
    static char *kwlist[] = {"loglevel", NULL};

    // Python Function Argument Parsing
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

    if (log_level == LOG_LEVEL_OFF) {
        as_log_set_callback(NULL);
    }
    else {
        as_log_set_level((as_log_level)log_level);

        if (user_callback.py_callback != NULL) {
            as_log_set_callback((as_log_callback)call_custom_py_callback);
        }
        else if (user_callback.logToConsole == true) {
            as_log_set_callback((as_log_callback)default_log_handler);
        }
    }

    user_callback.level = log_level;

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
    as_error err;
    as_error_init(&err);
    // Python function keyword arguments
    static char *kwlist[] = {"log_handler", NULL};

    // Python function arguments parsing
    PyArg_ParseTupleAndKeywords(args, kwds, "|O:setLogHandler", kwlist,
                                &py_callback);

    if (py_callback && PyCallable_Check(py_callback)) {
        // Store user callback
        Py_INCREF(py_callback);
        user_callback.py_callback = py_callback;
        user_callback.logToConsole = false;
        // Check log level to ensure log is enabled
        if (user_callback.level != LOG_LEVEL_OFF) {
            // Register callback to C-SDK
            as_log_set_callback((as_log_callback)call_custom_py_callback);
        }
    }
    else if (py_callback == Py_None) {
        Py_XDECREF(user_callback.py_callback);
        user_callback.py_callback = NULL;
        user_callback.logToConsole = false;
        as_log_set_callback(NULL);
    }
    else {
        // Register callback to C-SDK
        Py_XDECREF(user_callback.py_callback);
        user_callback.py_callback = NULL;

        user_callback.logToConsole = true;
        // Check log level to ensure log is enabled
        if (user_callback.level != LOG_LEVEL_OFF) {
            as_log_set_callback((as_log_callback)default_log_handler);
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
