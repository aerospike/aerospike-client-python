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
#include <aerospike/as_query.h>
#include <aerospike/as_error.h>
#include <aerospike/as_status.h>
#include <aerospike/as_log_macros.h>

#include "conversions.h"
#include <string.h>
#include <stdlib.h>
#include "exceptions.h"
#include "macros.h"

static PyObject *py_exc_module;

#define SUBMODULE_NAME "exception"

//  Used to create a Python Exception class
struct exception_def {
    // When adding the exception to the module, we only need the class name
    // Example: AerospikeError
    const char *class_name;
    // When creating an exception, we need to specify the module name + class name
    // Example: exception.AerospikeError
    const char *fully_qualified_class_name;
    // If NULL, there is no base class
    const char *base_class_name;
    enum as_status_e code;
    // Only applies to base exception classes that need their own fields
    // NULL if this doesn't apply
    const char *const *list_of_attrs;
};

// Used to create instances of the above struct
#define EXCEPTION_DEF(class_name, base_class_name, err_code, attrs)            \
    {                                                                          \
        class_name, SUBMODULE_NAME "." class_name, base_class_name, err_code,  \
            attrs                                                              \
    }

// Base exception names
#define AEROSPIKE_ERR_EXCEPTION_NAME "AerospikeError"
#define CLIENT_ERR_EXCEPTION_NAME "ClientError"
#define SERVER_ERR_EXCEPTION_NAME "ServerError"
#define CLUSTER_ERR_EXCEPTION_NAME "ClusterError"
#define RECORD_ERR_EXCEPTION_NAME "RecordError"
#define INDEX_ERR_EXCEPTION_NAME "IndexError"
#define UDF_ERR_EXCEPTION_NAME "UDFError"
#define ADMIN_ERR_EXCEPTION_NAME "AdminError"
#define QUERY_ERR_EXCEPTION_NAME "QueryError"

// If a base exception doesn't have an error code
// No exception should have an error code of 0, so this should be ok
#define NO_ERROR_CODE 0

// Same order as the tuple of args passed into the exception
const char *const aerospike_err_attrs[] = {"code", "msg",      "file",
                                           "line", "in_doubt", NULL};
const char *const record_err_attrs[] = {"key", "bin", NULL};
const char *const index_err_attrs[] = {"name", NULL};
const char *const udf_err_attrs[] = {"module", "func", NULL};

// TODO: idea. define this as a list of tuples in python?
// Base classes must be defined before classes that inherit from them (topological sorting)
struct exception_def exception_defs[] = {
    EXCEPTION_DEF(AEROSPIKE_ERR_EXCEPTION_NAME, NULL, NO_ERROR_CODE,
                  aerospike_err_attrs),
    EXCEPTION_DEF(CLIENT_ERR_EXCEPTION_NAME, AEROSPIKE_ERR_EXCEPTION_NAME,
                  AEROSPIKE_ERR_CLIENT, NULL),
    EXCEPTION_DEF(SERVER_ERR_EXCEPTION_NAME, AEROSPIKE_ERR_EXCEPTION_NAME,
                  AEROSPIKE_ERR_SERVER, NULL),
    EXCEPTION_DEF("TimeoutError", AEROSPIKE_ERR_EXCEPTION_NAME,
                  AEROSPIKE_ERR_TIMEOUT, NULL),
    // Client errors
    EXCEPTION_DEF("ParamError", CLIENT_ERR_EXCEPTION_NAME, AEROSPIKE_ERR_PARAM,
                  NULL),
    EXCEPTION_DEF("InvalidHostError", CLIENT_ERR_EXCEPTION_NAME,
                  AEROSPIKE_ERR_INVALID_HOST, NULL),
    EXCEPTION_DEF("ConnectionError", CLIENT_ERR_EXCEPTION_NAME,
                  AEROSPIKE_ERR_CONNECTION, NULL),
    EXCEPTION_DEF("TLSError", CLIENT_ERR_EXCEPTION_NAME,
                  AEROSPIKE_ERR_TLS_ERROR, NULL),
    EXCEPTION_DEF("BatchFailed", CLIENT_ERR_EXCEPTION_NAME,
                  AEROSPIKE_BATCH_FAILED, NULL),
    EXCEPTION_DEF("NoResponse", CLIENT_ERR_EXCEPTION_NAME,
                  AEROSPIKE_NO_RESPONSE, NULL),
    EXCEPTION_DEF("MaxErrorRateExceeded", CLIENT_ERR_EXCEPTION_NAME,
                  AEROSPIKE_MAX_ERROR_RATE, NULL),
    EXCEPTION_DEF("MaxRetriesExceeded", CLIENT_ERR_EXCEPTION_NAME,
                  AEROSPIKE_ERR_MAX_RETRIES_EXCEEDED, NULL),
    EXCEPTION_DEF("InvalidNodeError", CLIENT_ERR_EXCEPTION_NAME,
                  AEROSPIKE_ERR_INVALID_NODE, NULL),
    EXCEPTION_DEF("NoMoreConnectionsError", CLIENT_ERR_EXCEPTION_NAME,
                  AEROSPIKE_ERR_NO_MORE_CONNECTIONS, NULL),
    EXCEPTION_DEF("AsyncConnectionError", CLIENT_ERR_EXCEPTION_NAME,
                  AEROSPIKE_ERR_ASYNC_CONNECTION, NULL),
    EXCEPTION_DEF("ClientAbortError", CLIENT_ERR_EXCEPTION_NAME,
                  AEROSPIKE_ERR_CLIENT_ABORT, NULL),
    EXCEPTION_DEF("TransactionFailed", CLIENT_ERR_EXCEPTION_NAME,
                  AEROSPIKE_TXN_FAILED, NULL),
    EXCEPTION_DEF("TransactionAlreadyCommitted", CLIENT_ERR_EXCEPTION_NAME,
                  AEROSPIKE_TXN_ALREADY_COMMITTED, NULL),
    EXCEPTION_DEF("TransactionAlreadyAborted", CLIENT_ERR_EXCEPTION_NAME,
                  AEROSPIKE_TXN_ALREADY_ABORTED, NULL),

    // Server errors
    EXCEPTION_DEF("InvalidRequest", SERVER_ERR_EXCEPTION_NAME,
                  AEROSPIKE_ERR_REQUEST_INVALID, NULL),
    EXCEPTION_DEF("ServerFull", SERVER_ERR_EXCEPTION_NAME,
                  AEROSPIKE_ERR_SERVER_FULL, NULL),
    EXCEPTION_DEF("AlwaysForbidden", SERVER_ERR_EXCEPTION_NAME,
                  AEROSPIKE_ERR_ALWAYS_FORBIDDEN, NULL),
    EXCEPTION_DEF("UnsupportedFeature", SERVER_ERR_EXCEPTION_NAME,
                  AEROSPIKE_ERR_UNSUPPORTED_FEATURE, NULL),
    EXCEPTION_DEF("DeviceOverload", SERVER_ERR_EXCEPTION_NAME,
                  AEROSPIKE_ERR_DEVICE_OVERLOAD, NULL),
    EXCEPTION_DEF("NamespaceNotFound", SERVER_ERR_EXCEPTION_NAME,
                  AEROSPIKE_ERR_NAMESPACE_NOT_FOUND, NULL),
    EXCEPTION_DEF("ForbiddenError", SERVER_ERR_EXCEPTION_NAME,
                  AEROSPIKE_ERR_FAIL_FORBIDDEN, NULL),
    EXCEPTION_DEF(QUERY_ERR_EXCEPTION_NAME, SERVER_ERR_EXCEPTION_NAME,
                  AEROSPIKE_ERR_QUERY, NULL),
    EXCEPTION_DEF(CLUSTER_ERR_EXCEPTION_NAME, SERVER_ERR_EXCEPTION_NAME,
                  AEROSPIKE_ERR_CLUSTER, NULL),
    EXCEPTION_DEF("InvalidGeoJSON", SERVER_ERR_EXCEPTION_NAME,
                  AEROSPIKE_ERR_GEO_INVALID_GEOJSON, NULL),
    EXCEPTION_DEF("OpNotApplicable", SERVER_ERR_EXCEPTION_NAME,
                  AEROSPIKE_ERR_OP_NOT_APPLICABLE, NULL),
    EXCEPTION_DEF("FilteredOut", SERVER_ERR_EXCEPTION_NAME,
                  AEROSPIKE_FILTERED_OUT, NULL),
    EXCEPTION_DEF("LostConflict", SERVER_ERR_EXCEPTION_NAME,
                  AEROSPIKE_LOST_CONFLICT, NULL),
    EXCEPTION_DEF("ScanAbortedError", SERVER_ERR_EXCEPTION_NAME,
                  AEROSPIKE_ERR_SCAN_ABORTED, NULL),
    EXCEPTION_DEF("ElementNotFoundError", SERVER_ERR_EXCEPTION_NAME,
                  AEROSPIKE_ERR_FAIL_ELEMENT_NOT_FOUND, NULL),
    EXCEPTION_DEF("ElementExistsError", SERVER_ERR_EXCEPTION_NAME,
                  AEROSPIKE_ERR_FAIL_ELEMENT_EXISTS, NULL),
    EXCEPTION_DEF("BatchDisabledError", SERVER_ERR_EXCEPTION_NAME,
                  AEROSPIKE_ERR_BATCH_DISABLED, NULL),
    EXCEPTION_DEF("BatchMaxRequestError", SERVER_ERR_EXCEPTION_NAME,
                  AEROSPIKE_ERR_BATCH_MAX_REQUESTS_EXCEEDED, NULL),
    EXCEPTION_DEF("BatchQueueFullError", SERVER_ERR_EXCEPTION_NAME,
                  AEROSPIKE_ERR_BATCH_QUEUES_FULL, NULL),
    EXCEPTION_DEF("QueryAbortedError", SERVER_ERR_EXCEPTION_NAME,
                  AEROSPIKE_ERR_QUERY_ABORTED, NULL),
    // Cluster errors
    EXCEPTION_DEF("ClusterChangeError", CLUSTER_ERR_EXCEPTION_NAME,
                  AEROSPIKE_ERR_CLUSTER_CHANGE, NULL),
    // Record errors
    // RecordError doesn't have an error code. It will be ignored in this case
    EXCEPTION_DEF(RECORD_ERR_EXCEPTION_NAME, SERVER_ERR_EXCEPTION_NAME,
                  NO_ERROR_CODE, record_err_attrs),
    EXCEPTION_DEF("RecordKeyMismatch", RECORD_ERR_EXCEPTION_NAME,
                  AEROSPIKE_ERR_RECORD_KEY_MISMATCH, NULL),
    EXCEPTION_DEF("RecordNotFound", RECORD_ERR_EXCEPTION_NAME,
                  AEROSPIKE_ERR_RECORD_NOT_FOUND, NULL),
    EXCEPTION_DEF("RecordGenerationError", RECORD_ERR_EXCEPTION_NAME,
                  AEROSPIKE_ERR_RECORD_GENERATION, NULL),
    EXCEPTION_DEF("RecordExistsError", RECORD_ERR_EXCEPTION_NAME,
                  AEROSPIKE_ERR_RECORD_EXISTS, NULL),
    EXCEPTION_DEF("RecordTooBig", RECORD_ERR_EXCEPTION_NAME,
                  AEROSPIKE_ERR_RECORD_TOO_BIG, NULL),
    EXCEPTION_DEF("RecordBusy", RECORD_ERR_EXCEPTION_NAME,
                  AEROSPIKE_ERR_RECORD_BUSY, NULL),
    EXCEPTION_DEF("BinNameError", RECORD_ERR_EXCEPTION_NAME,
                  AEROSPIKE_ERR_BIN_NAME, NULL),
    EXCEPTION_DEF("BinIncompatibleType", RECORD_ERR_EXCEPTION_NAME,
                  AEROSPIKE_ERR_BIN_INCOMPATIBLE_TYPE, NULL),
    EXCEPTION_DEF("BinExistsError", RECORD_ERR_EXCEPTION_NAME,
                  AEROSPIKE_ERR_BIN_EXISTS, NULL),
    EXCEPTION_DEF("BinNotFound", RECORD_ERR_EXCEPTION_NAME,
                  AEROSPIKE_ERR_BIN_NOT_FOUND, NULL),
    // Index errors
    EXCEPTION_DEF(INDEX_ERR_EXCEPTION_NAME, SERVER_ERR_EXCEPTION_NAME,
                  AEROSPIKE_ERR_INDEX, index_err_attrs),
    EXCEPTION_DEF("IndexNotFound", INDEX_ERR_EXCEPTION_NAME,
                  AEROSPIKE_ERR_INDEX_NOT_FOUND, NULL),
    EXCEPTION_DEF("IndexFoundError", INDEX_ERR_EXCEPTION_NAME,
                  AEROSPIKE_ERR_INDEX_FOUND, NULL),
    EXCEPTION_DEF("IndexOOM", INDEX_ERR_EXCEPTION_NAME, AEROSPIKE_ERR_INDEX_OOM,
                  NULL),
    EXCEPTION_DEF("IndexNotReadable", INDEX_ERR_EXCEPTION_NAME,
                  AEROSPIKE_ERR_INDEX_NOT_READABLE, NULL),
    EXCEPTION_DEF("IndexNameMaxLen", INDEX_ERR_EXCEPTION_NAME,
                  AEROSPIKE_ERR_INDEX_NAME_MAXLEN, NULL),
    EXCEPTION_DEF("IndexNameMaxCount", INDEX_ERR_EXCEPTION_NAME,
                  AEROSPIKE_ERR_INDEX_MAXCOUNT, NULL),
    // UDF errors
    EXCEPTION_DEF(UDF_ERR_EXCEPTION_NAME, SERVER_ERR_EXCEPTION_NAME,
                  AEROSPIKE_ERR_UDF, udf_err_attrs),
    EXCEPTION_DEF("UDFNotFound", UDF_ERR_EXCEPTION_NAME,
                  AEROSPIKE_ERR_UDF_NOT_FOUND, NULL),
    EXCEPTION_DEF("LuaFileNotFound", UDF_ERR_EXCEPTION_NAME,
                  AEROSPIKE_ERR_LUA_FILE_NOT_FOUND, NULL),
    // Admin errors
    EXCEPTION_DEF(ADMIN_ERR_EXCEPTION_NAME, SERVER_ERR_EXCEPTION_NAME,
                  NO_ERROR_CODE, NULL),
    EXCEPTION_DEF("SecurityNotSupported", ADMIN_ERR_EXCEPTION_NAME,
                  AEROSPIKE_SECURITY_NOT_SUPPORTED, NULL),
    EXCEPTION_DEF("SecurityNotEnabled", ADMIN_ERR_EXCEPTION_NAME,
                  AEROSPIKE_SECURITY_NOT_ENABLED, NULL),
    EXCEPTION_DEF("SecuritySchemeNotSupported", ADMIN_ERR_EXCEPTION_NAME,
                  AEROSPIKE_SECURITY_SCHEME_NOT_SUPPORTED, NULL),
    EXCEPTION_DEF("InvalidCommand", ADMIN_ERR_EXCEPTION_NAME,
                  AEROSPIKE_INVALID_COMMAND, NULL),
    EXCEPTION_DEF("InvalidField", ADMIN_ERR_EXCEPTION_NAME,
                  AEROSPIKE_INVALID_FIELD, NULL),
    EXCEPTION_DEF("IllegalState", ADMIN_ERR_EXCEPTION_NAME,
                  AEROSPIKE_ILLEGAL_STATE, NULL),
    EXCEPTION_DEF("InvalidUser", ADMIN_ERR_EXCEPTION_NAME,
                  AEROSPIKE_INVALID_USER, NULL),
    EXCEPTION_DEF("UserExistsError", ADMIN_ERR_EXCEPTION_NAME,
                  AEROSPIKE_USER_ALREADY_EXISTS, NULL),
    EXCEPTION_DEF("InvalidPassword", ADMIN_ERR_EXCEPTION_NAME,
                  AEROSPIKE_INVALID_PASSWORD, NULL),
    EXCEPTION_DEF("ExpiredPassword", ADMIN_ERR_EXCEPTION_NAME,
                  AEROSPIKE_EXPIRED_PASSWORD, NULL),
    EXCEPTION_DEF("ForbiddenPassword", ADMIN_ERR_EXCEPTION_NAME,
                  AEROSPIKE_FORBIDDEN_PASSWORD, NULL),
    EXCEPTION_DEF("InvalidCredential", ADMIN_ERR_EXCEPTION_NAME,
                  AEROSPIKE_INVALID_CREDENTIAL, NULL),
    EXCEPTION_DEF("InvalidRole", ADMIN_ERR_EXCEPTION_NAME,
                  AEROSPIKE_INVALID_ROLE, NULL),
    EXCEPTION_DEF("RoleExistsError", ADMIN_ERR_EXCEPTION_NAME,
                  AEROSPIKE_ROLE_ALREADY_EXISTS, NULL),
    EXCEPTION_DEF("RoleViolation", ADMIN_ERR_EXCEPTION_NAME,
                  AEROSPIKE_ROLE_VIOLATION, NULL),
    EXCEPTION_DEF("InvalidPrivilege", ADMIN_ERR_EXCEPTION_NAME,
                  AEROSPIKE_INVALID_PRIVILEGE, NULL),
    EXCEPTION_DEF("NotAuthenticated", ADMIN_ERR_EXCEPTION_NAME,
                  AEROSPIKE_NOT_AUTHENTICATED, NULL),
    EXCEPTION_DEF("InvalidWhitelist", ADMIN_ERR_EXCEPTION_NAME,
                  AEROSPIKE_INVALID_WHITELIST, NULL),
    EXCEPTION_DEF("NotWhitelisted", ADMIN_ERR_EXCEPTION_NAME,
                  AEROSPIKE_NOT_WHITELISTED, NULL),
    EXCEPTION_DEF("QuotasNotEnabled", ADMIN_ERR_EXCEPTION_NAME,
                  AEROSPIKE_QUOTAS_NOT_ENABLED, NULL),
    EXCEPTION_DEF("InvalidQuota", ADMIN_ERR_EXCEPTION_NAME,
                  AEROSPIKE_INVALID_QUOTA, NULL),
    EXCEPTION_DEF("QuotaExceeded", ADMIN_ERR_EXCEPTION_NAME,
                  AEROSPIKE_QUOTA_EXCEEDED, NULL),
    // Query errors
    EXCEPTION_DEF("QueryQueueFull", QUERY_ERR_EXCEPTION_NAME,
                  AEROSPIKE_ERR_QUERY_QUEUE_FULL, NULL),
    EXCEPTION_DEF("QueryTimeout", QUERY_ERR_EXCEPTION_NAME,
                  AEROSPIKE_ERR_QUERY_TIMEOUT, NULL)};

// TODO: define aerospike module name somewhere else
#define FULLY_QUALIFIED_MODULE_NAME "aerospike." SUBMODULE_NAME
#define NAME_OF_PY_DICT_MAPPING_ERR_CODE_TO_EXC_CLASS "__errcode_to_exc_class"

// Returns NULL if an error occurred
PyObject *AerospikeException_New(void)
{
    static struct PyModuleDef moduledef = {PyModuleDef_HEAD_INIT,
                                           FULLY_QUALIFIED_MODULE_NAME,
                                           "Exception objects",
                                           -1,
                                           NULL,
                                           NULL,
                                           NULL,
                                           NULL};
    py_exc_module = PyModule_Create(&moduledef);
    if (py_exc_module == NULL) {
        return NULL;
    }

    PyObject *py_dict_errcode_to_exc_class = PyDict_New();
    if (py_dict_errcode_to_exc_class == NULL) {
        goto MODULE_CLEANUP_ON_ERROR;
    }

    int retval = PyModule_AddObject(
        py_exc_module, NAME_OF_PY_DICT_MAPPING_ERR_CODE_TO_EXC_CLASS,
        py_dict_errcode_to_exc_class);
    if (retval == -1) {
        Py_DECREF(py_dict_errcode_to_exc_class);
        goto MODULE_CLEANUP_ON_ERROR;
    }
    // Getting another strong ref to the error code dict isn't necessary
    // As long as the module exists, so will the dictionary

    unsigned long exception_count =
        sizeof(exception_defs) / sizeof(exception_defs[0]);
    for (unsigned long i = 0; i < exception_count; i++) {
        struct exception_def exception_def = exception_defs[i];

        // TODO: if fetching base class is too slow, cache them using variables
        // This only runs once when `import aerospike` is called, though
        // When a module is loaded once through an import, it won't be loaded again
        PyObject *py_base_class = NULL;
        if (exception_def.base_class_name != NULL) {
            py_base_class = PyObject_GetAttrString(
                py_exc_module, exception_def.base_class_name);
            if (py_base_class == NULL) {
                goto MODULE_CLEANUP_ON_ERROR;
            }
        }

        // Set up class attributes
        PyObject *py_exc_dict = NULL;
        if (exception_def.list_of_attrs != NULL) {
            py_exc_dict = PyDict_New();
            if (py_exc_dict == NULL) {
                Py_XDECREF(py_base_class);
                goto MODULE_CLEANUP_ON_ERROR;
            }

            const char *const *curr_attr_ref = exception_def.list_of_attrs;
            while (*curr_attr_ref != NULL) {
                int retval =
                    PyDict_SetItemString(py_exc_dict, *curr_attr_ref, Py_None);
                if (retval == -1) {
                    Py_DECREF(py_exc_dict);
                    Py_XDECREF(py_base_class);
                    goto MODULE_CLEANUP_ON_ERROR;
                }
                curr_attr_ref++;
            }
        }

        PyObject *py_exception_class =
            PyErr_NewException(exception_def.fully_qualified_class_name,
                               py_base_class, py_exc_dict);
        Py_XDECREF(py_base_class);
        Py_XDECREF(py_exc_dict);
        if (py_exception_class == NULL) {
            goto MODULE_CLEANUP_ON_ERROR;
        }

        PyObject *py_code = NULL;
        if (exception_def.code == NO_ERROR_CODE) {
            Py_INCREF(Py_None);
            py_code = Py_None;
        }
        else {
            py_code = PyLong_FromLong(exception_def.code);
            if (py_code == NULL) {
                goto EXC_CLASS_CLEANUP_ON_ERROR;
            }
        }
        int retval =
            PyObject_SetAttrString(py_exception_class, "code", py_code);
        if (retval == -1) {
            goto EXC_CLASS_CLEANUP_ON_ERROR;
        }

        retval = PyDict_SetItem(py_dict_errcode_to_exc_class, py_code,
                                py_exception_class);
        Py_DECREF(py_code);
        if (retval == -1) {
            goto EXC_CLASS_CLEANUP_ON_ERROR;
        }

        retval = PyModule_AddObject(py_exc_module, exception_def.class_name,
                                    py_exception_class);
        if (retval == -1) {
            goto EXC_CLASS_CLEANUP_ON_ERROR;
        }
        continue;

    EXC_CLASS_CLEANUP_ON_ERROR:
        Py_DECREF(py_exception_class);
        goto MODULE_CLEANUP_ON_ERROR;
    }

    return py_exc_module;

MODULE_CLEANUP_ON_ERROR:
    Py_DECREF(py_exc_module);
    return NULL;
}

void remove_exception(as_error *err)
{
    PyObject *py_key = NULL, *py_value = NULL;
    Py_ssize_t pos = 0;
    PyObject *py_module_dict = PyModule_GetDict(py_exc_module);

    while (PyDict_Next(py_module_dict, &pos, &py_key, &py_value)) {
        Py_DECREF(py_value);
    }
}

// Return NULL on error
// Otherwise returns strong reference to exception class
PyObject *get_py_exc_class_from_err_code(as_status err_code)
{
    PyObject *py_dict_err_code = PyObject_GetAttrString(
        py_exc_module, NAME_OF_PY_DICT_MAPPING_ERR_CODE_TO_EXC_CLASS);
    if (py_dict_err_code == NULL) {
        goto error;
    }

    PyObject *py_err_code = PyLong_FromLong(err_code);
    if (py_err_code == NULL) {
        Py_DECREF(py_dict_err_code);
        goto error;
    }

    PyObject *py_exc_class =
        PyDict_GetItemWithError(py_dict_err_code, py_err_code);
    Py_DECREF(py_dict_err_code);
    Py_XDECREF(py_err_code);

    if (py_exc_class == NULL) {
        if (PyErr_Occurred()) {
            goto error;
        }

        // KeyError
        // Exception class could not be found with the error code

        py_exc_class =
            PyObject_GetAttrString(py_exc_module, AEROSPIKE_ERR_EXCEPTION_NAME);
        if (py_exc_class == NULL) {
            goto error;
        }
    }
    else {
        Py_INCREF(py_exc_class);
    }

    return py_exc_class;
error:
    return NULL;
}

enum {
    AS_PY_ERR_TUPLE_CODE = 0,
    AS_PY_ERR_TUPLE_MSG = 1,
    AS_PY_ERR_TUPLE_FILE = 2,
    AS_PY_ERR_TUPLE_LINE = 3,
    AS_PY_ERR_TUPLE_IN_DOUBT = 4
};

#define ERROR_TUPLE_SIZE 5

PyObject *create_pytuple_using_as_error(const as_error *err)
{
    PyObject *py_err_tuple = PyTuple_New(ERROR_TUPLE_SIZE);
    if (py_err_tuple == NULL) {
        goto error;
    }

    Py_ssize_t size_of_py_tuple = PyTuple_Size(py_err_tuple);
    if (size_of_py_tuple == -1) {
        goto CLEANUP_TUPLE_ON_ERROR;
    }

    for (Py_ssize_t i = 0; i < size_of_py_tuple; i++) {
        PyObject *py_member_of_tuple = NULL;
        switch (i) {
        case AS_PY_ERR_TUPLE_CODE:
            py_member_of_tuple = PyLong_FromLongLong(err->code);
            break;
        case AS_PY_ERR_TUPLE_MSG:
            py_member_of_tuple = PyUnicode_FromString(err->message);
            break;
        case AS_PY_ERR_TUPLE_FILE:
            if (err->file) {
                py_member_of_tuple = PyUnicode_FromString(err->file);
            }
            else {
                Py_INCREF(Py_None);
                py_member_of_tuple = Py_None;
            }
            break;
        case AS_PY_ERR_TUPLE_LINE:
            if (err->line > 0) {
                py_member_of_tuple = PyLong_FromLong(err->line);
            }
            else {
                Py_INCREF(Py_None);
                py_member_of_tuple = Py_None;
            }
            break;
        case AS_PY_ERR_TUPLE_IN_DOUBT:
            py_member_of_tuple = PyBool_FromLong(err->in_doubt);
            break;
        default:
            // This codepath should not have executed
            as_log_warn("Invalid index %d when creating a Python error tuple",
                        i);
            break;
        }

        if (py_member_of_tuple == NULL) {
            // One of the above methods to create the member failed
            goto CLEANUP_TUPLE_ON_ERROR;
        }

        int retval = PyTuple_SetItem(py_err_tuple, i, py_member_of_tuple);
        if (retval == -1) {
            goto CLEANUP_TUPLE_ON_ERROR;
        }
    }

    return py_err_tuple;

CLEANUP_TUPLE_ON_ERROR:
    Py_DECREF(py_err_tuple);
error:
    return NULL;
}

void raise_exception(as_error *err)
{
    raise_exception_base(err, NULL, NULL, NULL, NULL, NULL);
}

void raise_exception_base(as_error *err, PyObject *py_as_key, PyObject *py_bin,
                          PyObject *py_module, PyObject *py_func,
                          PyObject *py_name)
{
// If there was an exception already raised, we need to chain it to the one we're raising now
#if PY_MAJOR_VERSION == 3 && PY_MINOR_VERSION >= 12
    PyObject *py_prev_exc = PyErr_GetRaisedException();
#else
    PyObject *py_prev_type, *py_prev_value, *py_prev_traceback;
    PyErr_Fetch(&py_prev_type, &py_prev_value, &py_prev_traceback);
#endif

    PyObject *py_exc_class = get_py_exc_class_from_err_code(err->code);
    if (py_exc_class == NULL) {
        goto CHAIN_PREV_EXC_AND_RETURN;
    }

    const char *extra_attrs[] = {"key", "bin", "module", "func", "name"};
    PyObject *py_extra_attrs[] = {py_as_key, py_bin, py_module, py_func,
                                  py_name};
    for (unsigned long i = 0;
         i < sizeof(py_extra_attrs) / sizeof(py_extra_attrs[0]); i++) {
        PyObject *py_exc_extra_attr =
            PyObject_GetAttrString(py_exc_class, extra_attrs[i]);
        if (py_exc_extra_attr) {
            PyObject_SetAttrString(py_exc_class, extra_attrs[i],
                                   py_extra_attrs[i]);
            Py_DECREF(py_exc_extra_attr);
        }
        else if (PyErr_ExceptionMatches(PyExc_AttributeError)) {
            // We are sure that we want to ignore this
            PyErr_Clear();
        }
        else {
            // This happens if the code that converts a C client error to a Python exception fails.
            // The caller of this function should be returning because of an exception anyways
            goto CHAIN_PREV_EXC_AND_RETURN;
        }
    }

    // Convert borrowed reference of exception class to strong reference
    // Py_INCREF(py_exc_class);

    // Convert C error to Python exception
    PyObject *py_err_tuple = create_pytuple_using_as_error(err);
    if (py_err_tuple == NULL) {
        // Py_DECREF(py_exc_class);
        goto CHAIN_PREV_EXC_AND_RETURN;
    }

    for (unsigned long i = 0;
         i < sizeof(aerospike_err_attrs) / sizeof(aerospike_err_attrs[0]) - 1;
         i++) {
        // Here, we are assuming the number of attrs is the same as the number of tuple members
        PyObject *py_arg = PyTuple_GetItem(py_err_tuple, i);
        if (py_arg == NULL) {
            break;
        }
        PyObject_SetAttrString(py_exc_class, aerospike_err_attrs[i], py_arg);
    }

    // Raise exception
    PyErr_SetObject(py_exc_class, py_err_tuple);
    Py_DECREF(py_err_tuple);

CHAIN_PREV_EXC_AND_RETURN:
#if PY_MAJOR_VERSION == 3 && PY_MINOR_VERSION >= 12
    if (py_prev_exc) {
        // Like PyErr_SetRaisedException, which steals a reference to the parameter
        _PyErr_ChainExceptions1(py_prev_exc);
    }
#else
    if (py_prev_type) {
        // Like PyErr_Restore, which steals a reference to the parameter
        _PyErr_ChainExceptions(py_prev_type, py_prev_value, py_prev_traceback);
    }
#endif
}
