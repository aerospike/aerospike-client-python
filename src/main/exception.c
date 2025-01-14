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

#include "conversions.h"
#include <string.h>
#include <stdlib.h>
#include "exceptions.h"
#include "exception_types.h"
#include "macros.h"

static PyObject *py_module;

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
    py_module = PyModule_Create(&moduledef);
    if (py_module == NULL) {
        return NULL;
    }

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
                py_module, exception_def.base_class_name);
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
        Py_DECREF(py_code);
        if (retval == -1) {
            goto EXC_CLASS_CLEANUP_ON_ERROR;
        }

        retval = PyModule_AddObject(py_module, exception_def.class_name,
                                    py_exception_class);
        if (retval == -1) {
            goto EXC_CLASS_CLEANUP_ON_ERROR;
        }
        continue;

    EXC_CLASS_CLEANUP_ON_ERROR:
        Py_DECREF(py_exception_class);
        goto MODULE_CLEANUP_ON_ERROR;
    }

    return py_module;

MODULE_CLEANUP_ON_ERROR:
    Py_DECREF(py_module);
    return NULL;
}

void remove_exception(as_error *err)
{
    PyObject *py_key = NULL, *py_value = NULL;
    Py_ssize_t pos = 0;
    PyObject *py_module_dict = PyModule_GetDict(py_module);

    while (PyDict_Next(py_module_dict, &pos, &py_key, &py_value)) {
        Py_DECREF(py_value);
    }
}

// We have this as a separate method because both raise_exception and raise_exception_old need to use it
void set_aerospike_exc_attrs_using_tuple_of_attrs(PyObject *py_exc,
                                                  PyObject *py_tuple)
{
    for (unsigned long i = 0;
         i < sizeof(aerospike_err_attrs) / sizeof(aerospike_err_attrs[0]) - 1;
         i++) {
        // Here, we are assuming the number of attrs is the same as the number of tuple members
        PyObject *py_arg = PyTuple_GetItem(py_tuple, i);
        if (py_arg == NULL) {
            // Don't fail out if number of attrs > number of tuple members
            // This condition should never be true, though
            PyErr_Clear();
            break;
        }
        PyObject_SetAttrString(py_exc, aerospike_err_attrs[i], py_arg);
    }
}

// TODO: idea. Use python dict to map error code to exception
void raise_exception(as_error *err)
{
    PyObject *py_key = NULL, *py_value = NULL;
    Py_ssize_t pos = 0;
    PyObject *py_module_dict = PyModule_GetDict(py_module);
    bool found = false;

    while (PyDict_Next(py_module_dict, &pos, &py_key, &py_value)) {
        if (PyObject_HasAttrString(py_value, "code")) {
            PyObject *py_code = PyObject_GetAttrString(py_value, "code");
            if (py_code == Py_None) {
                continue;
            }
            if (err->code == PyLong_AsLong(py_code)) {
                found = true;
                break;
            }
        }
    }
    // We haven't found the right exception, just use AerospikeError
    if (!found) {
        PyObject *base_exception =
            PyDict_GetItemString(py_module_dict, "AerospikeError");
        if (base_exception) {
            py_value = base_exception;
        }
    }

    // Convert borrowed reference of exception class to strong reference
    Py_INCREF(py_value);

    // Convert C error to Python exception
    PyObject *py_err = NULL;
    error_to_pyobject(err, &py_err);
    set_aerospike_exc_attrs_using_tuple_of_attrs(py_value, py_err);

    // Raise exception
    PyErr_SetObject(py_value, py_err);

    Py_DECREF(py_value);
    Py_DECREF(py_err);
}

PyObject *raise_exception_old(as_error *err)
{
    PyObject *py_key = NULL, *py_value = NULL;
    Py_ssize_t pos = 0;
    PyObject *py_module_dict = PyModule_GetDict(py_module);
    bool found = false;

    while (PyDict_Next(py_module_dict, &pos, &py_key, &py_value)) {
        if (PyObject_HasAttrString(py_value, "code")) {
            PyObject *py_code = PyObject_GetAttrString(py_value, "code");
            if (py_code == Py_None) {
                continue;
            }
            if (err->code == PyLong_AsLong(py_code)) {
                found = true;
                PyObject *py_attr = NULL;
                py_attr = PyUnicode_FromString(err->message);
                PyObject_SetAttrString(py_value, "msg", py_attr);
                Py_DECREF(py_attr);

                // as_error.file is a char* so this may be null
                if (err->file) {
                    py_attr = PyUnicode_FromString(err->file);
                    PyObject_SetAttrString(py_value, "file", py_attr);
                    Py_DECREF(py_attr);
                }
                else {
                    PyObject_SetAttrString(py_value, "file", Py_None);
                }
                // If the line is 0, set it as None
                if (err->line > 0) {
                    py_attr = PyLong_FromLong(err->line);
                    PyObject_SetAttrString(py_value, "line", py_attr);
                    Py_DECREF(py_attr);
                }
                else {
                    PyObject_SetAttrString(py_value, "line", Py_None);
                }

                PyObject_SetAttrString(py_value, "in_doubt",
                                       PyBool_FromLong(err->in_doubt));

                break;
            }
            Py_DECREF(py_code);
        }
    }
    // We haven't found the right exception, just use AerospikeError
    if (!found) {
        PyObject *base_exception =
            PyDict_GetItemString(py_module_dict, "AerospikeError");
        if (base_exception) {
            py_value = base_exception;
        }
    }
    return py_value;
}
