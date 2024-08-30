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

struct exception_def {
    // Example: exception.AerospikeError
    const char *fully_qualified_class_name;
    // If NULL, there is no base class
    const char *base_class_name;
    enum as_status_e code;
    // Only applies to base exception classes that need their own fields
    // NULL if this doesn't apply
    const char *const *list_of_attrs;
};

// Parent exception names that other exceptions inherit from
#define AEROSPIKE_ERR_EXCEPTION_NAME "AerospikeError"
#define CLIENT_ERR_EXCEPTION_NAME "ClientError"
#define SERVER_ERR_EXCEPTION_NAME "ServerError"
#define CLUSTER_ERR_EXCEPTION_NAME "ClusterError"
#define RECORD_ERR_EXCEPTION_NAME "RecordError"
#define INDEX_ERR_EXCEPTION_NAME "IndexError"
#define UDF_ERR_EXCEPTION_NAME "UDFError"
#define ADMIN_ERR_EXCEPTION_NAME "AdminError"
#define QUERY_ERR_EXCEPTION_NAME "QueryError"

#define NO_ERROR_CODE 0

const char *const aerospike_err_attrs[] = {"code", "file", "msg", "line", NULL};
const char *const record_err_attrs[] = {"key", "bin", NULL};
const char *const index_err_attrs[] = {"name", NULL};
const char *const udf_err_attrs[] = {"module", "func", NULL};

// We have to prepend this to every class name
// otherwise we have to dynamically allocate memory for each class name
// which will take longer to initialize this module
#define SUBMODULE_NAME "exception"
#define PREPEND_SUBMODULE_NAME(class_name) SUBMODULE_NAME "." class_name

// TODO: idea. define this as a list of tuples in python?
// Base classes must be defined before classes that inherit from them
struct exception_def exception_defs[] = {
    {PREPEND_SUBMODULE_NAME(AEROSPIKE_ERR_EXCEPTION_NAME), NULL, NO_ERROR_CODE,
     aerospike_err_attrs},
    {PREPEND_SUBMODULE_NAME(CLIENT_ERR_EXCEPTION_NAME),
     AEROSPIKE_ERR_EXCEPTION_NAME, AEROSPIKE_ERR_CLIENT, NULL},
    {PREPEND_SUBMODULE_NAME(SERVER_ERR_EXCEPTION_NAME),
     AEROSPIKE_ERR_EXCEPTION_NAME, AEROSPIKE_ERR_SERVER, NULL},
    {PREPEND_SUBMODULE_NAME("TimeoutError"), AEROSPIKE_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_TIMEOUT, NULL},
    // Client errors
    {PREPEND_SUBMODULE_NAME("ParamError"), CLIENT_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_PARAM, NULL},
    {PREPEND_SUBMODULE_NAME("InvalidHostError"), CLIENT_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_INVALID_HOST, NULL},
    {PREPEND_SUBMODULE_NAME("ConnectionError"), CLIENT_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_CONNECTION, NULL},
    {PREPEND_SUBMODULE_NAME("TLSError"), CLIENT_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_TLS_ERROR, NULL},
    {PREPEND_SUBMODULE_NAME("BatchFailed"), CLIENT_ERR_EXCEPTION_NAME,
     AEROSPIKE_BATCH_FAILED, NULL},
    {PREPEND_SUBMODULE_NAME("NoResponse"), CLIENT_ERR_EXCEPTION_NAME,
     AEROSPIKE_NO_RESPONSE, NULL},
    {PREPEND_SUBMODULE_NAME("MaxErrorRateExceeded"), CLIENT_ERR_EXCEPTION_NAME,
     AEROSPIKE_MAX_ERROR_RATE, NULL},
    {PREPEND_SUBMODULE_NAME("MaxRetriesExceeded"), CLIENT_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_MAX_RETRIES_EXCEEDED, NULL},
    {PREPEND_SUBMODULE_NAME("InvalidNodeError"), CLIENT_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_INVALID_NODE, NULL},
    {PREPEND_SUBMODULE_NAME("NoMoreConnectionsError"),
     CLIENT_ERR_EXCEPTION_NAME, AEROSPIKE_ERR_NO_MORE_CONNECTIONS, NULL},
    {PREPEND_SUBMODULE_NAME("AsyncConnectionError"), CLIENT_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_ASYNC_CONNECTION, NULL},
    {PREPEND_SUBMODULE_NAME("ClientAbortError"), CLIENT_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_CLIENT_ABORT, NULL},
    // Server errors
    {PREPEND_SUBMODULE_NAME("InvalidRequest"), SERVER_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_REQUEST_INVALID, NULL},
    {PREPEND_SUBMODULE_NAME("ServerFull"), SERVER_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_SERVER_FULL, NULL},
    {PREPEND_SUBMODULE_NAME("AlwaysForbidden"), SERVER_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_ALWAYS_FORBIDDEN, NULL},
    {PREPEND_SUBMODULE_NAME("UnsupportedFeature"), SERVER_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_UNSUPPORTED_FEATURE, NULL},
    {PREPEND_SUBMODULE_NAME("DeviceOverload"), SERVER_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_DEVICE_OVERLOAD, NULL},
    {PREPEND_SUBMODULE_NAME("NamespaceNotFound"), SERVER_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_NAMESPACE_NOT_FOUND, NULL},
    {PREPEND_SUBMODULE_NAME("ForbiddenError"), SERVER_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_FAIL_FORBIDDEN, NULL},
    {PREPEND_SUBMODULE_NAME(QUERY_ERR_EXCEPTION_NAME),
     SERVER_ERR_EXCEPTION_NAME, AEROSPIKE_ERR_QUERY, NULL},
    {PREPEND_SUBMODULE_NAME(CLUSTER_ERR_EXCEPTION_NAME),
     SERVER_ERR_EXCEPTION_NAME, AEROSPIKE_ERR_CLUSTER, NULL},
    {PREPEND_SUBMODULE_NAME("InvalidGeoJSON"), SERVER_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_GEO_INVALID_GEOJSON, NULL},
    {PREPEND_SUBMODULE_NAME("OpNotApplicable"), SERVER_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_OP_NOT_APPLICABLE, NULL},
    {PREPEND_SUBMODULE_NAME("FilteredOut"), SERVER_ERR_EXCEPTION_NAME,
     AEROSPIKE_FILTERED_OUT, NULL},
    {PREPEND_SUBMODULE_NAME("LostConflict"), SERVER_ERR_EXCEPTION_NAME,
     AEROSPIKE_LOST_CONFLICT, NULL},
    {PREPEND_SUBMODULE_NAME("ScanAbortedError"), SERVER_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_SCAN_ABORTED, NULL},
    {PREPEND_SUBMODULE_NAME("ElementNotFoundError"), SERVER_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_FAIL_ELEMENT_NOT_FOUND, NULL},
    {PREPEND_SUBMODULE_NAME("ElementExistsError"), SERVER_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_FAIL_ELEMENT_EXISTS, NULL},
    {PREPEND_SUBMODULE_NAME("BatchDisabledError"), SERVER_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_BATCH_DISABLED, NULL},
    {PREPEND_SUBMODULE_NAME("BatchMaxRequestError"), SERVER_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_BATCH_MAX_REQUESTS_EXCEEDED, NULL},
    {PREPEND_SUBMODULE_NAME("BatchQueueFullError"), SERVER_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_BATCH_QUEUES_FULL, NULL},
    {PREPEND_SUBMODULE_NAME("QueryAbortedError"), SERVER_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_QUERY_ABORTED, NULL},
    // Cluster errors
    {PREPEND_SUBMODULE_NAME("ClusterChangeError"), CLUSTER_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_CLUSTER_CHANGE, NULL},
    // Record errors
    // RecordError doesn't have an error code. It will be ignored in this case
    {PREPEND_SUBMODULE_NAME(RECORD_ERR_EXCEPTION_NAME),
     SERVER_ERR_EXCEPTION_NAME, NO_ERROR_CODE, record_err_attrs},
    {PREPEND_SUBMODULE_NAME("RecordKeyMismatch"), RECORD_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_RECORD_KEY_MISMATCH, NULL},
    {PREPEND_SUBMODULE_NAME("RecordNotFound"), RECORD_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_RECORD_NOT_FOUND, NULL},
    {PREPEND_SUBMODULE_NAME("RecordGenerationError"), RECORD_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_RECORD_GENERATION, NULL},
    {PREPEND_SUBMODULE_NAME("RecordExistsError"), RECORD_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_RECORD_EXISTS, NULL},
    {PREPEND_SUBMODULE_NAME("RecordTooBig"), RECORD_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_RECORD_TOO_BIG, NULL},
    {PREPEND_SUBMODULE_NAME("RecordBusy"), RECORD_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_RECORD_BUSY, NULL},
    {PREPEND_SUBMODULE_NAME("BinNameError"), RECORD_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_BIN_NAME, NULL},
    {PREPEND_SUBMODULE_NAME("BinIncompatibleType"), RECORD_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_BIN_INCOMPATIBLE_TYPE, NULL},
    {PREPEND_SUBMODULE_NAME("BinExistsError"), RECORD_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_BIN_EXISTS, NULL},
    {PREPEND_SUBMODULE_NAME("BinNotFound"), RECORD_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_BIN_NOT_FOUND, NULL},
    // Index errors
    {PREPEND_SUBMODULE_NAME(INDEX_ERR_EXCEPTION_NAME),
     SERVER_ERR_EXCEPTION_NAME, AEROSPIKE_ERR_INDEX, index_err_attrs},
    {PREPEND_SUBMODULE_NAME("IndexNotFound"), INDEX_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_INDEX_NOT_FOUND, NULL},
    {PREPEND_SUBMODULE_NAME("IndexFoundError"), INDEX_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_INDEX_FOUND, NULL},
    {PREPEND_SUBMODULE_NAME("IndexOOM"), INDEX_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_INDEX_OOM, NULL},
    {PREPEND_SUBMODULE_NAME("IndexNotReadable"), INDEX_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_INDEX_NOT_READABLE, NULL},
    {PREPEND_SUBMODULE_NAME("IndexNameMaxLen"), INDEX_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_INDEX_NAME_MAXLEN, NULL},
    {PREPEND_SUBMODULE_NAME("IndexNameMaxCount"), INDEX_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_INDEX_MAXCOUNT, NULL},
    // UDF errors
    {PREPEND_SUBMODULE_NAME(UDF_ERR_EXCEPTION_NAME), SERVER_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_UDF, udf_err_attrs},
    {PREPEND_SUBMODULE_NAME("UDFNotFound"), UDF_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_UDF_NOT_FOUND, NULL},
    {PREPEND_SUBMODULE_NAME("LuaFileNotFound"), UDF_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_LUA_FILE_NOT_FOUND, NULL},
    // Admin errors
    {ADMIN_ERR_EXCEPTION_NAME, SERVER_ERR_EXCEPTION_NAME, NO_ERROR_CODE, NULL},
    {PREPEND_SUBMODULE_NAME("SecurityNotSupported"), ADMIN_ERR_EXCEPTION_NAME,
     AEROSPIKE_SECURITY_NOT_SUPPORTED, NULL},
    {PREPEND_SUBMODULE_NAME("SecurityNotEnabled"), ADMIN_ERR_EXCEPTION_NAME,
     AEROSPIKE_SECURITY_NOT_ENABLED, NULL},
    {PREPEND_SUBMODULE_NAME("SecuritySchemeNotSupported"),
     ADMIN_ERR_EXCEPTION_NAME, AEROSPIKE_SECURITY_SCHEME_NOT_SUPPORTED, NULL},
    {PREPEND_SUBMODULE_NAME("InvalidCommand"), ADMIN_ERR_EXCEPTION_NAME,
     AEROSPIKE_INVALID_COMMAND, NULL},
    {PREPEND_SUBMODULE_NAME("InvalidField"), ADMIN_ERR_EXCEPTION_NAME,
     AEROSPIKE_INVALID_FIELD, NULL},
    {PREPEND_SUBMODULE_NAME("IllegalState"), ADMIN_ERR_EXCEPTION_NAME,
     AEROSPIKE_ILLEGAL_STATE, NULL},
    {PREPEND_SUBMODULE_NAME("InvalidUser"), ADMIN_ERR_EXCEPTION_NAME,
     AEROSPIKE_INVALID_USER, NULL},
    {PREPEND_SUBMODULE_NAME("UserExistsError"), ADMIN_ERR_EXCEPTION_NAME,
     AEROSPIKE_USER_ALREADY_EXISTS, NULL},
    {PREPEND_SUBMODULE_NAME("InvalidPassword"), ADMIN_ERR_EXCEPTION_NAME,
     AEROSPIKE_INVALID_PASSWORD, NULL},
    {PREPEND_SUBMODULE_NAME("ExpiredPassword"), ADMIN_ERR_EXCEPTION_NAME,
     AEROSPIKE_EXPIRED_PASSWORD, NULL},
    {PREPEND_SUBMODULE_NAME("ForbiddenPassword"), ADMIN_ERR_EXCEPTION_NAME,
     AEROSPIKE_FORBIDDEN_PASSWORD, NULL},
    {PREPEND_SUBMODULE_NAME("InvalidCredential"), ADMIN_ERR_EXCEPTION_NAME,
     AEROSPIKE_INVALID_CREDENTIAL, NULL},
    {PREPEND_SUBMODULE_NAME("InvalidRole"), ADMIN_ERR_EXCEPTION_NAME,
     AEROSPIKE_INVALID_ROLE, NULL},
    {PREPEND_SUBMODULE_NAME("RoleExistsError"), ADMIN_ERR_EXCEPTION_NAME,
     AEROSPIKE_ROLE_ALREADY_EXISTS, NULL},
    {PREPEND_SUBMODULE_NAME("RoleViolation"), ADMIN_ERR_EXCEPTION_NAME,
     AEROSPIKE_ROLE_VIOLATION, NULL},
    {PREPEND_SUBMODULE_NAME("InvalidPrivilege"), ADMIN_ERR_EXCEPTION_NAME,
     AEROSPIKE_INVALID_PRIVILEGE, NULL},
    {PREPEND_SUBMODULE_NAME("NotAuthenticated"), ADMIN_ERR_EXCEPTION_NAME,
     AEROSPIKE_NOT_AUTHENTICATED, NULL},
    {PREPEND_SUBMODULE_NAME("InvalidWhitelist"), ADMIN_ERR_EXCEPTION_NAME,
     AEROSPIKE_INVALID_WHITELIST, NULL},
    {PREPEND_SUBMODULE_NAME("NotWhitelisted"), ADMIN_ERR_EXCEPTION_NAME,
     AEROSPIKE_NOT_WHITELISTED, NULL},
    {PREPEND_SUBMODULE_NAME("QuotasNotEnabled"), ADMIN_ERR_EXCEPTION_NAME,
     AEROSPIKE_QUOTAS_NOT_ENABLED, NULL},
    {PREPEND_SUBMODULE_NAME("InvalidQuota"), ADMIN_ERR_EXCEPTION_NAME,
     AEROSPIKE_INVALID_QUOTA, NULL},
    {PREPEND_SUBMODULE_NAME("QuotaExceeded"), ADMIN_ERR_EXCEPTION_NAME,
     AEROSPIKE_QUOTA_EXCEEDED, NULL},
    // Query errors
    {PREPEND_SUBMODULE_NAME("QueryQueueFull"), QUERY_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_QUERY_QUEUE_FULL, NULL},
    {PREPEND_SUBMODULE_NAME("QueryTimeout"), QUERY_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_QUERY_TIMEOUT, NULL},
};

// TODO: define aerospike module name somewhere else
#define FULLY_QUALIFIED_MODULE_NAME "aerospike." SUBMODULE_NAME

// Returns NULL if an error occurred
// TODO: make sure this aligns with C-API docs
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

    unsigned long exception_count =
        sizeof(exception_defs) / sizeof(exception_defs[0]);
    for (unsigned long i = 0; i < exception_count; i++) {
        struct exception_def exception = exception_defs[i];

        // TODO: if fetching base class is too slow, cache them using variables
        // I think this only runs once when `import aerospike` is called, though
        PyObject *py_base_class = NULL;
        if (exception.base_class_name != NULL) {
            py_base_class =
                PyObject_GetAttrString(py_module, exception.base_class_name);
            if (py_base_class == NULL) {
                return NULL;
            }
        }

        PyObject *py_exc_dict = NULL;
        if (exception.list_of_attrs != NULL) {
            py_exc_dict = PyDict_New();
            if (py_exc_dict == NULL) {
                return NULL;
            }

            const char *const *curr_attr_ref = exception.list_of_attrs;
            while (*curr_attr_ref != NULL) {
                int retval =
                    PyDict_SetItemString(py_exc_dict, *curr_attr_ref, Py_None);
                if (retval == -1) {
                    Py_DECREF(py_exc_dict);
                    return NULL;
                }
                curr_attr_ref++;
            }
        }

        // TODO: same dictionary used for all classes?
        PyObject *py_exception_class = PyErr_NewException(
            (const char *)exception.fully_qualified_class_name, py_base_class,
            py_exc_dict);
        if (py_exception_class == NULL) {
            return NULL;
        }
        Py_XDECREF(py_base_class);
        Py_XDECREF(py_exc_dict);

        PyObject *py_code = NULL;
        if (exception.code == NO_ERROR_CODE) {
            py_code = Py_None;
        }
        else {
            py_code = PyLong_FromLong(exception.code);
            if (py_code == NULL) {
                goto LOOP_ITERATION_CLEANUP;
            }
        }
        int retval =
            PyObject_SetAttrString(py_exception_class, "code", py_code);
        Py_DECREF(py_code);
        if (retval == -1) {
            goto LOOP_ITERATION_CLEANUP;
        }

        retval =
            PyModule_AddObject(py_module, exception.fully_qualified_class_name,
                               py_exception_class);
        if (retval == -1) {
            goto LOOP_ITERATION_CLEANUP;
        }
        continue;

    LOOP_ITERATION_CLEANUP:
        Py_DECREF(py_exception_class);
        return NULL;
    }

    return py_module;
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

                py_attr = PyBool_FromLong(err->in_doubt);
                PyObject_SetAttrString(py_value, "in_doubt", py_attr);
                Py_DECREF(py_attr);

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

    // Convert borrowed reference of exception class to strong reference
    Py_INCREF(py_value);

    // Convert C error to Python exception
    PyObject *py_err = NULL;
    error_to_pyobject(err, &py_err);

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
