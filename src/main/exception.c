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
    const char *class_name;
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

// TODO: idea. define this as a list of tuples in python?
// Base classes must be defined before classes that inherit from them
struct exception_def exception_defs[] = {
    {"AerospikeError", NULL, AEROSPIKE_ERR, aerospike_err_attrs},
    {CLIENT_ERR_EXCEPTION_NAME, AEROSPIKE_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_CLIENT, NULL},
    {SERVER_ERR_EXCEPTION_NAME, AEROSPIKE_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_SERVER, NULL},
    {"TimeoutError", AEROSPIKE_ERR_EXCEPTION_NAME, AEROSPIKE_ERR_TIMEOUT, NULL},
    // Client errors
    {"ParamError", CLIENT_ERR_EXCEPTION_NAME, AEROSPIKE_ERR_PARAM, NULL},
    {"InvalidHostError", CLIENT_ERR_EXCEPTION_NAME, AEROSPIKE_ERR_INVALID_HOST,
     NULL},
    {"ConnectionError", CLIENT_ERR_EXCEPTION_NAME, AEROSPIKE_ERR_CONNECTION,
     NULL},
    {"TLSError", CLIENT_ERR_EXCEPTION_NAME, AEROSPIKE_ERR_TLS_ERROR, NULL},
    {"BatchFailed", CLIENT_ERR_EXCEPTION_NAME, AEROSPIKE_BATCH_FAILED, NULL},
    {"NoResponse", CLIENT_ERR_EXCEPTION_NAME, AEROSPIKE_NO_RESPONSE, NULL},
    {"MaxErrorRateExceeded", CLIENT_ERR_EXCEPTION_NAME,
     AEROSPIKE_MAX_ERROR_RATE, NULL},
    {"MaxRetriesExceeded", CLIENT_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_MAX_RETRIES_EXCEEDED, NULL},
    {"InvalidNodeError", CLIENT_ERR_EXCEPTION_NAME, AEROSPIKE_ERR_INVALID_NODE,
     NULL},
    {"NoMoreConnectionsError", CLIENT_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_NO_MORE_CONNECTIONS, NULL},
    {"AsyncConnectionError", CLIENT_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_ASYNC_CONNECTION, NULL},
    {"ClientAbortError", CLIENT_ERR_EXCEPTION_NAME, AEROSPIKE_ERR_CLIENT_ABORT,
     NULL},
    // Server errors
    {"InvalidRequest", SERVER_ERR_EXCEPTION_NAME, AEROSPIKE_ERR_REQUEST_INVALID,
     NULL},
    {"ServerFull", SERVER_ERR_EXCEPTION_NAME, AEROSPIKE_ERR_SERVER_FULL, NULL},
    {"AlwaysForbidden", SERVER_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_ALWAYS_FORBIDDEN, NULL},
    {"UnsupportedFeature", SERVER_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_UNSUPPORTED_FEATURE, NULL},
    {"DeviceOverload", SERVER_ERR_EXCEPTION_NAME, AEROSPIKE_ERR_DEVICE_OVERLOAD,
     NULL},
    {"NamespaceNotFound", SERVER_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_NAMESPACE_NOT_FOUND, NULL},
    {"ForbiddenError", SERVER_ERR_EXCEPTION_NAME, AEROSPIKE_ERR_FAIL_FORBIDDEN,
     NULL},
    {QUERY_ERR_EXCEPTION_NAME, SERVER_ERR_EXCEPTION_NAME, AEROSPIKE_ERR_QUERY,
     NULL},
    {CLUSTER_ERR_EXCEPTION_NAME, SERVER_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_CLUSTER, NULL},
    {"InvalidGeoJSON", SERVER_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_GEO_INVALID_GEOJSON, NULL},
    {"OpNotApplicable", SERVER_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_OP_NOT_APPLICABLE, NULL},
    {"FilteredOut", SERVER_ERR_EXCEPTION_NAME, AEROSPIKE_FILTERED_OUT, NULL},
    {"LostConflict", SERVER_ERR_EXCEPTION_NAME, AEROSPIKE_LOST_CONFLICT, NULL},
    {"ScanAbortedError", SERVER_ERR_EXCEPTION_NAME, AEROSPIKE_ERR_SCAN_ABORTED,
     NULL},
    {"ElementNotFoundError", SERVER_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_FAIL_ELEMENT_NOT_FOUND, NULL},
    {"ElementExistsError", SERVER_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_FAIL_ELEMENT_EXISTS, NULL},
    {"BatchDisabledError", SERVER_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_BATCH_DISABLED, NULL},
    {"BatchMaxRequestError", SERVER_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_BATCH_MAX_REQUESTS_EXCEEDED, NULL},
    {"BatchQueueFullError", SERVER_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_BATCH_QUEUES_FULL, NULL},
    {"QueryAbortedError", SERVER_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_QUERY_ABORTED, NULL},
    // Cluster errors
    {"ClusterChangeError", CLUSTER_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_CLUSTER_CHANGE, NULL},
    // Record errors
    // RecordError doesn't have an error code. It will be ignored in this case
    {RECORD_ERR_EXCEPTION_NAME, SERVER_ERR_EXCEPTION_NAME, NO_ERROR_CODE,
     record_err_attrs},
    {"RecordKeyMismatch", RECORD_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_RECORD_KEY_MISMATCH, NULL},
    {"RecordNotFound", RECORD_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_RECORD_NOT_FOUND, NULL},
    {"RecordGenerationError", RECORD_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_RECORD_GENERATION, NULL},
    {"RecordExistsError", RECORD_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_RECORD_EXISTS, NULL},
    {"RecordTooBig", RECORD_ERR_EXCEPTION_NAME, AEROSPIKE_ERR_RECORD_TOO_BIG,
     NULL},
    {"RecordBusy", RECORD_ERR_EXCEPTION_NAME, AEROSPIKE_ERR_RECORD_BUSY, NULL},
    {"BinNameError", RECORD_ERR_EXCEPTION_NAME, AEROSPIKE_ERR_BIN_NAME, NULL},
    {"BinIncompatibleType", RECORD_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_BIN_INCOMPATIBLE_TYPE, NULL},
    {"BinExistsError", RECORD_ERR_EXCEPTION_NAME, AEROSPIKE_ERR_BIN_EXISTS,
     NULL},
    {"BinNotFound", RECORD_ERR_EXCEPTION_NAME, AEROSPIKE_ERR_BIN_NOT_FOUND,
     NULL},
    // Index errors
    {INDEX_ERR_EXCEPTION_NAME, SERVER_ERR_EXCEPTION_NAME, AEROSPIKE_ERR_INDEX,
     index_err_attrs},
    {"IndexNotFound", INDEX_ERR_EXCEPTION_NAME, AEROSPIKE_ERR_INDEX_NOT_FOUND,
     NULL},
    {"IndexFoundError", INDEX_ERR_EXCEPTION_NAME, AEROSPIKE_ERR_INDEX_FOUND,
     NULL},
    {"IndexOOM", INDEX_ERR_EXCEPTION_NAME, AEROSPIKE_ERR_INDEX_OOM, NULL},
    {"IndexNotReadable", INDEX_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_INDEX_NOT_READABLE, NULL},
    {"IndexNameMaxLen", INDEX_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_INDEX_NAME_MAXLEN, NULL},
    {"IndexNameMaxCount", INDEX_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_INDEX_MAXCOUNT, NULL},
    // UDF errors
    {UDF_ERR_EXCEPTION_NAME, SERVER_ERR_EXCEPTION_NAME, AEROSPIKE_ERR_UDF,
     udf_err_attrs},
    {"UDFNotFound", UDF_ERR_EXCEPTION_NAME, AEROSPIKE_ERR_UDF_NOT_FOUND, NULL},
    {"LuaFileNotFound", UDF_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_LUA_FILE_NOT_FOUND, NULL},
    // Admin errors
    {ADMIN_ERR_EXCEPTION_NAME, SERVER_ERR_EXCEPTION_NAME, NO_ERROR_CODE, NULL},
    {"SecurityNotSupported", ADMIN_ERR_EXCEPTION_NAME,
     AEROSPIKE_SECURITY_NOT_SUPPORTED, NULL},
    {"SecurityNotEnabled", ADMIN_ERR_EXCEPTION_NAME,
     AEROSPIKE_SECURITY_NOT_ENABLED, NULL},
    {"SecuritySchemeNotSupported", ADMIN_ERR_EXCEPTION_NAME,
     AEROSPIKE_SECURITY_SCHEME_NOT_SUPPORTED, NULL},
    {"InvalidCommand", ADMIN_ERR_EXCEPTION_NAME, AEROSPIKE_INVALID_COMMAND,
     NULL},
    {"InvalidField", ADMIN_ERR_EXCEPTION_NAME, AEROSPIKE_INVALID_FIELD, NULL},
    {"IllegalState", ADMIN_ERR_EXCEPTION_NAME, AEROSPIKE_ILLEGAL_STATE, NULL},
    {"InvalidUser", ADMIN_ERR_EXCEPTION_NAME, AEROSPIKE_INVALID_USER, NULL},
    {"UserExistsError", ADMIN_ERR_EXCEPTION_NAME, AEROSPIKE_USER_ALREADY_EXISTS,
     NULL},
    {"InvalidPassword", ADMIN_ERR_EXCEPTION_NAME, AEROSPIKE_INVALID_PASSWORD,
     NULL},
    {"ExpiredPassword", ADMIN_ERR_EXCEPTION_NAME, AEROSPIKE_EXPIRED_PASSWORD,
     NULL},
    {"ForbiddenPassword", ADMIN_ERR_EXCEPTION_NAME,
     AEROSPIKE_FORBIDDEN_PASSWORD, NULL},
    {"InvalidCredential", ADMIN_ERR_EXCEPTION_NAME,
     AEROSPIKE_INVALID_CREDENTIAL, NULL},
    {"InvalidRole", ADMIN_ERR_EXCEPTION_NAME, AEROSPIKE_INVALID_ROLE, NULL},
    {"RoleExistsError", ADMIN_ERR_EXCEPTION_NAME, AEROSPIKE_ROLE_ALREADY_EXISTS,
     NULL},
    {"RoleViolation", ADMIN_ERR_EXCEPTION_NAME, AEROSPIKE_ROLE_VIOLATION, NULL},
    {"InvalidPrivilege", ADMIN_ERR_EXCEPTION_NAME, AEROSPIKE_INVALID_PRIVILEGE,
     NULL},
    {"NotAuthenticated", ADMIN_ERR_EXCEPTION_NAME, AEROSPIKE_NOT_AUTHENTICATED,
     NULL},
    {"InvalidWhitelist", ADMIN_ERR_EXCEPTION_NAME, AEROSPIKE_INVALID_WHITELIST,
     NULL},
    {"NotWhitelisted", ADMIN_ERR_EXCEPTION_NAME, AEROSPIKE_NOT_WHITELISTED,
     NULL},
    {"QuotasNotEnabled", ADMIN_ERR_EXCEPTION_NAME, AEROSPIKE_QUOTAS_NOT_ENABLED,
     NULL},
    {"InvalidQuota", ADMIN_ERR_EXCEPTION_NAME, AEROSPIKE_INVALID_QUOTA, NULL},
    {"QuotaExceeded", ADMIN_ERR_EXCEPTION_NAME, AEROSPIKE_QUOTA_EXCEEDED, NULL},
    // Query errors
    {"QueryQueueFull", QUERY_ERR_EXCEPTION_NAME, AEROSPIKE_ERR_QUERY_QUEUE_FULL,
     NULL},
    {"QueryTimeout", QUERY_ERR_EXCEPTION_NAME, AEROSPIKE_ERR_QUERY_TIMEOUT,
     NULL},
};

// Returns NULL if an error occurred
// TODO: make sure this aligns with C-API docs
PyObject *AerospikeException_New(void)
{
    static struct PyModuleDef moduledef = {PyModuleDef_HEAD_INIT,
                                           "aerospike.exception",
                                           "Exception objects",
                                           -1,
                                           NULL,
                                           NULL,
                                           NULL,
                                           NULL};
    py_module = PyModule_Create(&moduledef);

    unsigned long exception_count =
        sizeof(exception_defs) / sizeof(exception_defs[0]);
    // TODO: Define variable at moduledef
    const char *submodule_name = "exception";
    for (unsigned long i = 0; i < exception_count; i++) {
        struct exception_def exception = exception_defs[i];
        // Create fully qualified name
        char exception_fully_qualified_name[strlen(submodule_name) + 1 +
                                            strlen(exception.class_name) + 1];
        strcpy(exception_fully_qualified_name, submodule_name);
        strcat(exception_fully_qualified_name, ".");
        strcat(exception_fully_qualified_name, exception.class_name);

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
            exception_fully_qualified_name, py_base_class, py_exc_dict);
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

        retval = PyModule_AddObject(py_module, exception.class_name,
                                    py_exception_class);
        if (retval == -1) {
            goto LOOP_ITERATION_CLEANUP;
        }

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
