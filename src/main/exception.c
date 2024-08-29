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
};

#define AEROSPIKE_ERR_EXCEPTION_NAME "AerospikeError"
#define CLIENT_ERR_EXCEPTION_NAME "ClientError"
#define SERVER_ERR_EXCEPTION_NAME "ServerError"
#define CLUSTER_ERR_EXCEPTION_NAME "ClusterError"

// Base classes must be defined before classes that inherit from them
struct exception_def exception_defs[] = {
    {"AerospikeError", NULL, AEROSPIKE_ERR},
    {CLIENT_ERR_EXCEPTION_NAME, AEROSPIKE_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_CLIENT},
    {SERVER_ERR_EXCEPTION_NAME, AEROSPIKE_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_SERVER},
    {"TimeoutError", AEROSPIKE_ERR_EXCEPTION_NAME, AEROSPIKE_ERR_TIMEOUT},
    // Client errors
    {"ParamError", CLIENT_ERR_EXCEPTION_NAME, AEROSPIKE_ERR_PARAM},
    {"InvalidHostError", CLIENT_ERR_EXCEPTION_NAME, AEROSPIKE_ERR_INVALID_HOST},
    {"ConnectionError", CLIENT_ERR_EXCEPTION_NAME, AEROSPIKE_ERR_CONNECTION},
    {"TLSError", CLIENT_ERR_EXCEPTION_NAME, AEROSPIKE_ERR_TLS_ERROR},
    {"BatchFailed", CLIENT_ERR_EXCEPTION_NAME, AEROSPIKE_BATCH_FAILED},
    {"NoResponse", CLIENT_ERR_EXCEPTION_NAME, AEROSPIKE_NO_RESPONSE},
    {"MaxErrorRateExceeded", CLIENT_ERR_EXCEPTION_NAME,
     AEROSPIKE_MAX_ERROR_RATE},
    {"MaxRetriesExceeded", CLIENT_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_MAX_RETRIES_EXCEEDED},
    {"InvalidNodeError", CLIENT_ERR_EXCEPTION_NAME, AEROSPIKE_ERR_INVALID_NODE},
    {"NoMoreConnectionsError", CLIENT_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_NO_MORE_CONNECTIONS},
    {"AsyncConnectionError", CLIENT_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_ASYNC_CONNECTION},
    {"ClientAbortError", CLIENT_ERR_EXCEPTION_NAME, AEROSPIKE_ERR_CLIENT_ABORT},
    // Server errors
    {"InvalidRequest", SERVER_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_REQUEST_INVALID},
    {"ServerFull", SERVER_ERR_EXCEPTION_NAME, AEROSPIKE_ERR_SERVER_FULL},
    {"AlwaysForbidden", SERVER_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_ALWAYS_FORBIDDEN},
    {"UnsupportedFeature", SERVER_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_UNSUPPORTED_FEATURE},
    {"DeviceOverload", SERVER_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_DEVICE_OVERLOAD},
    {"NamespaceNotFound", SERVER_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_NAMESPACE_NOT_FOUND},
    {"ForbiddenError", SERVER_ERR_EXCEPTION_NAME, AEROSPIKE_ERR_FAIL_FORBIDDEN},
    {"QueryError", SERVER_ERR_EXCEPTION_NAME, AEROSPIKE_ERR_QUERY},
    {CLUSTER_ERR_EXCEPTION_NAME, SERVER_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_CLUSTER},
    {"InvalidGeoJSON", SERVER_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_GEO_INVALID_GEOJSON},
    {"OpNotApplicable", SERVER_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_OP_NOT_APPLICABLE},
    {"FilteredOut", SERVER_ERR_EXCEPTION_NAME, AEROSPIKE_FILTERED_OUT},
    {"LostConflict", SERVER_ERR_EXCEPTION_NAME, AEROSPIKE_LOST_CONFLICT},
    {"ScanAbortedError", SERVER_ERR_EXCEPTION_NAME, AEROSPIKE_ERR_SCAN_ABORTED},
    {"ElementNotFoundError", SERVER_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_FAIL_ELEMENT_NOT_FOUND},
    {"ElementExistsError", SERVER_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_FAIL_ELEMENT_EXISTS},
    {"BatchDisabledError", SERVER_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_BATCH_DISABLED},
    {"BatchMaxRequestError", SERVER_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_BATCH_MAX_REQUESTS_EXCEEDED},
    {"BatchQueueFullError", SERVER_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_BATCH_QUEUES_FULL},
    {"QueryAbortedError", SERVER_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_QUERY_ABORTED},
    // Cluster errors
    {"ClusterChangeError", CLUSTER_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_CLUSTER_CHANGE},
    // Record errors
    // 0 means no error code
    {"RecordError", SERVER_ERR_EXCEPTION_NAME, 0},
};

static PyObject *create_py_dict_of_attrs() {}

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

    struct exceptions exceptions_array;

    memset(&exceptions_array, 0, sizeof(exceptions_array));

    struct record_exceptions_struct record_array = {
        {&exceptions_array.RecordKeyMismatch, &exceptions_array.RecordNotFound,
         &exceptions_array.RecordGenerationError,
         &exceptions_array.RecordExistsError, &exceptions_array.RecordTooBig,
         &exceptions_array.RecordBusy, &exceptions_array.BinNameError,
         &exceptions_array.BinIncompatibleType,
         &exceptions_array.BinExistsError, &exceptions_array.BinNotFound},
        {"RecordKeyMismatch", "RecordNotFound", "RecordGenerationError",
         "RecordExistsError", "RecordTooBig", "RecordBusy", "BinNameError",
         "BinIncompatibleType", "BinExistsError", "BinNotFound"},
        {AEROSPIKE_ERR_RECORD_KEY_MISMATCH, AEROSPIKE_ERR_RECORD_NOT_FOUND,
         AEROSPIKE_ERR_RECORD_GENERATION, AEROSPIKE_ERR_RECORD_EXISTS,
         AEROSPIKE_ERR_RECORD_TOO_BIG, AEROSPIKE_ERR_RECORD_BUSY,
         AEROSPIKE_ERR_BIN_NAME, AEROSPIKE_ERR_BIN_INCOMPATIBLE_TYPE,
         AEROSPIKE_ERR_BIN_EXISTS, AEROSPIKE_ERR_BIN_NOT_FOUND}};

    struct index_exceptions_struct index_array = {
        {&exceptions_array.IndexNotFound, &exceptions_array.IndexFoundError,
         &exceptions_array.IndexOOM, &exceptions_array.IndexNotReadable,
         &exceptions_array.IndexNameMaxLen,
         &exceptions_array.IndexNameMaxCount},
        {"IndexNotFound", "IndexFoundError", "IndexOOM", "IndexNotReadable",
         "IndexNameMaxLen", "IndexNameMaxCount"},
        {AEROSPIKE_ERR_INDEX_NOT_FOUND, AEROSPIKE_ERR_INDEX_FOUND,
         AEROSPIKE_ERR_INDEX_OOM, AEROSPIKE_ERR_INDEX_NOT_READABLE,
         AEROSPIKE_ERR_INDEX_NAME_MAXLEN, AEROSPIKE_ERR_INDEX_MAXCOUNT}};

    struct admin_exceptions_struct admin_array = {
        {&exceptions_array.SecurityNotSupported,
         &exceptions_array.SecurityNotEnabled,
         &exceptions_array.SecuritySchemeNotSupported,
         &exceptions_array.InvalidCommand,
         &exceptions_array.InvalidField,
         &exceptions_array.IllegalState,
         &exceptions_array.InvalidUser,
         &exceptions_array.UserExistsError,
         &exceptions_array.InvalidPassword,
         &exceptions_array.ExpiredPassword,
         &exceptions_array.ForbiddenPassword,
         &exceptions_array.InvalidCredential,
         &exceptions_array.InvalidRole,
         &exceptions_array.RoleExistsError,
         &exceptions_array.RoleViolation,
         &exceptions_array.InvalidPrivilege,
         &exceptions_array.NotAuthenticated,
         &exceptions_array.InvalidWhitelist,
         &exceptions_array.NotWhitelisted,
         &exceptions_array.QuotasNotEnabled,
         &exceptions_array.InvalidQuota,
         &exceptions_array.QuotaExceeded},
        {"SecurityNotSupported",
         "SecurityNotEnabled",
         "SecuritySchemeNotSupported",
         "InvalidCommand",
         "InvalidField",
         "IllegalState",
         "InvalidUser",
         "UserExistsError",
         "InvalidPassword",
         "ExpiredPassword",
         "ForbiddenPassword",
         "InvalidCredential",
         "InvalidRole",
         "RoleExistsError",
         "RoleViolation",
         "InvalidPrivilege",
         "NotAuthenticated",
         "InvalidWhitelist",
         "NotWhitelisted",
         "QuotasNotEnabled",
         "InvalidQuota",
         "QuotaExceeded"},
        {AEROSPIKE_SECURITY_NOT_SUPPORTED,
         AEROSPIKE_SECURITY_NOT_ENABLED,
         AEROSPIKE_SECURITY_SCHEME_NOT_SUPPORTED,
         AEROSPIKE_INVALID_COMMAND,
         AEROSPIKE_INVALID_FIELD,
         AEROSPIKE_ILLEGAL_STATE,
         AEROSPIKE_INVALID_USER,
         AEROSPIKE_USER_ALREADY_EXISTS,
         AEROSPIKE_INVALID_PASSWORD,
         AEROSPIKE_EXPIRED_PASSWORD,
         AEROSPIKE_FORBIDDEN_PASSWORD,
         AEROSPIKE_INVALID_CREDENTIAL,
         AEROSPIKE_INVALID_ROLE,
         AEROSPIKE_ROLE_ALREADY_EXISTS,
         AEROSPIKE_ROLE_VIOLATION,
         AEROSPIKE_INVALID_PRIVILEGE,
         AEROSPIKE_NOT_AUTHENTICATED,
         AEROSPIKE_INVALID_WHITELIST,
         AEROSPIKE_NOT_WHITELISTED,
         AEROSPIKE_QUOTAS_NOT_ENABLED,
         AEROSPIKE_INVALID_QUOTA,
         AEROSPIKE_QUOTA_EXCEEDED}};

    PyObject *py_code = NULL;
    PyObject *py_dict = PyDict_New();
    if (py_dict == NULL) {
        goto CLEANUP_ON_ERROR;
    }
    const char *aerospike_exception_attrs[] = {"code", "file", "msg", "line"};
    // TODO: use another macro?
    for (int i = 0; i < sizeof(aerospike_exception_attrs) /
                            sizeof(aerospike_exception_attrs[0]);
         i++) {
        int retval = PyDict_SetItemString(py_dict, aerospike_exception_attrs[i],
                                          Py_None);
        if (retval == -1) {
            goto CLEANUP_ON_ERROR;
        }
    }

    // TODO: in doubt flag missing

    unsigned long exception_count =
        sizeof(exception_defs) / sizeof(exception_defs[0]);
    const char *submodule_name = "exception";
    for (unsigned long i = 0; i < exception_count; i++) {
        struct exception_def exception = exception_defs[i];
        // Create fully qualified name
        char exception_fully_qualified_name[strlen(submodule_name) +
                                            strlen(exception.class_name)];
        strcpy(exception_fully_qualified_name, submodule_name);
        strcat(exception_fully_qualified_name, exception.class_name);

        // TODO: if fetching base class is too slow, cache them using variables
        PyObject *py_base_class = NULL;
        if (exception.base_class_name != NULL) {
            py_base_class =
                PyObject_GetAttrString(py_module, exception.base_class_name);
            if (py_base_class == NULL) {
                goto CLEANUP_ON_ERROR;
            }
        }

        // TODO: same dictionary used for all classes?
        PyObject *py_exception_class = PyErr_NewException(
            exception_fully_qualified_name, py_base_class, py_dict);
        if (py_exception_class == NULL) {
            goto CLEANUP_ON_ERROR;
        }
        Py_DECREF(py_base_class);
        Py_DECREF(py_dict);

        PyObject *py_code = PyLong_FromLong(exception.code);
        if (py_code == NULL) {
            Py_DECREF(py_exception_class);
            goto CLEANUP_ON_ERROR;
        }
        int retval =
            PyObject_SetAttrString(py_exception_class, "code", py_code);
        Py_DECREF(py_code);
        if (retval == -1) {
            Py_DECREF(py_exception_class);
            goto CLEANUP_ON_ERROR;
        }

        retval = PyModule_AddObject(py_module, exception.class_name,
                                    py_exception_class);
        if (retval == -1) {
            Py_DECREF(py_exception_class);
            goto CLEANUP_ON_ERROR;
        }
    }

    //Record exceptions
    PyObject *py_record_dict = PyDict_New();
    PyDict_SetItemString(py_record_dict, "key", Py_None);
    PyDict_SetItemString(py_record_dict, "bin", Py_None);

    exceptions_array.RecordError = PyErr_NewException(
        "exception.RecordError", exceptions_array.ServerError, py_record_dict);
    Py_INCREF(exceptions_array.RecordError);
    Py_DECREF(py_record_dict);
    PyObject_SetAttrString(exceptions_array.RecordError, "code", Py_None);
    PyModule_AddObject(py_module, "RecordError", exceptions_array.RecordError);

    //int count = sizeof(record_exceptions)/sizeof(record_exceptions[0]);
    count = sizeof(record_array.record_exceptions) /
            sizeof(record_array.record_exceptions[0]);
    for (i = 0; i < count; i++) {
        current_exception = record_array.record_exceptions[i];
        char *name = record_array.record_exceptions_name[i];
        char prefix[40] = "exception.";
        *current_exception = PyErr_NewException(
            strcat(prefix, name), exceptions_array.RecordError, NULL);
        Py_INCREF(*current_exception);
        PyModule_AddObject(py_module, name, *current_exception);
        PyObject *py_code =
            PyLong_FromLong(record_array.record_exceptions_codes[i]);
        PyObject_SetAttrString(*current_exception, "code", py_code);
        Py_DECREF(py_code);
    }

    //Index exceptions
    PyObject *py_index_dict = PyDict_New();
    PyDict_SetItemString(py_index_dict, "name", Py_None);

    exceptions_array.IndexError = PyErr_NewException(
        "exception.IndexError", exceptions_array.ServerError, py_index_dict);
    Py_INCREF(exceptions_array.IndexError);
    Py_DECREF(py_index_dict);
    py_code = PyLong_FromLong(AEROSPIKE_ERR_INDEX);
    PyObject_SetAttrString(exceptions_array.IndexError, "code", py_code);
    Py_DECREF(py_code);
    PyModule_AddObject(py_module, "IndexError", exceptions_array.IndexError);

    count = sizeof(index_array.index_exceptions) /
            sizeof(index_array.index_exceptions[0]);
    for (i = 0; i < count; i++) {
        current_exception = index_array.index_exceptions[i];
        char *name = index_array.index_exceptions_name[i];
        char prefix[40] = "exception.";
        *current_exception = PyErr_NewException(
            strcat(prefix, name), exceptions_array.IndexError, NULL);
        Py_INCREF(*current_exception);
        PyModule_AddObject(py_module, name, *current_exception);
        PyObject *py_code =
            PyLong_FromLong(index_array.index_exceptions_codes[i]);
        PyObject_SetAttrString(*current_exception, "code", py_code);
        Py_DECREF(py_code);
    }

    //UDF exceptions
    PyObject *py_udf_dict = PyDict_New();
    PyDict_SetItemString(py_udf_dict, "module", Py_None);
    PyDict_SetItemString(py_udf_dict, "func", Py_None);

    exceptions_array.UDFError = PyErr_NewException(
        "exception.UDFError", exceptions_array.ServerError, py_udf_dict);
    Py_INCREF(exceptions_array.UDFError);
    Py_DECREF(py_udf_dict);
    PyModule_AddObject(py_module, "UDFError", exceptions_array.UDFError);
    py_code = PyLong_FromLong(AEROSPIKE_ERR_UDF);
    PyObject_SetAttrString(exceptions_array.UDFError, "code", py_code);
    Py_DECREF(py_code);

    exceptions_array.UDFNotFound = PyErr_NewException(
        "exception.UDFNotFound", exceptions_array.UDFError, NULL);
    Py_INCREF(exceptions_array.UDFNotFound);
    PyModule_AddObject(py_module, "UDFNotFound", exceptions_array.UDFNotFound);
    py_code = PyLong_FromLong(AEROSPIKE_ERR_UDF_NOT_FOUND);
    PyObject_SetAttrString(exceptions_array.UDFNotFound, "code", py_code);
    Py_DECREF(py_code);

    exceptions_array.LuaFileNotFound = PyErr_NewException(
        "exception.LuaFileNotFound", exceptions_array.UDFError, NULL);
    Py_INCREF(exceptions_array.LuaFileNotFound);
    PyModule_AddObject(py_module, "LuaFileNotFound",
                       exceptions_array.LuaFileNotFound);
    py_code = PyLong_FromLong(AEROSPIKE_ERR_LUA_FILE_NOT_FOUND);
    PyObject_SetAttrString(exceptions_array.LuaFileNotFound, "code", py_code);
    Py_DECREF(py_code);

    //Admin exceptions
    exceptions_array.AdminError = PyErr_NewException(
        "exception.AdminError", exceptions_array.ServerError, NULL);
    Py_INCREF(exceptions_array.AdminError);
    PyObject_SetAttrString(exceptions_array.AdminError, "code", Py_None);
    PyModule_AddObject(py_module, "AdminError", exceptions_array.AdminError);

    count = sizeof(admin_array.admin_exceptions) /
            sizeof(admin_array.admin_exceptions[0]);
    for (i = 0; i < count; i++) {
        current_exception = admin_array.admin_exceptions[i];
        char *name = admin_array.admin_exceptions_name[i];
        char prefix[40] = "exception.";
        *current_exception = PyErr_NewException(
            strcat(prefix, name), exceptions_array.AdminError, NULL);
        Py_INCREF(*current_exception);
        PyModule_AddObject(py_module, name, *current_exception);
        PyObject *py_code =
            PyLong_FromLong(admin_array.admin_exceptions_codes[i]);
        PyObject_SetAttrString(*current_exception, "code", py_code);
        Py_DECREF(py_code);
    }

    //Query exceptions
    exceptions_array.QueryQueueFull = PyErr_NewException(
        "exception.QueryQueueFull", exceptions_array.QueryError, NULL);
    Py_INCREF(exceptions_array.QueryQueueFull);
    PyModule_AddObject(py_module, "QueryQueueFull",
                       exceptions_array.QueryQueueFull);
    py_code = PyLong_FromLong(AEROSPIKE_ERR_QUERY_QUEUE_FULL);
    PyObject_SetAttrString(exceptions_array.QueryQueueFull, "code", py_code);
    Py_DECREF(py_code);

    exceptions_array.QueryTimeout = PyErr_NewException(
        "exception.QueryTimeout", exceptions_array.QueryError, NULL);
    Py_INCREF(exceptions_array.QueryTimeout);
    PyModule_AddObject(py_module, "QueryTimeout",
                       exceptions_array.QueryTimeout);
    py_code = PyLong_FromLong(AEROSPIKE_ERR_QUERY_TIMEOUT);
    PyObject_SetAttrString(exceptions_array.QueryTimeout, "code", py_code);
    Py_DECREF(py_code);

    return py_module;

CLEANUP_ON_ERROR:
    // TODO: use Py_CLEAR()?
    Py_XDECREF(py_dict);
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
