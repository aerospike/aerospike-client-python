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

// TODO: idea. define this as a list of tuples in python?
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
    {QUERY_ERR_EXCEPTION_NAME, SERVER_ERR_EXCEPTION_NAME, AEROSPIKE_ERR_QUERY},
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
    // RecordError doesn't have an error code. It will be ignored in this case
    {RECORD_ERR_EXCEPTION_NAME, SERVER_ERR_EXCEPTION_NAME, NO_ERROR_CODE},
    {"RecordKeyMismatch", RECORD_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_RECORD_KEY_MISMATCH},
    {"RecordNotFound", RECORD_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_RECORD_NOT_FOUND},
    {"RecordGenerationError", RECORD_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_RECORD_GENERATION},
    {"RecordExistsError", RECORD_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_RECORD_EXISTS},
    {"RecordTooBig", RECORD_ERR_EXCEPTION_NAME, AEROSPIKE_ERR_RECORD_TOO_BIG},
    {"RecordBusy", RECORD_ERR_EXCEPTION_NAME, AEROSPIKE_ERR_RECORD_BUSY},
    {"BinNameError", RECORD_ERR_EXCEPTION_NAME, AEROSPIKE_ERR_BIN_NAME},
    {"BinIncompatibleType", RECORD_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_BIN_INCOMPATIBLE_TYPE},
    {"BinExistsError", RECORD_ERR_EXCEPTION_NAME, AEROSPIKE_ERR_BIN_EXISTS},
    {"BinNotFound", RECORD_ERR_EXCEPTION_NAME, AEROSPIKE_ERR_BIN_NOT_FOUND},
    // Index errors
    {INDEX_ERR_EXCEPTION_NAME, SERVER_ERR_EXCEPTION_NAME, AEROSPIKE_ERR_INDEX},
    {"IndexNotFound", INDEX_ERR_EXCEPTION_NAME, AEROSPIKE_ERR_INDEX_NOT_FOUND},
    {"IndexFoundError", INDEX_ERR_EXCEPTION_NAME, AEROSPIKE_ERR_INDEX_FOUND},
    {"IndexOOM", INDEX_ERR_EXCEPTION_NAME, AEROSPIKE_ERR_INDEX_OOM},
    {"IndexNotReadable", INDEX_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_INDEX_NOT_READABLE},
    {"IndexNameMaxLen", INDEX_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_INDEX_NAME_MAXLEN},
    {"IndexNameMaxCount", INDEX_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_INDEX_MAXCOUNT},
    // UDF errors
    {UDF_ERR_EXCEPTION_NAME, SERVER_ERR_EXCEPTION_NAME, AEROSPIKE_ERR_UDF},
    {"UDFNotFound", UDF_ERR_EXCEPTION_NAME, AEROSPIKE_ERR_UDF_NOT_FOUND},
    {"LuaFileNotFound", UDF_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_LUA_FILE_NOT_FOUND},
    // Admin errors
    {ADMIN_ERR_EXCEPTION_NAME, SERVER_ERR_EXCEPTION_NAME, NO_ERROR_CODE},
    {"SecurityNotSupported", ADMIN_ERR_EXCEPTION_NAME,
     AEROSPIKE_SECURITY_NOT_SUPPORTED},
    {"SecurityNotEnabled", ADMIN_ERR_EXCEPTION_NAME,
     AEROSPIKE_SECURITY_NOT_ENABLED},
    {"SecuritySchemeNotSupported", ADMIN_ERR_EXCEPTION_NAME,
     AEROSPIKE_SECURITY_SCHEME_NOT_SUPPORTED},
    {"InvalidCommand", ADMIN_ERR_EXCEPTION_NAME, AEROSPIKE_INVALID_COMMAND},
    {"InvalidField", ADMIN_ERR_EXCEPTION_NAME, AEROSPIKE_INVALID_FIELD},
    {"IllegalState", ADMIN_ERR_EXCEPTION_NAME, AEROSPIKE_ILLEGAL_STATE},
    {"InvalidUser", ADMIN_ERR_EXCEPTION_NAME, AEROSPIKE_INVALID_USER},
    {"UserExistsError", ADMIN_ERR_EXCEPTION_NAME,
     AEROSPIKE_USER_ALREADY_EXISTS},
    {"InvalidPassword", ADMIN_ERR_EXCEPTION_NAME, AEROSPIKE_INVALID_PASSWORD},
    {"ExpiredPassword", ADMIN_ERR_EXCEPTION_NAME, AEROSPIKE_EXPIRED_PASSWORD},
    {"ForbiddenPassword", ADMIN_ERR_EXCEPTION_NAME,
     AEROSPIKE_FORBIDDEN_PASSWORD},
    {"InvalidCredential", ADMIN_ERR_EXCEPTION_NAME,
     AEROSPIKE_INVALID_CREDENTIAL},
    {"InvalidRole", ADMIN_ERR_EXCEPTION_NAME, AEROSPIKE_INVALID_ROLE},
    {"RoleExistsError", ADMIN_ERR_EXCEPTION_NAME,
     AEROSPIKE_ROLE_ALREADY_EXISTS},
    {"RoleViolation", ADMIN_ERR_EXCEPTION_NAME, AEROSPIKE_ROLE_VIOLATION},
    {"InvalidPrivilege", ADMIN_ERR_EXCEPTION_NAME, AEROSPIKE_INVALID_PRIVILEGE},
    {"NotAuthenticated", ADMIN_ERR_EXCEPTION_NAME, AEROSPIKE_NOT_AUTHENTICATED},
    {"InvalidWhitelist", ADMIN_ERR_EXCEPTION_NAME, AEROSPIKE_INVALID_WHITELIST},
    {"NotWhitelisted", ADMIN_ERR_EXCEPTION_NAME, AEROSPIKE_NOT_WHITELISTED},
    {"QuotasNotEnabled", ADMIN_ERR_EXCEPTION_NAME,
     AEROSPIKE_QUOTAS_NOT_ENABLED},
    {"InvalidQuota", ADMIN_ERR_EXCEPTION_NAME, AEROSPIKE_INVALID_QUOTA},
    {"QuotaExceeded", ADMIN_ERR_EXCEPTION_NAME, AEROSPIKE_QUOTA_EXCEEDED},
    // Query errors
    {"QueryQueueFull", QUERY_ERR_EXCEPTION_NAME,
     AEROSPIKE_ERR_QUERY_QUEUE_FULL},
    {"QueryTimeout", QUERY_ERR_EXCEPTION_NAME, AEROSPIKE_ERR_QUERY_TIMEOUT},
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

    struct exceptions exceptions_array;

    memset(&exceptions_array, 0, sizeof(exceptions_array));

    // Exception attrs

    PyObject *py_aerospike_exception_dict = NULL;
    const char *aerospike_exception_attrs[] = {
        "code", "file", "msg", "line"
        // TODO: in doubt flag missing
    };

    PyObject *py_record_exception_dict = NULL;
    const char *record_exception_attrs[] = {
        "key",
        "bin",
    };

    PyObject *py_index_exception_dict = NULL;
    const char *index_exception_attrs[] = {
        // TODO: this doesn't match the docs
        "name",
    };

    PyObject *py_udf_exception_dict = NULL;
    const char *udf_exception_attrs[] = {"module", "func"};

    struct {
        PyObject **ref_to_py_dict;
        char **attr_list
    } mapper[] = {{&py_aerospike_exception_dict, aerospike_exception_attrs},
                  {&py_record_exception_dict, record_exception_attrs},
                  {&py_index_exception_dict, index_exception_attrs},
                  {&py_udf_exception_dict, udf_exception_attrs}};

    for (int i = 0; i < sizeof(mapper) / sizeof(mapper[0]); i++) {
        PyObject *py_dict = PyDict_New();
        if (py_dict == NULL) {
            goto CLEANUP;
        }

        // TODO: use another macro?
        char **attr_list = mapper[i].attr_list;
        for (int i = 0; i < sizeof(attr_list) / sizeof(attr_list[0]); i++) {
            int retval = PyDict_SetItemString(py_dict, attr_list[i], Py_None);
            if (retval == -1) {
                // TODO: cleanup properly
                goto CLEANUP;
            }
        }
        *(mapper[i].ref_to_py_dict) = py_dict;
        continue;

    CLEANUP:
        Py_XDECREF(py_dict);
        return NULL;
    }

    unsigned long exception_count =
        sizeof(exception_defs) / sizeof(exception_defs[0]);
    // TODO: Define variable at moduledef
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

        // TODO: Move this into struct somehow?
        bool is_exc_record_error =
            strcmp(exception.class_name, RECORD_ERR_EXCEPTION_NAME) == 0;
        bool is_exc_descendent_of_record_error =
            strcmp(exception.base_class_name, RECORD_ERR_EXCEPTION_NAME) == 0;
        bool is_exc_index_error =
            strcmp(exception.class_name, INDEX_ERR_EXCEPTION_NAME) == 0;
        bool is_exc_udf_error =
            strcmp(exception.class_name, UDF_ERR_EXCEPTION_NAME) == 0;
        bool is_exc_admin_error =
            strcmp(exception.class_name, ADMIN_ERR_EXCEPTION_NAME) == 0;
        bool is_exc_desc_of_admin_error =
            strcmp(exception.base_class_name, ADMIN_ERR_EXCEPTION_NAME) == 0;
        bool is_exc_desc_of_query_error =
            strcmp(exception.base_class_name, QUERY_ERR_EXCEPTION_NAME) == 0;
        PyObject *py_exc_dict = NULL;
        if (is_exc_descendent_of_record_error || is_exc_admin_error ||
            is_exc_desc_of_admin_error || is_exc_desc_of_query_error) {
            py_exc_dict = NULL;
        }
        else if (is_exc_record_error) {
            py_exc_dict = py_record_exception_dict;
        }
        else if (is_exc_index_error) {
            py_exc_dict = py_index_exception_dict;
        }
        else if (is_exc_udf_error) {
            py_exc_dict = py_udf_exception_dict;
        }
        else {
            // TODO: only need to assign this to AerospikeError?
            py_exc_dict = py_aerospike_exception_dict;
        }

        // TODO: same dictionary used for all classes?
        PyObject *py_exception_class = PyErr_NewException(
            exception_fully_qualified_name, py_base_class, py_exc_dict);
        if (py_exception_class == NULL) {
            goto CLEANUP_ON_ERROR;
        }
        Py_DECREF(py_base_class);
        Py_DECREF(py_exc_dict);

        PyObject *py_code = NULL;
        if (is_exc_record_error || is_exc_admin_error) {
            py_code = Py_None;
        }
        else {
            py_code = PyLong_FromLong(exception.code);
            if (py_code == NULL) {
                Py_DECREF(py_exception_class);
                goto CLEANUP_ON_ERROR;
            }
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

    return py_module;

CLEANUP_ON_ERROR:
    for (int i = 0; i < sizeof(mapper) / sizeof(mapper[0]); i++) {
        // TODO: use Py_CLEAR()?
        PyObject *py_dict = *mapper[i].ref_to_py_dict;
        Py_DECREF(py_dict);
    }
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
