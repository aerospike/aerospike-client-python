/*******************************************************************************
 * Copyright 2013-2016 Aerospike, Inc.
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

static PyObject *module;

PyObject * AerospikeException_New(void)
{
	MOD_DEF(module, "aerospike.exception", "Exception objects", NULL);

	struct exceptions exceptions_array;

	memset(&exceptions_array, 0, sizeof(exceptions_array));

	struct server_exceptions_struct server_array = { 
		{&exceptions_array.InvalidRequest, &exceptions_array.ServerFull, &exceptions_array.NoXDR, 
			&exceptions_array.UnsupportedFeature, &exceptions_array.DeviceOverload, &exceptions_array.NamespaceNotFound, 
			&exceptions_array.ForbiddenError, &exceptions_array.QueryError, &exceptions_array.ClusterError, 
			&exceptions_array.InvalidGeoJSON},
		{"InvalidRequest", "ServerFull", "NoXDR", "UnsupportedFeature", "DeviceOverload", "NamespaceNotFound", 
			"ForbiddenError", "QueryError", "ClusterError", "InvalidGeoJSON"},
		{AEROSPIKE_ERR_REQUEST_INVALID, AEROSPIKE_ERR_SERVER_FULL, AEROSPIKE_ERR_NO_XDR, 
			AEROSPIKE_ERR_UNSUPPORTED_FEATURE, AEROSPIKE_ERR_DEVICE_OVERLOAD, AEROSPIKE_ERR_NAMESPACE_NOT_FOUND,  
			AEROSPIKE_ERR_FAIL_FORBIDDEN, AEROSPIKE_ERR_QUERY, AEROSPIKE_ERR_CLUSTER, AEROSPIKE_ERR_GEO_INVALID_GEOJSON}
	};

	struct record_exceptions_struct record_array = { 
		{&exceptions_array.RecordKeyMismatch, &exceptions_array.RecordNotFound, &exceptions_array.RecordGenerationError, 
			&exceptions_array.RecordExistsError, &exceptions_array.RecordTooBig, &exceptions_array.RecordBusy, 
			&exceptions_array.BinNameError, &exceptions_array.BinExistsError, &exceptions_array.BinNotFound, 
			&exceptions_array.BinIncompatibleType},
		{"RecordKeyMismatch", "RecordNotFound", "RecordGenerationError", "RecordExistsError", "RecordTooBig", "RecordBusy", 
			"BinNameError", "BinExistsError", "BinNotFound", "BinIncompatibleType"},
		{AEROSPIKE_ERR_RECORD_KEY_MISMATCH, AEROSPIKE_ERR_RECORD_NOT_FOUND, AEROSPIKE_ERR_RECORD_GENERATION, 
			AEROSPIKE_ERR_RECORD_EXISTS, AEROSPIKE_ERR_RECORD_TOO_BIG, AEROSPIKE_ERR_RECORD_BUSY, 
			AEROSPIKE_ERR_BIN_NAME, AEROSPIKE_ERR_BIN_EXISTS, AEROSPIKE_ERR_BIN_NOT_FOUND, 
			AEROSPIKE_ERR_BIN_INCOMPATIBLE_TYPE}
	};

	struct index_exceptions_struct index_array = { 
		{&exceptions_array.IndexNotFound, &exceptions_array.IndexFoundError, &exceptions_array.IndexOOM, 
			&exceptions_array.IndexNotReadable, &exceptions_array.IndexNameMaxLen, &exceptions_array.IndexNameMaxCount},
		{"IndexNotFound", "IndexFoundError", "IndexOOM", "IndexNotReadable", "IndexNameMaxLen", "IndexNameMaxCount"},
		{AEROSPIKE_ERR_INDEX_NOT_FOUND, AEROSPIKE_ERR_INDEX_FOUND, AEROSPIKE_ERR_INDEX_OOM, 
			AEROSPIKE_ERR_INDEX_NOT_READABLE, AEROSPIKE_ERR_INDEX_NAME_MAXLEN, AEROSPIKE_ERR_INDEX_MAXCOUNT}
	};

	struct admin_exceptions_struct admin_array = { 
		{&exceptions_array.SecurityNotSupported, &exceptions_array.SecurityNotEnabled, &exceptions_array.SecuritySchemeNotSupported, 
			&exceptions_array.InvalidCommand, &exceptions_array.InvalidField, &exceptions_array.IllegalState, 
			&exceptions_array.InvalidUser, &exceptions_array.UserExistsError, &exceptions_array.InvalidPassword, 
			&exceptions_array.ExpiredPassword, &exceptions_array.ForbiddenPassword, &exceptions_array.InvalidCredential, 
			&exceptions_array.InvalidRole, &exceptions_array.RoleExistsError, &exceptions_array.RoleViolation, 
			&exceptions_array.InvalidPrivilege, &exceptions_array.NotAuthenticated},
		{"SecurityNotSupported", "SecurityNotEnabled", "SecuritySchemeNotSupported", "InvalidCommand", "InvalidField", 
			"IllegalState", "InvalidUser", "UserExistsError", "InvalidPassword", "ExpiredPassword", "ForbiddenPassword", 
			"InvalidCredential", "InvalidRole", "RoleExistsError", "RoleViolation", "InvalidPrivilege", "NotAuthenticated"},
		{AEROSPIKE_SECURITY_NOT_SUPPORTED, AEROSPIKE_SECURITY_NOT_ENABLED, AEROSPIKE_SECURITY_SCHEME_NOT_SUPPORTED, 
			AEROSPIKE_INVALID_COMMAND, AEROSPIKE_INVALID_FIELD, AEROSPIKE_ILLEGAL_STATE, 
			AEROSPIKE_INVALID_USER, AEROSPIKE_USER_ALREADY_EXISTS, AEROSPIKE_INVALID_PASSWORD, 
			AEROSPIKE_EXPIRED_PASSWORD, AEROSPIKE_FORBIDDEN_PASSWORD, AEROSPIKE_INVALID_CREDENTIAL, 
			AEROSPIKE_INVALID_ROLE, AEROSPIKE_ROLE_ALREADY_EXISTS, AEROSPIKE_ROLE_VIOLATION,  
			AEROSPIKE_INVALID_PRIVILEGE, AEROSPIKE_NOT_AUTHENTICATED}
	};

	struct ldt_exceptions_struct ldt_array = { 
		{&exceptions_array.LargeItemNotFound, &exceptions_array.LDTInternalError, &exceptions_array.LDTNotFound, 
			&exceptions_array.LDTUniqueKeyError, &exceptions_array.LDTInsertError, &exceptions_array.LDTSearchError, 
			&exceptions_array.LDTDeleteError, &exceptions_array.LDTInputParamError, &exceptions_array.LDTTypeMismatch, 
			&exceptions_array.LDTBinNameNull, &exceptions_array.LDTBinNameNotString, &exceptions_array.LDTBinNameTooLong, 
			&exceptions_array.LDTTooManyOpenSubrecs, &exceptions_array.LDTTopRecNotFound, &exceptions_array.LDTSubRecNotFound, 
			&exceptions_array.LDTBinNotFound, &exceptions_array.LDTBinExistsError, &exceptions_array.LDTBinDamaged, 
			&exceptions_array.LDTSubrecPoolDamaged, &exceptions_array.LDTSubrecDamaged, &exceptions_array.LDTSubrecOpenError, 
			&exceptions_array.LDTSubrecUpdateError, &exceptions_array.LDTSubrecCreateError, &exceptions_array.LDTSubrecDeleteError, 
			&exceptions_array.LDTSubrecCloseError, &exceptions_array.LDTToprecUpdateError, &exceptions_array.LDTToprecCreateError, 
			&exceptions_array.LDTFilterFunctionBad, &exceptions_array.LDTFilterFunctionNotFound, &exceptions_array.LDTKeyFunctionBad, 
			&exceptions_array.LDTKeyFunctionNotFound, &exceptions_array.LDTTransFunctionBad, &exceptions_array.LDTTransFunctionNotFound, 
			&exceptions_array.LDTUntransFunctionBad, &exceptions_array.LDTUntransFunctionNotFound, &exceptions_array.LDTUserModuleBad, 
			&exceptions_array.LDTUserModuleNotFound},
		{"LargeItemNotFound", "LDTInternalError", "LDTNotFound", "LDTUniqueKeyError", "LDTInsertError", "LDTSearchError", 
			"LDTDeleteError", "LDTInputParamError", "LDTTypeMismatch", "LDTBinNameNull", "LDTBinNameNotString", "LDTBinNameTooLong", 
			"LDTTooManyOpenSubrecs", "LDTTopRecNotFound", "LDTSubRecNotFound", "LDTBinNotFound", "LDTBinExistsError", "LDTBinDamaged", 
			"LDTSubrecPoolDamaged", "LDTSubrecDamaged", "LDTSubrecOpenError", "LDTSubrecUpdateError", "LDTSubrecCreateError", 
			"LDTSubrecDeleteError", "LDTSubrecCloseError", "LDTToprecUpdateError", "LDTToprecCreateError", "LDTFilterFunctionBad",
			"LDTFilterFunctionNotFound", "LDTKeyFunctionBad", "LDTKeyFunctionNotFound", "LDTTransFunctionBad", "LDTTransFunctionNotFound", 
			"LDTUntransFunctionBad", "LDTUntransFunctionNotFound", "LDTUserModuleBad", "LDTUserModuleNotFound"},
		{AEROSPIKE_ERR_LARGE_ITEM_NOT_FOUND, AEROSPIKE_ERR_LDT_INTERNAL, AEROSPIKE_ERR_LDT_NOT_FOUND, 
			AEROSPIKE_ERR_LDT_UNIQUE_KEY, AEROSPIKE_ERR_LDT_INSERT, AEROSPIKE_ERR_LDT_SEARCH, 
			AEROSPIKE_ERR_LDT_DELETE, AEROSPIKE_ERR_LDT_INPUT_PARM, AEROSPIKE_ERR_LDT_TYPE_MISMATCH, 
			AEROSPIKE_ERR_LDT_NULL_BIN_NAME, AEROSPIKE_ERR_LDT_BIN_NAME_NOT_STRING, AEROSPIKE_ERR_LDT_BIN_NAME_TOO_LONG, 
			AEROSPIKE_ERR_LDT_TOO_MANY_OPEN_SUBRECS, AEROSPIKE_ERR_LDT_TOP_REC_NOT_FOUND, AEROSPIKE_ERR_LDT_SUB_REC_NOT_FOUND, 
			AEROSPIKE_ERR_LDT_BIN_DOES_NOT_EXIST, AEROSPIKE_ERR_LDT_BIN_ALREADY_EXISTS, AEROSPIKE_ERR_LDT_BIN_DAMAGED, 
			AEROSPIKE_ERR_LDT_SUBREC_POOL_DAMAGED, AEROSPIKE_ERR_LDT_SUBREC_DAMAGED, AEROSPIKE_ERR_LDT_SUBREC_OPEN, 
			AEROSPIKE_ERR_LDT_SUBREC_UPDATE, AEROSPIKE_ERR_LDT_SUBREC_CREATE, AEROSPIKE_ERR_LDT_SUBREC_DELETE, 
			AEROSPIKE_ERR_LDT_SUBREC_CLOSE, AEROSPIKE_ERR_LDT_TOPREC_UPDATE, AEROSPIKE_ERR_LDT_TOPREC_CREATE, 
			AEROSPIKE_ERR_LDT_FILTER_FUNCTION_BAD, AEROSPIKE_ERR_LDT_FILTER_FUNCTION_NOT_FOUND, AEROSPIKE_ERR_LDT_KEY_FUNCTION_BAD, 
			AEROSPIKE_ERR_LDT_KEY_FUNCTION_NOT_FOUND, AEROSPIKE_ERR_LDT_TRANS_FUNCTION_BAD, AEROSPIKE_ERR_LDT_TRANS_FUNCTION_NOT_FOUND, 
			AEROSPIKE_ERR_LDT_UNTRANS_FUNCTION_BAD, AEROSPIKE_ERR_LDT_UNTRANS_FUNCTION_NOT_FOUND, AEROSPIKE_ERR_LDT_USER_MODULE_BAD, 
			AEROSPIKE_ERR_LDT_USER_MODULE_NOT_FOUND}
	};

	PyObject *py_code = NULL;
	PyObject *py_dict = PyDict_New();
	PyDict_SetItemString(py_dict, "code", Py_None);
	PyDict_SetItemString(py_dict, "file", Py_None);
	PyDict_SetItemString(py_dict, "msg", Py_None);
	PyDict_SetItemString(py_dict, "line", Py_None);

	exceptions_array.AerospikeError = PyErr_NewException("exception.AerospikeError", NULL, py_dict);
	Py_INCREF(exceptions_array.AerospikeError);
	Py_DECREF(py_dict);
	PyModule_AddObject(module, "AerospikeError", exceptions_array.AerospikeError);
	PyObject_SetAttrString(exceptions_array.AerospikeError, "code", Py_None);

	exceptions_array.ClientError = PyErr_NewException("exception.ClientError", exceptions_array.AerospikeError, NULL);
	Py_INCREF(exceptions_array.ClientError);
	PyModule_AddObject(module, "ClientError", exceptions_array.ClientError);
	py_code = PyInt_FromLong(AEROSPIKE_ERR_CLIENT);
	PyObject_SetAttrString(exceptions_array.ClientError, "code", py_code);
	Py_DECREF(py_code);

	exceptions_array.ServerError = PyErr_NewException("exception.ServerError", exceptions_array.AerospikeError, NULL);
	Py_INCREF(exceptions_array.ServerError);
	PyModule_AddObject(module, "ServerError", exceptions_array.ServerError);
	py_code = PyInt_FromLong(AEROSPIKE_ERR_SERVER);
	PyObject_SetAttrString(exceptions_array.ServerError, "code", py_code);
	Py_DECREF(py_code);

	exceptions_array.TimeoutError = PyErr_NewException("exception.TimeoutError", exceptions_array.AerospikeError, NULL);
	Py_INCREF(exceptions_array.TimeoutError);
	PyModule_AddObject(module, "TimeoutError", exceptions_array.TimeoutError);
	py_code = PyInt_FromLong(AEROSPIKE_ERR_TIMEOUT);
	PyObject_SetAttrString(exceptions_array.TimeoutError, "code", py_code);
	Py_DECREF(py_code);

	//Client Exceptions
	exceptions_array.ParamError = PyErr_NewException("exception.ParamError", exceptions_array.ClientError, NULL);
	Py_INCREF(exceptions_array.ParamError);
	PyModule_AddObject(module, "ParamError", exceptions_array.ParamError);
	py_code = PyInt_FromLong(AEROSPIKE_ERR_PARAM);
	PyObject_SetAttrString(exceptions_array.ParamError, "code", py_code);
	Py_DECREF(py_code);

	exceptions_array.InvalidHostError = PyErr_NewException("exception.InvalidHostError", exceptions_array.ClientError, NULL);
	Py_INCREF(exceptions_array.InvalidHostError);
	PyModule_AddObject(module, "InvalidHostError", exceptions_array.InvalidHostError);
	py_code = PyInt_FromLong(AEROSPIKE_ERR_INVALID_HOST);
	PyObject_SetAttrString(exceptions_array.InvalidHostError, "code", py_code);
	Py_DECREF(py_code);

	exceptions_array.ConnectionError= PyErr_NewException("exception.ConnectionError", exceptions_array.ClientError, NULL);
	Py_INCREF(exceptions_array.ConnectionError);
	PyModule_AddObject(module, "ConnectionError", exceptions_array.ConnectionError);
	py_code = PyInt_FromLong(AEROSPIKE_ERR_CONNECTION);
	PyObject_SetAttrString(exceptions_array.ConnectionError, "code", py_code);
	Py_DECREF(py_code);

	// TLSError, AEROSPIKE_ERR_TLS_ERROR, -9
	exceptions_array.TLSError = PyErr_NewException("exception.TLSError", exceptions_array.ClientError, NULL);
	Py_INCREF(exceptions_array.TLSError);
	PyModule_AddObject(module, "TLSError", exceptions_array.TLSError);
	py_code = PyInt_FromLong(AEROSPIKE_ERR_TLS_ERROR);
	PyObject_SetAttrString(exceptions_array.TLSError, "code", py_code);
	Py_DECREF(py_code);

	// InvalidNodeError, AEROSPIKE_ERR_INVALID_NODE, -8
	exceptions_array.InvalidNodeError = PyErr_NewException("exception.InvalidNodeError", exceptions_array.ClientError, NULL);
	Py_INCREF(exceptions_array.InvalidNodeError);
	PyModule_AddObject(module, "InvalidNodeError", exceptions_array.InvalidNodeError);
	py_code = PyInt_FromLong(AEROSPIKE_ERR_INVALID_NODE);
	PyObject_SetAttrString(exceptions_array.InvalidNodeError, "code", py_code);
	Py_DECREF(py_code);

	// NoMoreConnectionsError, AEROSPIKE_ERR_NO_MORE_CONNECTIONS, -7
	exceptions_array.NoMoreConnectionsError = PyErr_NewException("exception.NoMoreConnectionsError", exceptions_array.ClientError, NULL);
	Py_INCREF(exceptions_array.NoMoreConnectionsError);
	PyModule_AddObject(module, "NoMoreConnectionsError", exceptions_array.NoMoreConnectionsError);
	py_code = PyInt_FromLong(AEROSPIKE_ERR_NO_MORE_CONNECTIONS);
	PyObject_SetAttrString(exceptions_array.NoMoreConnectionsError, "code", py_code);
	Py_DECREF(py_code);

	// AsyncConnectionError, AEROSPIKE_ERR_ASYNC_CONNECTION, -6
	exceptions_array.AsyncConnectionError = PyErr_NewException("exception.AsyncConnectionError", exceptions_array.ClientError, NULL);
	Py_INCREF(exceptions_array.AsyncConnectionError);
	PyModule_AddObject(module, "AsyncConnectionError", exceptions_array.AsyncConnectionError);
	py_code = PyInt_FromLong(AEROSPIKE_ERR_ASYNC_CONNECTION);
	PyObject_SetAttrString(exceptions_array.AsyncConnectionError, "code", py_code);
	Py_DECREF(py_code);

	// ClientAbortError, AEROSPIKE_ERR_CLIENT_ABORT, -5
	exceptions_array.ClientAbortError = PyErr_NewException("exception.ClientAbortError", exceptions_array.ClientError, NULL);
	Py_INCREF(exceptions_array.ClientAbortError);
	PyModule_AddObject(module, "ClientAbortError", exceptions_array.ClientAbortError);
	py_code = PyInt_FromLong(AEROSPIKE_ERR_CLIENT_ABORT);
	PyObject_SetAttrString(exceptions_array.ClientAbortError, "code", py_code);
	Py_DECREF(py_code);

	//Server Exceptions
	int count = sizeof(server_array.server_exceptions)/sizeof(server_array.server_exceptions[0]);
	int i;
	PyObject **current_exception;
	for (i=0; i < count; i++) {
		current_exception = server_array.server_exceptions[i];
		char * name = server_array.server_exceptions_name[i];
		char prefix[40] = "exception.";
		*current_exception = PyErr_NewException(strcat(prefix, name), exceptions_array.ServerError, NULL);
		Py_INCREF(*current_exception);
		PyModule_AddObject(module, name, *current_exception);
		PyObject *py_code = PyInt_FromLong(server_array.server_exceptions_codes[i]);
		PyObject_SetAttrString(*current_exception, "code", py_code);
		Py_DECREF(py_code);
	}

	exceptions_array.ClusterChangeError = PyErr_NewException("exception.ClusterChangeError", exceptions_array.ClusterError, NULL);
	Py_INCREF(exceptions_array.ClusterChangeError);
	PyModule_AddObject(module, "ClusterChangeError", exceptions_array.ClusterChangeError);
	py_code = PyInt_FromLong(AEROSPIKE_ERR_CLUSTER_CHANGE);
	PyObject_SetAttrString(exceptions_array.ClusterChangeError, "code", py_code);
	Py_DECREF(py_code);

	//Extra Server Errors
	// ScanAbortedError , AEROSPIKE_ERR_SCAN_ABORTED, 15
	exceptions_array.ScanAbortedError = PyErr_NewException("exception.ScanAbortedError", exceptions_array.ServerError, NULL);
	Py_INCREF(exceptions_array.ScanAbortedError);
	PyModule_AddObject(module, "ScanAbortedError", exceptions_array.ScanAbortedError);
	py_code = PyInt_FromLong(AEROSPIKE_ERR_SCAN_ABORTED);
	PyObject_SetAttrString(exceptions_array.ScanAbortedError, "code", py_code);
	Py_DECREF(py_code);

	// ElementNotFoundError , AEROSPIKE_ERR_FAIL_ELEMENT_NOT_FOUND, 23
	exceptions_array.ElementNotFoundError = PyErr_NewException("exception.ElementNotFoundError", exceptions_array.ServerError, NULL);
	Py_INCREF(exceptions_array.ElementNotFoundError);
	PyModule_AddObject(module, "ElementNotFoundError", exceptions_array.ElementNotFoundError);
	py_code = PyInt_FromLong(AEROSPIKE_ERR_FAIL_ELEMENT_NOT_FOUND);
	PyObject_SetAttrString(exceptions_array.ElementNotFoundError, "code", py_code);
	Py_DECREF(py_code);

	// ElementExistsError , AEROSPIKE_ERR_FAIL_ELEMENT_EXISTS, 24
	exceptions_array.ElementExistsError = PyErr_NewException("exception.ElementExistsError", exceptions_array.ServerError, NULL);
	Py_INCREF(exceptions_array.ElementExistsError);
	PyModule_AddObject(module, "ElementExistsError", exceptions_array.ElementExistsError);
	py_code = PyInt_FromLong(AEROSPIKE_ERR_FAIL_ELEMENT_EXISTS);
	PyObject_SetAttrString(exceptions_array.ElementExistsError, "code", py_code);
	Py_DECREF(py_code);

	// BatchDisabledError , AEROSPIKE_ERR_BATCH_DISABLED, 150
	exceptions_array.BatchDisabledError = PyErr_NewException("exception.BatchDisabledError", exceptions_array.ServerError, NULL);
	Py_INCREF(exceptions_array.BatchDisabledError);
	PyModule_AddObject(module, "BatchDisabledError", exceptions_array.BatchDisabledError);
	py_code = PyInt_FromLong(AEROSPIKE_ERR_BATCH_DISABLED);
	PyObject_SetAttrString(exceptions_array.BatchDisabledError, "code", py_code);
	Py_DECREF(py_code);

	// BatchMaxRequestError , AEROSPIKE_ERR_BATCH_MAX_REQUESTS_EXCEEDED, 151
	exceptions_array.BatchMaxRequestError = PyErr_NewException("exception.BatchMaxRequestError", exceptions_array.ServerError, NULL);
	Py_INCREF(exceptions_array.BatchMaxRequestError);
	PyModule_AddObject(module, "BatchMaxRequestError", exceptions_array.BatchMaxRequestError);
	py_code = PyInt_FromLong(AEROSPIKE_ERR_BATCH_MAX_REQUESTS_EXCEEDED);
	PyObject_SetAttrString(exceptions_array.BatchMaxRequestError, "code", py_code);
	Py_DECREF(py_code);

	// BatchQueueFullError , AEROSPIKE_ERR_BATCH_QUEUES_FULL, 152
	exceptions_array.BatchQueueFullError = PyErr_NewException("exception.BatchQueueFullError", exceptions_array.ServerError, NULL);
	Py_INCREF(exceptions_array.BatchQueueFullError);
	PyModule_AddObject(module, "BatchQueueFullError", exceptions_array.BatchQueueFullError);
	py_code = PyInt_FromLong(AEROSPIKE_ERR_BATCH_QUEUES_FULL);
	PyObject_SetAttrString(exceptions_array.BatchQueueFullError, "code", py_code);
	Py_DECREF(py_code);

	// QueryAbortedError , AEROSPIKE_ERR_QUERY_ABORTED, 210
	exceptions_array.QueryAbortedError = PyErr_NewException("exception.QueryAbortedError", exceptions_array.ServerError, NULL);
	Py_INCREF(exceptions_array.QueryAbortedError);
	PyModule_AddObject(module, "QueryAbortedError", exceptions_array.QueryAbortedError);
	py_code = PyInt_FromLong(AEROSPIKE_ERR_QUERY_ABORTED);
	PyObject_SetAttrString(exceptions_array.QueryAbortedError, "code", py_code);
	Py_DECREF(py_code);

	//Record exceptions
	PyObject *py_record_dict = PyDict_New();
	PyDict_SetItemString(py_record_dict, "key", Py_None);
	PyDict_SetItemString(py_record_dict, "bin", Py_None);

	exceptions_array.RecordError = PyErr_NewException("exception.RecordError", exceptions_array.ServerError, py_record_dict);
	Py_INCREF(exceptions_array.RecordError);
	Py_DECREF(py_record_dict);
	PyObject_SetAttrString(exceptions_array.RecordError, "code", Py_None);
	PyModule_AddObject(module, "RecordError", exceptions_array.RecordError);

	//int count = sizeof(record_exceptions)/sizeof(record_exceptions[0]);
	count = sizeof(record_array.record_exceptions)/sizeof(record_array.record_exceptions[0]);
	for (i=0; i < count; i++) {
		current_exception = record_array.record_exceptions[i];
		char * name = record_array.record_exceptions_name[i];
		char prefix[40] = "exception.";
		*current_exception = PyErr_NewException(strcat(prefix, name), exceptions_array.RecordError, NULL);
		Py_INCREF(*current_exception);
		PyModule_AddObject(module, name, *current_exception);
		PyObject *py_code = PyInt_FromLong(record_array.record_exceptions_codes[i]);
		PyObject_SetAttrString(*current_exception, "code", py_code);
		Py_DECREF(py_code);
	}

	//Index exceptions
	PyObject *py_index_dict = PyDict_New();
	PyDict_SetItemString(py_index_dict, "name", Py_None);

	exceptions_array.IndexError = PyErr_NewException("exception.IndexError", exceptions_array.ServerError, py_index_dict);
	Py_INCREF(exceptions_array.IndexError);
	Py_DECREF(py_index_dict);
	py_code = PyInt_FromLong(AEROSPIKE_ERR_INDEX);
	PyObject_SetAttrString(exceptions_array.IndexError, "code", py_code);
	Py_DECREF(py_code);
	PyModule_AddObject(module, "IndexError", exceptions_array.IndexError);

	count = sizeof(index_array.index_exceptions)/sizeof(index_array.index_exceptions[0]);
	for (i=0; i < count; i++) {
		current_exception = index_array.index_exceptions[i];
		char * name = index_array.index_exceptions_name[i];
		char prefix[40] = "exception.";
		*current_exception = PyErr_NewException(strcat(prefix, name), exceptions_array.IndexError, NULL);
		Py_INCREF(*current_exception);
		PyModule_AddObject(module, name, *current_exception);
		PyObject *py_code = PyInt_FromLong(index_array.index_exceptions_codes[i]);
		PyObject_SetAttrString(*current_exception, "code", py_code);
		Py_DECREF(py_code);
	}

	//UDF exceptions
	PyObject *py_udf_dict = PyDict_New();
	PyDict_SetItemString(py_udf_dict, "module", Py_None);	
	PyDict_SetItemString(py_udf_dict, "func", Py_None);	

	exceptions_array.UDFError = PyErr_NewException("exception.UDFError", exceptions_array.ServerError, py_udf_dict);
	Py_INCREF(exceptions_array.UDFError);
	Py_DECREF(py_udf_dict);
	PyModule_AddObject(module, "UDFError", exceptions_array.UDFError);
	py_code = PyInt_FromLong(AEROSPIKE_ERR_UDF);
	PyObject_SetAttrString(exceptions_array.UDFError, "code", py_code);
	Py_DECREF(py_code);

	exceptions_array.UDFNotFound = PyErr_NewException("exception.UDFNotFound", exceptions_array.UDFError, NULL);
	Py_INCREF(exceptions_array.UDFNotFound);
	PyModule_AddObject(module, "UDFNotFound", exceptions_array.UDFNotFound);
	py_code = PyInt_FromLong(AEROSPIKE_ERR_UDF_NOT_FOUND);
	PyObject_SetAttrString(exceptions_array.UDFNotFound, "code", py_code);
	Py_DECREF(py_code);

	exceptions_array.LuaFileNotFound = PyErr_NewException("exception.LuaFileNotFound", exceptions_array.UDFError, NULL);
	Py_INCREF(exceptions_array.LuaFileNotFound);
	PyModule_AddObject(module, "LuaFileNotFound", exceptions_array.LuaFileNotFound);
	py_code = PyInt_FromLong(AEROSPIKE_ERR_LUA_FILE_NOT_FOUND);
	PyObject_SetAttrString(exceptions_array.LuaFileNotFound, "code", py_code);
	Py_DECREF(py_code);

	//Admin exceptions
	exceptions_array.AdminError = PyErr_NewException("exception.AdminError", exceptions_array.ServerError, NULL);
	Py_INCREF(exceptions_array.AdminError);
	PyObject_SetAttrString(exceptions_array.AdminError, "code", Py_None);
	PyModule_AddObject(module, "AdminError", exceptions_array.AdminError);

	count = sizeof(admin_array.admin_exceptions)/sizeof(admin_array.admin_exceptions[0]);
	for (i=0; i < count; i++) {
		current_exception = admin_array.admin_exceptions[i];
		char * name = admin_array.admin_exceptions_name[i];
		char prefix[40] = "exception.";
		*current_exception = PyErr_NewException(strcat(prefix, name), exceptions_array.AdminError, NULL);
		Py_INCREF(*current_exception);
		PyModule_AddObject(module, name, *current_exception);
		PyObject *py_code = PyInt_FromLong(admin_array.admin_exceptions_codes[i]);
		PyObject_SetAttrString(*current_exception, "code", py_code);
		Py_DECREF(py_code);
	}

	//Query exceptions
	exceptions_array.QueryQueueFull = PyErr_NewException("exception.QueryQueueFull", exceptions_array.QueryError, NULL);
	Py_INCREF(exceptions_array.QueryQueueFull);
	PyModule_AddObject(module, "QueryQueueFull", exceptions_array.QueryQueueFull);
	py_code = PyInt_FromLong(AEROSPIKE_ERR_QUERY_QUEUE_FULL);
	PyObject_SetAttrString(exceptions_array.QueryQueueFull, "code", py_code);
	Py_DECREF(py_code);

	exceptions_array.QueryTimeout = PyErr_NewException("exception.QueryTimeout", exceptions_array.QueryError, NULL);
	Py_INCREF(exceptions_array.QueryTimeout);
	PyModule_AddObject(module, "QueryTimeout", exceptions_array.QueryTimeout);
	py_code = PyInt_FromLong(AEROSPIKE_ERR_QUERY_TIMEOUT);
	PyObject_SetAttrString(exceptions_array.QueryQueueFull, "code", py_code);
	Py_DECREF(py_code);

	//LDT exceptions
	PyObject *py_ldt_dict = PyDict_New();
	PyDict_SetItemString(py_ldt_dict, "key", Py_None);
	PyDict_SetItemString(py_ldt_dict, "bin", Py_None);
	exceptions_array.LDTError = PyErr_NewException("exception.LDTError", exceptions_array.ServerError, py_ldt_dict);
	PyObject_SetAttrString(exceptions_array.LDTError, "code", Py_None);
	Py_INCREF(exceptions_array.LDTError);
	Py_DECREF(py_ldt_dict);
	PyModule_AddObject(module, "LDTError", exceptions_array.LDTError);

	count = sizeof(ldt_array.ldt_exceptions)/sizeof(ldt_array.ldt_exceptions[0]);
	for (i=0; i < count; i++) {
		current_exception = ldt_array.ldt_exceptions[i];
		char * name = ldt_array.ldt_exceptions_name[i];
		char prefix[40] = "exception.";
		*current_exception = PyErr_NewException(strcat(prefix, name), exceptions_array.LDTError, NULL);
		Py_INCREF(*current_exception);
		PyModule_AddObject(module, name, *current_exception);
		PyObject *py_code = PyInt_FromLong(ldt_array.ldt_exceptions_codes[i]);
		PyObject_SetAttrString(*current_exception, "code", py_code);
		Py_DECREF(py_code);
	}
	return module;
}

PyObject* raise_exception(as_error *err) {
	PyObject * py_key = NULL, *py_value = NULL;
	Py_ssize_t pos = 0;
	PyObject * py_module_dict = PyModule_GetDict(module);
	char * err_msg= err->message, *err_code = err->message;
	char *final_code = NULL;
	if (err->code == AEROSPIKE_ERR_UDF) {
		while (strstr(err_code, ": ")) {
			err_code++;
		}
		while (strstr(err_msg, ":LDT")) {
			err_msg++;
		}
		final_code = (char *)malloc(err_msg - err_code + 2);
		if (err_code != err->message && err_msg != err->message) {
			strncpy(final_code, err_code + 1, err_msg - err_code + 2);
			err->code = atoi(final_code);
			strcpy(err->message, err_msg);
		}
		free(final_code);
	}
	while (PyDict_Next(py_module_dict, &pos, &py_key, &py_value)) {
		if (PyObject_HasAttrString(py_value, "code")) {
			PyObject * py_code = PyObject_GetAttrString(py_value, "code");
			if (py_code == Py_None) {
				continue;
			}
			if (err->code == PyInt_AsLong(py_code)) {
				PyObject *py_attr = NULL;
				py_attr = PyString_FromString(err->message);
				PyObject_SetAttrString(py_value, "msg", py_attr);
				Py_DECREF(py_attr);
				py_attr = PyString_FromString(err->file);
				PyObject_SetAttrString(py_value, "file", py_attr);
				Py_DECREF(py_attr);
				py_attr = PyInt_FromLong(err->line);
				PyObject_SetAttrString(py_value, "line", py_attr);
				Py_DECREF(py_attr);
				break;
			}
			Py_DECREF(py_code);
		}
	}

	return py_value;
}
