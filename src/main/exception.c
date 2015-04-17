/*******************************************************************************
 * Copyright 2013-2014 Aerospike, Inc.
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

/**
 * from aerospike import predicates as p
 *
 * q = client.query(ns,set).where(p.equals("bin",1))
 */
#include <Python.h>
#include <aerospike/as_query.h>
#include <aerospike/as_error.h>
#include <aerospike/as_status.h>

#include "conversions.h"
#include <string.h>
#include "exceptions.h"
PyObject *ParamError;
PyObject *ClientError, *ServerError, *TimeoutError;
PyObject *InvalidRequest, *ServerFull, *NoXDR, *UnsupportedFeature, *DeviceOverload, *ForbiddenError, *QueryError;
PyObject *AdminError, *SecurityNotSupported, *SecurityNotEnabled, *SecuritySchemeNotSupported, *InvalidCommand, *InvalidField, *IllegalState, *InvalidUser, *UserExistsError, *InvalidPassword, *ExpiredPassword, *ForbiddenPassword, *InvalidCredential, *InvalidRole, *RoleExistsError, *RoleViolation, *InvalidPrivilege, *NotAuthenticated;
static PyObject *AerospikeError, *InvalidHostError, *NamespaceNotFound;
static PyObject *RecordError, *RecordKeyMismatch=NULL, *RecordNotFound=NULL, *BinNameError=NULL, *RecordGenerationError=NULL, *RecordExistsError, *RecordTooBig, *RecordBusy, *BinNameError, *BinExistsError, *BinNotFound, *BinIncompatibleType;
static PyObject *IndexError, *IndexNotFound, *IndexFoundError, *IndexOOM, *IndexNotReadable, *IndexNameMaxLen, *IndexNameMaxCount;
static PyObject *UDFError, *UDFNotFound, *LuaFileNotFound;
static PyObject *ClusterError, *ClusterChangeError;
static PyObject *module;

struct server_exceptions_struct {
	PyObject * *server_exceptions[9];
	char * server_exceptions_name[9];
	int server_exceptions_codes[9];
};
struct record_exceptions_struct {
	PyObject * *record_exceptions[10];
	char * record_exceptions_name[10];
	int record_exceptions_codes[10];
};

struct index_exceptions_struct {
	PyObject * *index_exceptions[5];
	char * index_exceptions_name[5];
	int index_exceptions_codes[5];
};

struct admin_exceptions_struct {
	PyObject * *admin_exceptions[17];
	char * admin_exceptions_name[17];
	int admin_exceptions_codes[17];
};

PyObject * AerospikeException_New(void)
{
	module = Py_InitModule3("aerospike.exception", NULL, "Exception objects");

	struct server_exceptions_struct server_array = { 
		{&InvalidRequest, &ServerFull, &NoXDR, &UnsupportedFeature, &DeviceOverload, &NamespaceNotFound, &ForbiddenError, &QueryError, &ClusterError},
		{"InvalidRequest", "ServerFull", "NoXDR", "UnsupportedFeature", "DeviceOverload", "NamespaceNotFound", "ForbiddenError", "QueryError", "ClusterError"},
		{AEROSPIKE_ERR_REQUEST_INVALID, AEROSPIKE_ERR_SERVER_FULL, AEROSPIKE_ERR_NO_XDR, AEROSPIKE_ERR_UNSUPPORTED_FEATURE, AEROSPIKE_ERR_DEVICE_OVERLOAD, 
			AEROSPIKE_ERR_NAMESPACE_NOT_FOUND,  AEROSPIKE_ERR_FAIL_FORBIDDEN, AEROSPIKE_ERR_QUERY, AEROSPIKE_ERR_CLUSTER}
	};

	struct record_exceptions_struct record_array = { 
		{&RecordKeyMismatch, &RecordNotFound, &RecordGenerationError, &RecordExistsError, &RecordTooBig, &RecordBusy, &BinNameError, &BinExistsError, &BinNotFound, 
			&BinIncompatibleType},
		{"RecordKeyMismatch", "RecordNotFound", "RecordGenerationError", "RecordExistsError", "RecordTooBig", "RecordBusy", "BinNameError", "BinExistsError", 
			"BinNotFound", "BinIncompatibleType"},
		{AEROSPIKE_ERR_RECORD_KEY_MISMATCH, AEROSPIKE_ERR_RECORD_NOT_FOUND, AEROSPIKE_ERR_RECORD_GENERATION, AEROSPIKE_ERR_RECORD_EXISTS, AEROSPIKE_ERR_RECORD_TOO_BIG, AEROSPIKE_ERR_RECORD_BUSY, AEROSPIKE_ERR_BIN_NAME, AEROSPIKE_ERR_BIN_EXISTS, AEROSPIKE_ERR_BIN_NOT_FOUND, AEROSPIKE_ERR_BIN_INCOMPATIBLE_TYPE}
	};

	struct index_exceptions_struct index_array = { 
		{&IndexNotFound, &IndexOOM, &IndexNotReadable, &IndexNameMaxLen, &IndexNameMaxCount},
		{"IndexNotFound", "IndexOOM", "IndexNotReadable", "IndexNameMaxLen", "IndexNameMaxCount"},
		{AEROSPIKE_ERR_INDEX_NOT_FOUND, AEROSPIKE_ERR_INDEX_FOUND, AEROSPIKE_ERR_INDEX_OOM, AEROSPIKE_ERR_INDEX_NOT_READABLE, AEROSPIKE_ERR_INDEX_NAME_MAXLEN, 
			AEROSPIKE_ERR_INDEX_MAXCOUNT}
	};

	struct admin_exceptions_struct admin_array = { 
		{&SecurityNotSupported, &SecurityNotEnabled, &SecuritySchemeNotSupported, &InvalidCommand, &InvalidField, &IllegalState, &InvalidUser, &UserExistsError, 
			&InvalidPassword, &ExpiredPassword, &ForbiddenPassword, &InvalidCredential, &InvalidRole, &RoleExistsError, &RoleViolation, &InvalidPrivilege, 
			&NotAuthenticated},
		{"SecurityNotSupported", "SecurityNotEnabled", "SecuritySchemeNotSupported", "InvalidCommand", "InvalidField", "IllegalState", "InvalidUser", "UserExistsError", 
			"InvalidPassword", "ExpiredPassword", "ForbiddenPassword", "InvalidCredential", "InvalidRole", "RoleExistsError", "RoleViolation", "InvalidPrivilege", 
			"NotAuthenticated"},
		{AEROSPIKE_SECURITY_NOT_SUPPORTED, AEROSPIKE_SECURITY_NOT_ENABLED, AEROSPIKE_SECURITY_SCHEME_NOT_SUPPORTED, AEROSPIKE_INVALID_COMMAND, AEROSPIKE_INVALID_FIELD, 
			AEROSPIKE_ILLEGAL_STATE, AEROSPIKE_INVALID_USER, AEROSPIKE_USER_ALREADY_EXISTS, AEROSPIKE_INVALID_PASSWORD, AEROSPIKE_EXPIRED_PASSWORD, 
			AEROSPIKE_FORBIDDEN_PASSWORD, AEROSPIKE_INVALID_CREDENTIAL, AEROSPIKE_INVALID_ROLE, AEROSPIKE_ROLE_ALREADY_EXISTS, AEROSPIKE_ROLE_VIOLATION,  
			AEROSPIKE_INVALID_PRIVILEGE, AEROSPIKE_NOT_AUTHENTICATED}
	};

	PyObject *py_dict = PyDict_New();
	PyDict_SetItemString(py_dict, "code", Py_None);
	PyDict_SetItemString(py_dict, "file", Py_None);
	PyDict_SetItemString(py_dict, "msg", Py_None);
	PyDict_SetItemString(py_dict, "line", Py_None);

	AerospikeError = PyErr_NewException("exception.AerospikeError", NULL, py_dict);
	Py_INCREF(AerospikeError);
	Py_DECREF(py_dict);
	PyModule_AddObject(module, "AerospikeError", AerospikeError);

	ClientError = PyErr_NewException("exception.ClientError", AerospikeError, NULL);
	Py_INCREF(ClientError);
	PyModule_AddObject(module, "ClientError", ClientError);
	PyObject_SetAttrString(ClientError, "code", PyInt_FromLong(AEROSPIKE_ERR_CLIENT));

	ServerError = PyErr_NewException("exception.ServerError", AerospikeError, NULL);
	Py_INCREF(ServerError);
	PyModule_AddObject(module, "ServerError", ServerError);
	PyObject_SetAttrString(ServerError, "code", PyInt_FromLong(AEROSPIKE_ERR_SERVER));

	TimeoutError = PyErr_NewException("exception.TimeoutError", AerospikeError, NULL);
	Py_INCREF(TimeoutError);
	PyModule_AddObject(module, "TimeoutError", TimeoutError);
	PyObject_SetAttrString(TimeoutError, "code", PyInt_FromLong(AEROSPIKE_ERR_TIMEOUT));

	//Client Exceptions
	ParamError = PyErr_NewException("exception.ParamError", ClientError, NULL);
	Py_INCREF(ParamError);
	PyModule_AddObject(module, "ParamError", ParamError);
	PyObject_SetAttrString(ParamError, "code", PyInt_FromLong(AEROSPIKE_ERR_PARAM));

	InvalidHostError = PyErr_NewException("exception.InvalidHostError", ClientError, NULL);
	Py_INCREF(InvalidHostError);
	PyModule_AddObject(module, "InvalidHostError", InvalidHostError);
	PyObject_SetAttrString(InvalidHostError, "code", PyInt_FromLong(AEROSPIKE_ERR_INVALID_HOST));

	//Server Exceptions
	int count = sizeof(server_array.server_exceptions)/sizeof(server_array.server_exceptions[0]);
	int i;
	PyObject **current_exception;
	for(i=0; i < count; i++) {
		current_exception = server_array.server_exceptions[i];
		char * name = server_array.server_exceptions_name[i];
		char prefix[40] = "exception.";
		*current_exception = PyErr_NewException(strcat(prefix, name), ServerError, NULL);
		Py_INCREF(*current_exception);
		PyModule_AddObject(module, name, *current_exception);
		PyObject_SetAttrString(*current_exception, "code", PyInt_FromLong(server_array.server_exceptions_codes[i]));
	}

	ClusterChangeError = PyErr_NewException("exception.ClusterChangeError", ClusterError, NULL);
	Py_INCREF(ClusterChangeError);
	PyModule_AddObject(module, "ClusterChangeError", ClusterChangeError);

	//Record exceptions
	PyObject *py_record_dict = PyDict_New();
	PyDict_SetItemString(py_record_dict, "key", Py_None);
	PyDict_SetItemString(py_record_dict, "bin", Py_None);

	RecordError = PyErr_NewException("exception.RecordError", ServerError, py_record_dict);
	Py_INCREF(RecordError);
	Py_DECREF(py_record_dict);
	PyModule_AddObject(module, "RecordError", RecordError);
	
	//int count = sizeof(record_exceptions)/sizeof(record_exceptions[0]);
	count = sizeof(record_array.record_exceptions)/sizeof(record_array.record_exceptions[0]);
	for(i=0; i < count; i++) {
		current_exception = record_array.record_exceptions[i];
		char * name = record_array.record_exceptions_name[i];
		char prefix[40] = "exception.";
		*current_exception = PyErr_NewException(strcat(prefix, name), RecordError, NULL);
		Py_INCREF(*current_exception);
		PyModule_AddObject(module, name, *current_exception);
		PyObject_SetAttrString(*current_exception, "code", PyInt_FromLong(record_array.record_exceptions_codes[i]));
	}

	//Index exceptions
	PyObject *py_index_dict = PyDict_New();
	PyDict_SetItemString(py_index_dict, "name", Py_None);

	IndexError = PyErr_NewException("exception.IndexError", ServerError, py_index_dict);
	Py_INCREF(IndexError);
	Py_DECREF(py_index_dict);
	PyModule_AddObject(module, "IndexError", IndexError);

	count = sizeof(index_array.index_exceptions)/sizeof(index_array.index_exceptions[0]);
	for(i=0; i < count; i++) {
		current_exception = index_array.index_exceptions[i];
		char * name = index_array.index_exceptions_name[i];
		char prefix[40] = "exception.";
		*current_exception = PyErr_NewException(strcat(prefix, name), IndexError, NULL);
		Py_INCREF(*current_exception);
		PyModule_AddObject(module, name, *current_exception);
		PyObject_SetAttrString(*current_exception, "code", PyInt_FromLong(index_array.index_exceptions_codes[i]));
	}

	//UDF exceptions
	PyObject *py_udf_dict = PyDict_New();
	PyDict_SetItemString(py_udf_dict, "module", Py_None);	
	PyDict_SetItemString(py_udf_dict, "func", Py_None);	

	UDFError = PyErr_NewException("exception.UDFError", ServerError, py_udf_dict);
	Py_INCREF(UDFError);
	Py_DECREF(py_udf_dict);
	PyModule_AddObject(module, "UDFError", UDFError);
	PyObject_SetAttrString(UDFError, "code", PyInt_FromLong(AEROSPIKE_ERR_UDF));

	UDFNotFound = PyErr_NewException("exception.UDFNotFound", UDFError, NULL);
	Py_INCREF(UDFNotFound);
	PyModule_AddObject(module, "UDFNotFound", UDFNotFound);
	PyObject_SetAttrString(UDFNotFound, "code", PyInt_FromLong(AEROSPIKE_ERR_UDF_NOT_FOUND));

	LuaFileNotFound = PyErr_NewException("exception.LuaFileNotFound", UDFError, NULL);
	Py_INCREF(LuaFileNotFound);
	PyModule_AddObject(module, "LuaFileNotFound", LuaFileNotFound);
	PyObject_SetAttrString(LuaFileNotFound, "code", PyInt_FromLong(AEROSPIKE_ERR_LUA_FILE_NOT_FOUND));

	//Admin exceptions
	AdminError = PyErr_NewException("exception.AdminError", ServerError, NULL);
	Py_INCREF(AdminError);
	PyModule_AddObject(module, "AdminError", AdminError);

	count = sizeof(admin_array.admin_exceptions)/sizeof(admin_array.admin_exceptions[0]);
	for(i=0; i < count; i++) {
		current_exception = admin_array.admin_exceptions[i];
		char * name = admin_array.admin_exceptions_name[i];
		char prefix[40] = "exception.";
		*current_exception = PyErr_NewException(strcat(prefix, name), AdminError, NULL);
		Py_INCREF(*current_exception);
		PyModule_AddObject(module, name, *current_exception);
		PyObject_SetAttrString(*current_exception, "code", PyInt_FromLong(admin_array.admin_exceptions_codes[i]));
	}
	return module;
}

PyObject* raise_exception(as_error *err) {
		PyObject * py_key = NULL, *py_value = NULL;
		Py_ssize_t pos = 0;
		PyObject * py_module_dict = PyModule_GetDict(module);
		while(PyDict_Next(py_module_dict, &pos, &py_key, &py_value)) {
			if(PyObject_HasAttrString(py_value, "file")) {
				PyObject * py_code = PyObject_GetAttrString(py_value, "code");
				if(err->code == PyInt_AsLong(py_code)) {
					PyObject_SetAttrString(py_value, "msg", PyString_FromString(err->message));
					PyObject_SetAttrString(py_value, "file", PyString_FromString(err->file));
					PyObject_SetAttrString(py_value, "line", PyInt_FromLong(err->line));
					break;
				}
			}
		}
		PyObject * type_of_exception = NULL;
		/*switch(err->code) {
			case -1:
				type_of_exception = ClientError;
			break;

			case -2:
				type_of_exception = ParamError;
			break;

			case 20:
				type_of_exception = NamespaceNotFound;
			break;

			//Catch Record Errors
			case 19:
				type_of_exception = RecordKeyMismatch;
			break;

			case 2:
				type_of_exception = RecordNotFound;
			break;

			case 3:
				type_of_exception = RecordGenerationError;
			break;

			case 5:
				type_of_exception = RecordExistsError;
			break;

			case 13:
				type_of_exception = RecordTooBig;
			break;

			case 14:
				type_of_exception = RecordBusy;
			break;

			case 21:
				type_of_exception = BinNameError;
			break;

			case 6:
				type_of_exception = BinExistsError;
			break;

			case 17:
				type_of_exception = BinNotFound;
			break;

			case 12:
				type_of_exception = BinIncompatibleType;
			break;

			//Catch cluster errors
			case 11:
				type_of_exception = ClusterError;
			break;

			case 7:
				type_of_exception = ClusterChangeError;
			break;

			//Catch index exceptions
			case 201:
				type_of_exception = IndexNotFound;
			break;

			case 200:
				type_of_exception = IndexFoundError;
			break;

			case 202:
				type_of_exception = IndexOOM;
			break;

			case 203:
				type_of_exception = IndexNotReadable;
			break;

			case 205:
				type_of_exception = IndexNameMaxLen;
			break;

			case 206:
				type_of_exception = IndexNameMaxCount;
			break;

			//Catch udf exceptions
			case 100:
				type_of_exception = UDFError;
			break;

			case 1301:
				type_of_exception = UDFNotFound;
			break;

			case 1302:
				type_of_exception = LuaFileNotFound;
			break;
		}*/

		//PyObject_SetAttrString(type_of_exception, "code", PyInt_FromLong(err->code));

		return py_value;
}
