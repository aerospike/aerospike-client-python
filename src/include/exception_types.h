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

#pragma once

#include <Python.h>

struct exceptions {
	PyObject *AerospikeError;

	//Aerospike exceptions
	PyObject *ClientError;
	PyObject *ServerError;
	PyObject *TimeoutError;

	//Server exceptions
	PyObject *InvalidRequest;
	PyObject *ServerFull;
	PyObject *NoXDR;
	PyObject *UnsupportedFeature;
	PyObject *DeviceOverload;
	PyObject *ForbiddenError;
	PyObject *QueryError;
	PyObject *InvalidGeoJSON;

	//Client exceptions
	PyObject *ParamError;
	PyObject *InvalidHostError;
	PyObject *NamespaceNotFound;
	PyObject *ConnectionError;

	//Record exceptions
	PyObject *RecordError;
	PyObject *RecordKeyMismatch;
	PyObject *RecordNotFound;
	PyObject *BinNameError;
	PyObject *RecordGenerationError;
	PyObject *RecordExistsError;
	PyObject *RecordTooBig;
	PyObject *RecordBusy;
	PyObject *BinExistsError;
	PyObject *BinNotFound;
	PyObject *BinIncompatibleType;

	//Index exceptions
	PyObject *IndexError;
	PyObject *IndexNotFound;
	PyObject *IndexFoundError;
	PyObject *IndexOOM;
	PyObject *IndexNotReadable;
	PyObject *IndexNameMaxLen;
	PyObject *IndexNameMaxCount;

	//Admin exceptions
	PyObject *AdminError;
	PyObject *SecurityNotSupported;
	PyObject *SecurityNotEnabled;
	PyObject *SecuritySchemeNotSupported;
	PyObject *InvalidCommand;
	PyObject *InvalidField;
	PyObject *IllegalState;
	PyObject *InvalidUser;
	PyObject *UserExistsError;
	PyObject *InvalidPassword;
	PyObject *ExpiredPassword;
	PyObject *ForbiddenPassword;
	PyObject *InvalidCredential;
	PyObject *InvalidRole;
	PyObject *RoleExistsError;
	PyObject *RoleViolation;
	PyObject *InvalidPrivilege;
	PyObject *NotAuthenticated;

	//UDF exceptions
	PyObject *UDFError;
	PyObject *UDFNotFound;
	PyObject *LuaFileNotFound;

	//Cluster exceptions
	PyObject *ClusterError;
	PyObject *ClusterChangeError;

	//Query exceptions
	PyObject *QueryQueueFull;
	PyObject *QueryTimeout;

	//LDT exceptions
	PyObject *LDTError;
	PyObject *LargeItemNotFound;
	PyObject *LDTInternalError;
	PyObject *LDTNotFound;
	PyObject *LDTUniqueKeyError;
	PyObject *LDTInsertError;
	PyObject *LDTSearchError;
	PyObject *LDTDeleteError;
	PyObject *LDTInputParamError;
	PyObject *LDTTypeMismatch;
	PyObject *LDTBinNameNull;
	PyObject *LDTBinNameNotString;
	PyObject *LDTBinNameTooLong;
	PyObject *LDTTooManyOpenSubrecs;
	PyObject *LDTTopRecNotFound;
	PyObject *LDTSubRecNotFound;
	PyObject *LDTBinNotFound;
	PyObject *LDTBinExistsError;
	PyObject *LDTBinDamaged;
	PyObject *LDTSubrecPoolDamaged;
	PyObject *LDTSubrecDamaged;
	PyObject *LDTSubrecOpenError;
	PyObject *LDTSubrecUpdateError;
	PyObject *LDTSubrecCreateError;
	PyObject *LDTSubrecDeleteError;
	PyObject *LDTSubrecCloseError;
	PyObject *LDTToprecUpdateError;
	PyObject *LDTToprecCreateError;
	PyObject *LDTFilterFunctionBad;
	PyObject *LDTFilterFunctionNotFound;
	PyObject *LDTKeyFunctionBad;
	PyObject *LDTKeyFunctionNotFound;
	PyObject *LDTTransFunctionBad;
	PyObject *LDTTransFunctionNotFound;
	PyObject *LDTUntransFunctionBad;
	PyObject *LDTUntransFunctionNotFound;
	PyObject *LDTUserModuleBad;
	PyObject *LDTUserModuleNotFound;
};

struct server_exceptions_struct {
	PyObject * *server_exceptions[10];
	char * server_exceptions_name[10];
	int server_exceptions_codes[10];
};
struct record_exceptions_struct {
	PyObject * *record_exceptions[10];
	char * record_exceptions_name[10];
	int record_exceptions_codes[10];
};

struct index_exceptions_struct {
	PyObject * *index_exceptions[6];
	char * index_exceptions_name[6];
	int index_exceptions_codes[6];
};

struct admin_exceptions_struct {
	PyObject * *admin_exceptions[17];
	char * admin_exceptions_name[17];
	int admin_exceptions_codes[17];
};

struct ldt_exceptions_struct {
	PyObject * *ldt_exceptions[37];
	char * ldt_exceptions_name[37];
	int ldt_exceptions_codes[37];
};
