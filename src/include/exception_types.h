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

#pragma once

#include <Python.h>

// These sizes correspond to the structs in exception.c.
#define RECORD_EXCEPTION_COUNT 10
#define SERVER_EXCEPTION_COUNT 13
#define INDEX_EXCEPTION_COUNT 6
#define ADMIN_EXCEPTION_COUNT 22

struct exceptions {
	PyObject *AerospikeError;

	//Aerospike exceptions
	PyObject *ClientError;
	PyObject *ServerError;
	PyObject *TimeoutError;

	//Server exceptions
	PyObject *InvalidRequest;
	PyObject *ServerFull;
	PyObject *AlwaysForbidden;
	PyObject *UnsupportedFeature;
	PyObject *DeviceOverload;
	PyObject *ForbiddenError;
	PyObject *QueryError;
	PyObject *InvalidGeoJSON;
	PyObject *OpNotApplicable;		//26
	PyObject *FilteredOut;			//27
	PyObject *LostConflict;			//28
	PyObject *ScanAbortedError;		//15
	PyObject *ElementNotFoundError; //23
	PyObject *ElementExistsError;	//24
	PyObject *BatchDisabledError;	//150
	PyObject *BatchMaxRequestError; //151
	PyObject *BatchQueueFullError;	//152
	PyObject *QueryAbortedError;	//210

	//Client exceptions
	PyObject *ParamError;
	PyObject *InvalidHostError;
	PyObject *NamespaceNotFound;
	PyObject *ConnectionError;
	PyObject *TLSError;				  //-9
	PyObject *InvalidNodeError;		  //-8
	PyObject *NoMoreConnectionsError; // -7
	PyObject *AsyncConnectionError;	  // -6
	PyObject *ClientAbortError;		  // -5

	//Record exceptions
	PyObject *RecordError;
	PyObject *RecordKeyMismatch;
	PyObject *RecordNotFound;
	PyObject *BinNameError;
	PyObject *RecordGenerationError;
	PyObject *RecordExistsError;
	PyObject *RecordTooBig;
	PyObject *RecordBusy;
	PyObject *BinIncompatibleType;
	PyObject *BinExistsError;
	PyObject *BinNotFound;

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
	PyObject *InvalidWhitelist;
	PyObject *NotWhitelisted;
	PyObject *QuotasNotEnabled;
	PyObject *InvalidQuota;
	PyObject *QuotaExceeded;

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
};

struct server_exceptions_struct {
	PyObject **server_exceptions[SERVER_EXCEPTION_COUNT];
	char *server_exceptions_name[SERVER_EXCEPTION_COUNT];
	int server_exceptions_codes[SERVER_EXCEPTION_COUNT];
};
struct record_exceptions_struct {
	PyObject **record_exceptions[RECORD_EXCEPTION_COUNT];
	char *record_exceptions_name[RECORD_EXCEPTION_COUNT];
	int record_exceptions_codes[RECORD_EXCEPTION_COUNT];
};

struct index_exceptions_struct {
	PyObject **index_exceptions[INDEX_EXCEPTION_COUNT];
	char *index_exceptions_name[INDEX_EXCEPTION_COUNT];
	int index_exceptions_codes[INDEX_EXCEPTION_COUNT];
};

struct admin_exceptions_struct {
	PyObject **admin_exceptions[ADMIN_EXCEPTION_COUNT];
	char *admin_exceptions_name[ADMIN_EXCEPTION_COUNT];
	int admin_exceptions_codes[ADMIN_EXCEPTION_COUNT];
};
