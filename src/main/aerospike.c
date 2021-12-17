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
#include <stdbool.h>
#include <stdint.h>
#include <string.h>

#include "client.h"
#include "query.h"
#include "geo.h"
#include "scan.h"
#include "key_ordered_dict.h"
#include "predicates.h"
#include "exceptions.h"
#include "policy.h"
#include "log.h"
#include <aerospike/as_operations.h>
#include "serializer.h"
#include "module_functions.h"
#include "nullobject.h"
#include "cdt_types.h"
#include <aerospike/as_log_macros.h>

PyObject *py_global_hosts;
int counter = 0xA8000000;
bool user_shm_key = false;
uint32_t async_support = 0;

PyDoc_STRVAR(client_doc, "client(config) -> client object\n\
\n\
Creates a new instance of the Client class.\n\
This client can connect() to the cluster and perform operations against it, such as put() and get() records.\n\
\n\
config = {\n\
    'hosts':    [ ('127.0.0.1', 3000) ],\n\
    'policies': {'timeout': 1000},\n\
}\n\
client = aerospike.client(config)");

PyDoc_STRVAR(init_async_doc, "init_async() -> initialize aerospike async eventloop library\n\
aerospike.init_async()");

static PyMethodDef Aerospike_Methods[] = {

	//Serialization
	{"set_serializer", (PyCFunction)AerospikeClient_Set_Serializer,
	 METH_VARARGS | METH_KEYWORDS, "Sets the serializer"},
	{"set_deserializer", (PyCFunction)AerospikeClient_Set_Deserializer,
	 METH_VARARGS | METH_KEYWORDS, "Sets the deserializer"},
	{"unset_serializers", (PyCFunction)AerospikeClient_Unset_Serializers,
	 METH_VARARGS | METH_KEYWORDS, "Unsets the serializer and deserializer"},

	{"init_async", (PyCFunction)AerospikeInitAsync, METH_VARARGS | METH_KEYWORDS,
	 init_async_doc},
	{"client", (PyCFunction)AerospikeClient_New, METH_VARARGS | METH_KEYWORDS,
	 client_doc},
	{"set_log_level", (PyCFunction)Aerospike_Set_Log_Level,
	 METH_VARARGS | METH_KEYWORDS, "Sets the log level"},
	{"set_log_handler", (PyCFunction)Aerospike_Set_Log_Handler,
	 METH_VARARGS | METH_KEYWORDS, "Enables the log handler"},
	{"geodata", (PyCFunction)Aerospike_Set_Geo_Data,
	 METH_VARARGS | METH_KEYWORDS,
	 "Creates a GeoJSON object from geospatial data."},
	{"geojson", (PyCFunction)Aerospike_Set_Geo_Json,
	 METH_VARARGS | METH_KEYWORDS,
	 "Creates a GeoJSON object from a raw GeoJSON string."},

	//Calculate the digest of a key
	{"calc_digest", (PyCFunction)Aerospike_Calc_Digest,
	 METH_VARARGS | METH_KEYWORDS, "Calculate the digest of a key"},
	
	//Get partition ID for given digest
	{"get_partition_id", (PyCFunction)Aerospike_Get_Partition_Id,
	 METH_VARARGS, "Get partition ID for given digest"},

	//Is async supported
	{"is_async_supoorted", (PyCFunction)Aerospike_Is_AsyncSupported,
	 METH_NOARGS, "check whether async supported or not"},

	{NULL}};

static AerospikeConstants operator_constants[] = {
	{AS_OPERATOR_READ, "OPERATOR_READ"},
	{AS_OPERATOR_WRITE, "OPERATOR_WRITE"},
	{AS_OPERATOR_INCR, "OPERATOR_INCR"},
	{AS_OPERATOR_APPEND, "OPERATOR_APPEND"},
	{AS_OPERATOR_PREPEND, "OPERATOR_PREPEND"},
	{AS_OPERATOR_TOUCH, "OPERATOR_TOUCH"},
	{AS_OPERATOR_DELETE, "OPERATOR_DELETE"}};

#define OPERATOR_CONSTANTS_ARR_SIZE                                            \
	(sizeof(operator_constants) / sizeof(AerospikeConstants))

static AerospikeConstants auth_mode_constants[] = {
	{AS_AUTH_INTERNAL, "AUTH_INTERNAL"},
	{AS_AUTH_EXTERNAL, "AUTH_EXTERNAL"},
	{AS_AUTH_EXTERNAL_INSECURE, "AUTH_EXTERNAL_INSECURE"},
	{AS_AUTH_PKI, "AUTH_PKI"}};

#define AUTH_MODE_CONSTANTS_ARR_SIZE                                            \
	(sizeof(auth_mode_constants) / sizeof(AerospikeConstants))

struct Aerospike_State{
	PyObject			*exception;
	PyTypeObject 		*client;
	PyTypeObject		*query;
	PyTypeObject		*scan;
	PyTypeObject		*kdict;
	PyObject			*predicates;
	PyObject			*predexps;
	PyTypeObject		*geospatial;
	PyTypeObject		*null_object;
	PyTypeObject		*wildcard_object;
	PyTypeObject		*infinite_object;
};

#define Aerospike_State(o) ((struct Aerospike_State*)PyModule_GetState(o))

static int Aerospike_Clear(PyObject *aerospike)
{

#if AS_EVENT_LIB_DEFINED
	as_event_close_loops();
#endif

	Py_CLEAR(Aerospike_State(aerospike)->exception);
	Py_CLEAR(Aerospike_State(aerospike)->client);
	Py_CLEAR(Aerospike_State(aerospike)->query);
	Py_CLEAR(Aerospike_State(aerospike)->scan);
	Py_CLEAR(Aerospike_State(aerospike)->kdict);
	Py_CLEAR(Aerospike_State(aerospike)->predicates);
	Py_CLEAR(Aerospike_State(aerospike)->predexps);
	Py_CLEAR(Aerospike_State(aerospike)->geospatial);
	Py_CLEAR(Aerospike_State(aerospike)->null_object);
	Py_CLEAR(Aerospike_State(aerospike)->wildcard_object);
	Py_CLEAR(Aerospike_State(aerospike)->infinite_object);

	return 0;
}

MOD_INIT(aerospike)
{

	const char version[8] = "6.1.2";
	// Makes things "thread-safe"
	PyEval_InitThreads();
	int i = 0;

	// aerospike Module
	PyObject *aerospike;

	MOD_DEF(aerospike, "aerospike", "Aerospike Python Client",
			sizeof(struct Aerospike_State), Aerospike_Methods, Aerospike_Clear)

	Aerospike_Enable_Default_Logging();

	py_global_hosts = PyDict_New();

	PyModule_AddStringConstant(aerospike, "__version__", version);

	PyObject *exception = AerospikeException_New();
	Py_INCREF(exception);
	PyModule_AddObject(aerospike, "exception", exception);
	Aerospike_State(aerospike)->exception = exception;

	PyTypeObject *client = AerospikeClient_Ready();
	Py_INCREF(client);
	PyModule_AddObject(aerospike, "Client", (PyObject *)client);
	Aerospike_State(aerospike)->client = client;

	PyTypeObject *query = AerospikeQuery_Ready();
	Py_INCREF(query);
	PyModule_AddObject(aerospike, "Query", (PyObject *)query);
	Aerospike_State(aerospike)->query = query;

	PyTypeObject *scan = AerospikeScan_Ready();
	Py_INCREF(scan);
	PyModule_AddObject(aerospike, "Scan", (PyObject *)scan);
	Aerospike_State(aerospike)->scan = scan;

	PyTypeObject *kdict = AerospikeKeyOrderedDict_Ready();
	Py_INCREF(kdict);
	PyModule_AddObject(aerospike, "KeyOrderedDict", (PyObject *)kdict);
	Aerospike_State(aerospike)->kdict = kdict;

	/*
	 * Add constants to module.
	 */
	for (i = 0; i < (int)OPERATOR_CONSTANTS_ARR_SIZE; i++) {
		PyModule_AddIntConstant(aerospike, operator_constants[i].constant_str,
								operator_constants[i].constantno);
	}

	for (i = 0; i < (int)AUTH_MODE_CONSTANTS_ARR_SIZE; i++) {
		PyModule_AddIntConstant(aerospike, auth_mode_constants[i].constant_str,
								auth_mode_constants[i].constantno);
	}

	declare_policy_constants(aerospike);
	RegisterPredExpConstants(aerospike);
	declare_log_constants(aerospike);

	PyObject *predicates = AerospikePredicates_New();
	Py_INCREF(predicates);
	PyModule_AddObject(aerospike, "predicates", predicates);
	Aerospike_State(aerospike)->predicates = predicates;

	PyObject *predexps = AerospikePredExp_New();
	Py_INCREF(predexps);
	PyModule_AddObject(aerospike, "predexp", predexps);
	Aerospike_State(aerospike)->predexps = predexps;

	PyTypeObject *geospatial = AerospikeGeospatial_Ready();
	Py_INCREF(geospatial);
	PyModule_AddObject(aerospike, "GeoJSON", (PyObject *)geospatial);
	Aerospike_State(aerospike)->geospatial = geospatial;

	PyTypeObject *null_object = AerospikeNullObject_Ready();
	Py_INCREF(null_object);
	PyModule_AddObject(aerospike, "null", (PyObject *)null_object);
	Aerospike_State(aerospike)->null_object = null_object;

	PyTypeObject *wildcard_object = AerospikeWildcardObject_Ready();
	Py_INCREF(wildcard_object);
	PyModule_AddObject(aerospike, "CDTWildcard", (PyObject *)wildcard_object);
	Aerospike_State(aerospike)->wildcard_object = wildcard_object;

	PyTypeObject *infinite_object = AerospikeInfiniteObject_Ready();
	Py_INCREF(infinite_object);
	PyModule_AddObject(aerospike, "CDTInfinite", (PyObject *)infinite_object);
	Aerospike_State(aerospike)->infinite_object = infinite_object;

	return MOD_SUCCESS_VAL(aerospike);
}

PyObject *AerospikeInitAsync(PyObject *self, PyObject *args, PyObject *kwds)
{
#if AS_EVENT_LIB_DEFINED
	as_log_info("AerospikeInitAsync");
	as_event_destroy_loops();
	as_event_create_loops(1);
	async_support = true;
#else
	as_error err;
	as_error_init(&err);
	as_error_update(&err, AEROSPIKE_ERR, "Support for async is disabled, build software with async option");
	PyObject *py_err = NULL, *exception_type = NULL;
	error_to_pyobject(&err, &py_err);
	exception_type = raise_exception(&err);
	PyErr_SetObject(exception_type, py_err);
	Py_DECREF(py_err);
	return NULL;
#endif
	return PyLong_FromLong(0);
}
