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

static PyMethodDef Aerospike_Methods[] = {

    //Serialization
    {"set_serializer", (PyCFunction)AerospikeClient_Set_Serializer,
     METH_VARARGS | METH_KEYWORDS, "Sets the serializer"},
    {"set_deserializer", (PyCFunction)AerospikeClient_Set_Deserializer,
     METH_VARARGS | METH_KEYWORDS, "Sets the deserializer"},
    {"unset_serializers", (PyCFunction)AerospikeClient_Unset_Serializers,
     METH_VARARGS | METH_KEYWORDS, "Unsets the serializer and deserializer"},

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
    {"get_partition_id", (PyCFunction)Aerospike_Get_Partition_Id, METH_VARARGS,
     "Get partition ID for given digest"},

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

#define AUTH_MODE_CONSTANTS_ARR_SIZE                                           \
    (sizeof(auth_mode_constants) / sizeof(AerospikeConstants))

struct Aerospike_State {
    PyObject *exception;
    PyTypeObject *client;
    PyTypeObject *query;
    PyTypeObject *scan;
    PyTypeObject *kdict;
    PyObject *predicates;
    PyTypeObject *geospatial;
    PyTypeObject *null_object;
    PyTypeObject *wildcard_object;
    PyTypeObject *infinite_object;
};

#define Aerospike_State(o) ((struct Aerospike_State *)PyModule_GetState(o))

static int Aerospike_Clear(PyObject *aerospike)
{
    Py_CLEAR(Aerospike_State(aerospike)->exception);
    Py_CLEAR(Aerospike_State(aerospike)->client);
    Py_CLEAR(Aerospike_State(aerospike)->query);
    Py_CLEAR(Aerospike_State(aerospike)->scan);
    Py_CLEAR(Aerospike_State(aerospike)->kdict);
    Py_CLEAR(Aerospike_State(aerospike)->predicates);
    Py_CLEAR(Aerospike_State(aerospike)->geospatial);
    Py_CLEAR(Aerospike_State(aerospike)->null_object);
    Py_CLEAR(Aerospike_State(aerospike)->wildcard_object);
    Py_CLEAR(Aerospike_State(aerospike)->infinite_object);

    return 0;
}

PyMODINIT_FUNC PyInit_aerospike(void)
{

    const char version[] = "13.0.0";
    // Makes things "thread-safe"
    Py_Initialize();
    int i = 0;

    static struct PyModuleDef moduledef = {PyModuleDef_HEAD_INIT,
                                           "aerospike",
                                           "Aerospike Python Client",
                                           sizeof(struct Aerospike_State),
                                           Aerospike_Methods,
                                           NULL,
                                           NULL,
                                           Aerospike_Clear};

    PyObject *aerospike = PyModule_Create(&moduledef);

    // In case adding objects to module fails, we can properly deallocate the module state later
    memset(Aerospike_State(aerospike), NULL, sizeof(struct Aerospike_State));

    Aerospike_Enable_Default_Logging();

    py_global_hosts = PyDict_New();

    PyModule_AddStringConstant(aerospike, "__version__", version);

    PyObject *exception = AerospikeException_New();
    Py_INCREF(exception);
    int retval = PyModule_AddObject(aerospike, "exception", exception);
    if (retval == -1) {
        goto CLEANUP;
    }
    Aerospike_State(aerospike)->exception = exception;

    PyTypeObject *client = AerospikeClient_Ready();
    Py_INCREF(client);
    retval = PyModule_AddObject(aerospike, "Client", (PyObject *)client);
    if (retval == -1) {
        goto CLEANUP;
    }
    Aerospike_State(aerospike)->client = client;

    PyTypeObject *query = AerospikeQuery_Ready();
    Py_INCREF(query);
    retval = PyModule_AddObject(aerospike, "Query", (PyObject *)query);
    if (retval == -1) {
        goto CLEANUP;
    }
    Aerospike_State(aerospike)->query = query;

    PyTypeObject *scan = AerospikeScan_Ready();
    Py_INCREF(scan);
    retval = PyModule_AddObject(aerospike, "Scan", (PyObject *)scan);
    if (retval == -1) {
        goto CLEANUP;
    }
    Aerospike_State(aerospike)->scan = scan;

    PyTypeObject *kdict = AerospikeKeyOrderedDict_Ready();
    Py_INCREF(kdict);
    retval = PyModule_AddObject(aerospike, "KeyOrderedDict", (PyObject *)kdict);
    if (retval == -1) {
        goto CLEANUP;
    }
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
    declare_log_constants(aerospike);

    PyObject *predicates = AerospikePredicates_New();
    Py_INCREF(predicates);
    retval = PyModule_AddObject(aerospike, "predicates", predicates);
    if (retval == -1) {
        goto CLEANUP;
    }
    Aerospike_State(aerospike)->predicates = predicates;

    PyTypeObject *geospatial = AerospikeGeospatial_Ready();
    Py_INCREF(geospatial);
    retval = PyModule_AddObject(aerospike, "GeoJSON", (PyObject *)geospatial);
    if (retval == -1) {
        goto CLEANUP;
    }
    Aerospike_State(aerospike)->geospatial = geospatial;

    PyTypeObject *null_object = AerospikeNullObject_Ready();
    Py_INCREF(null_object);
    retval = PyModule_AddObject(aerospike, "null", (PyObject *)null_object);
    if (retval == -1) {
        goto CLEANUP;
    }
    Aerospike_State(aerospike)->null_object = null_object;

    PyTypeObject *wildcard_object = AerospikeWildcardObject_Ready();
    Py_INCREF(wildcard_object);
    retval = PyModule_AddObject(aerospike, "CDTWildcard",
                                (PyObject *)wildcard_object);
    if (retval == -1) {
        goto CLEANUP;
    }
    Aerospike_State(aerospike)->wildcard_object = wildcard_object;

    PyTypeObject *infinite_object = AerospikeInfiniteObject_Ready();
    Py_INCREF(infinite_object);
    retval = PyModule_AddObject(aerospike, "CDTInfinite",
                                (PyObject *)infinite_object);
    if (retval == -1) {
        goto CLEANUP;
    }
    Aerospike_State(aerospike)->infinite_object = infinite_object;

    return aerospike;

CLEANUP:
    Aerospike_Clear(aerospike);
    return NULL;
}
