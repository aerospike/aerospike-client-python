#include <Python.h>
#include <stdbool.h>
#include <stdint.h>
#include <string.h>

#include "client.h"
#include "key.h"
#include "query.h"
#include "scan.h"
#include "predicates.h"

static PyMethodDef Aerospike_Methods[] = {

	{"client",		(PyCFunction) AerospikeClient_New,	METH_VARARGS | METH_KEYWORDS, 
					"Create a new instance of Client class."},	
	
	{NULL}
};

PyMODINIT_FUNC initaerospike()
{
	// Makes things "thread-safe"
	PyEval_InitThreads();

	// aerospike Module
	PyObject * aerospike = Py_InitModule3("aerospike", Aerospike_Methods, "documentation string ....");
	
	PyTypeObject * client = AerospikeClient_Ready();
	Py_INCREF(client);
	PyModule_AddObject(aerospike, "Client", (PyObject *) client);

	PyTypeObject * key = AerospikeKey_Ready();
	Py_INCREF(key);
	PyModule_AddObject(aerospike, "Key", (PyObject *) key);

	PyTypeObject * query = AerospikeQuery_Ready();
	Py_INCREF(query);
	PyModule_AddObject(aerospike, "Query", (PyObject *) query);

	PyTypeObject * scan = AerospikeScan_Ready();
	Py_INCREF(scan);
	PyModule_AddObject(aerospike, "Scan", (PyObject *) scan);

	PyObject * predicates = AerospikePredicates_New();
	PyModule_AddObject(aerospike, PyModule_GetName(predicates), predicates);
}
