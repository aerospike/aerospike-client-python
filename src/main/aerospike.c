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
	{"client",		(PyCFunction) AerospikeClient_New,	METH_VARARGS | METH_KEYWORDS, "Create a new client."},	
	{NULL, NULL, 0, NULL}
};

PyMODINIT_FUNC initaerospike()
{
	// Makes things "thread-safe"
	PyEval_InitThreads();

	// Make ann types, ready
	AerospikeClient_Ready();
	AerospikeKey_Ready();
	AerospikeQuery_Ready();
	AerospikeScan_Ready();


	PyObject * aerospike = Py_InitModule3("aerospike", Aerospike_Methods, "documentation string ....");

	PyObject * predicates = AerospikePredicates_New();
	PyModule_AddObject(aerospike, PyModule_GetName(predicates), predicates);
}
