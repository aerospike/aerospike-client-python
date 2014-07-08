#include <Python.h>
#include <stdbool.h>
#include <stdint.h>
#include <string.h>

#include "client.h"
#include "key.h"
#include "query.h"
#include "scan.h"

static PyMethodDef Aerospike_Methods[] = {
	
	// Create a new client object to the database
	{"client",		(PyCFunction) AerospikeClient_Create,	METH_VARARGS | METH_KEYWORDS, "Create a new client."},
	
	{NULL, NULL, 0, NULL}
};

PyMODINIT_FUNC initaerospike()
{
	PyObject * m;

	PyEval_InitThreads();

	AerospikeClient_Ready();
	AerospikeKey_Ready();
	AerospikeQuery_Ready();
	AerospikeScan_Ready();


	m = Py_InitModule3("aerospike", Aerospike_Methods, "documentation string ....");

	if (m == NULL)
		return;
}
