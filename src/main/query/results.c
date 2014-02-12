#include <Python.h>
#include <pthread.h>
#include <stdbool.h>

#include <aerospike/aerospike_query.h>
#include <aerospike/as_error.h>
#include <aerospike/as_query.h>

#include "client.h"
#include "conversions.h"
#include "query.h"

#undef TRACE
#define TRACE()

static bool each_result(const as_val * val, void * udata)
{
	if ( !val ) {
		return false;
	}

	PyObject * py_results = (PyObject *) udata;
	PyObject * py_result = NULL;

	as_error err;
	
	TRACE();

	PyGILState_STATE gstate;
	gstate = PyGILState_Ensure();

	TRACE();

	val_to_pyobject(&err, val, &py_result);

	TRACE();

	if ( py_result ) {
	
		TRACE();
		PyList_Append(py_results, py_result);
	
		TRACE();
		Py_DECREF(py_result);
	}
	
	TRACE();

	PyGILState_Release(gstate);

	TRACE();
	return true;
}

PyObject * AerospikeQuery_Results(AerospikeQuery * self, PyObject * args, PyObject * kwds)
{
	AerospikeQuery * py_query = self;
	AerospikeClient * py_client = py_query->client;
	PyObject * py_policy = NULL;
	
	static char * kwlist[] = {"policy", NULL};

	if ( PyArg_ParseTupleAndKeywords(args, kwds, "|O:foreach", kwlist, &py_policy) == false ) {
		return NULL;
	}

	as_error err;
	as_error_init(&err);

	TRACE();
	PyObject * py_results = PyList_New(0);
	
	TRACE();
	PyThreadState * _save = PyEval_SaveThread();
	
	TRACE();
    aerospike_query_foreach(py_client->as, &err, NULL, &py_query->query, each_result, py_results);
    
	TRACE();
	PyEval_RestoreThread(_save);
  	
	TRACE();
	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		TRACE();
		return NULL;
	}

	TRACE();
	return py_results;
}


