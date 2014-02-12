#include <Python.h>
#include <stdbool.h>

#include <aerospike/aerospike_query.h>
#include <aerospike/as_error.h>
#include <aerospike/as_query.h>

#include "client.h"
#include "conversions.h"
#include "query.h"

// #undef TRACE
// #define TRACE()

typedef struct {
	as_error error;
	PyObject * callback;
} LocalData;


static bool each_result(const as_val * val, void * udata)
{
	if ( !val ) {
		return false;
	}

	LocalData * data = (LocalData *) udata;
	as_error * err = &data->error;
	PyObject * py_callback = data->callback;

	PyObject * py_arglist = NULL; 
	PyObject * py_result = NULL;

	PyGILState_STATE gstate;
	gstate = PyGILState_Ensure();

	TRACE();

	val_to_pyobject(err, val, &py_result);

	TRACE();

	py_arglist = Py_BuildValue("(O)", py_result);
	
	TRACE();

	PyGILState_Release(gstate);
	
	TRACE();

	PyEval_CallObject(py_callback, py_arglist);
	
	TRACE();

	gstate = PyGILState_Ensure();

	TRACE();

	Py_DECREF(py_arglist);

	PyGILState_Release(gstate);
	
	return true;
}

PyObject * AerospikeQuery_Foreach(AerospikeQuery * self, PyObject * args, PyObject * kwds)
{
	AerospikeQuery * py_query = self;
	AerospikeClient * py_client = py_query->client;
	PyObject * py_callback = NULL;
	PyObject * py_policy = NULL;

	static char * kwlist[] = {"callback", "policy", NULL};

	if ( PyArg_ParseTupleAndKeywords(args, kwds, "O|O:foreach", kwlist, &py_callback, &py_policy) == false ) {
		return NULL;
	}

	as_error err;
	as_error_init(&err);

	LocalData data;
	data.callback = py_callback;
	as_error_init(&data.error);

	PyThreadState * _save = PyEval_SaveThread();
	
	aerospike_query_foreach(py_client->as, &err, NULL, &py_query->query, each_result, &data);

	PyEval_RestoreThread(_save);
	
	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		return NULL;
	}

	Py_INCREF(Py_None);
	return Py_None;
}






