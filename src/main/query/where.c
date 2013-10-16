#include <Python.h>
#include <stdbool.h>

#include <aerospike/as_query.h>

#include "client.h"
#include "query.h"

#undef TRACE
#define TRACE()

static int64_t pyobject_to_int64(PyObject * py_obj)
{
	if ( PyInt_Check(py_obj) ) {
		return PyInt_AsLong(py_obj);
	}
	else if ( PyLong_Check(py_obj) ) {
		return PyLong_AsLongLong(py_obj);
	}
	else {
		return 0;
	}
}

PyObject * AerospikeQuery_Where(AerospikeQuery * self, PyObject * args, PyObject * kwds)
{
	TRACE();
	
	AerospikeQuery * py_query = self;
	// AerospikeClient * py_client = self->client;
	PyObject * py_bin = NULL;
	PyObject * py_equals = NULL;
	PyObject * py_range = NULL;

	static char * kwlist[] = {"bin", "equals", "range", NULL};

	TRACE();
	
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "O|OO:get", kwlist, &py_bin, &py_equals, &py_range) == false ) {
		return NULL;
	}

	TRACE();
	
	as_query_where_init(&py_query->query, 1);

	TRACE();
	
	char * bin = NULL;

	if ( PyString_Check(py_bin) ) {
		bin = PyString_AsString(py_bin);
	}

	TRACE();
	
	if ( bin == NULL ) {
		// raise exception
		TRACE();
		return NULL;
	}

	TRACE();
	
	if ( py_equals != NULL ) {
		if ( PyInt_Check(py_equals) || PyLong_Check(py_equals) ) {
			int64_t i = pyobject_to_int64(py_equals);
			as_query_where(&py_query->query, bin, AS_PREDICATE_INTEGER_EQUAL, i);
			TRACE();
		}
		else if ( PyString_Check(py_equals) ) {
			char * s = PyString_AsString(py_equals);
			as_query_where(&py_query->query, bin, AS_PREDICATE_STRING_EQUAL, s);
			TRACE();
		}
		else {
			// raise exception
			TRACE();
		}
	}
	else if ( py_range != NULL ) {
		if ( PyTuple_Check(py_range) ) {
			Py_ssize_t size = PyTuple_Size(py_range);
			if ( size == 2 ) {
				PyObject * py_min = PyTuple_GetItem(py_range, 0);
				PyObject * py_max = PyTuple_GetItem(py_range, 1);
				int64_t min = 0;
				int64_t max = 0;

				if ( PyInt_Check(py_min) || PyLong_Check(py_min) ) {
					min = pyobject_to_int64(py_min);
					TRACE();
				}
				else {
					// raise exception
					TRACE();
				}

				if ( PyInt_Check(py_max) || PyLong_Check(py_max) ) {
					max = pyobject_to_int64(py_max);
					TRACE();
				}
				else {
					// raise exception
					TRACE();
				}

				as_query_where(&py_query->query, bin, AS_PREDICATE_INTEGER_RANGE, min, max);

				TRACE();
			}
			else {
				// raise exception
				TRACE();
			}
		}
		else {
			// raise exception
			TRACE();
		}
	}
	else {
		TRACE();
	}

	Py_INCREF(self);
	return (PyObject *) self;
}