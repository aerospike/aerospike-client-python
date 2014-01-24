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
	PyObject * py_operator = NULL;
	PyObject * py_value = NULL;

	static char * kwlist[] = {"bin", "operator", "value", NULL};

	TRACE();
	
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "OOO:where", kwlist, &py_bin, &py_operator, &py_value) == false ) {
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
	
	if ( PyString_Check(py_operator) ) {
		char * o = PyString_AsString(py_operator);
		if ( strcmp(o, "equals") == 0 ) {

			if ( PyInt_Check(py_value) || PyLong_Check(py_value) ) {
				int64_t i = pyobject_to_int64(py_value);
				as_query_where(&py_query->query, bin, integer_equals(i));
				TRACE();
			}
			else if ( PyString_Check(py_value) ) {
				char * s = PyString_AsString(py_value);
				as_query_where(&py_query->query, bin, string_equals(s));
				TRACE();
			}
			else {
				// unhandled type
				TRACE();
			}
		}
		else if ( strcmp(o, "between") == 0 ) {
			if ( PyTuple_Check(py_value) ) {
				Py_ssize_t size = PyTuple_Size(py_value);
				if ( size == 2 ) {
					PyObject * py_min = PyTuple_GetItem(py_value, 0);
					PyObject * py_max = PyTuple_GetItem(py_value, 1);
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
					
					as_query_where(&py_query->query, bin, integer_range(min, max));

					TRACE();
				}
				else {
					// should be a tuple(2)
					TRACE();
				}
			}
			else {
				// must be a tuple
				TRACE();
			}
		}
		else {
			// unknown operator
		}
	}
	else {
		// operator must be a string
		TRACE();
	}

	Py_INCREF(self);
	return (PyObject *) self;
}