#include <Python.h>
#include <stdbool.h>

#include <aerospike/as_query.h>

#include "client.h"
#include "query.h"

#undef TRACE
#define TRACE()

PyObject * AerospikeQuery_Select(AerospikeQuery * self, PyObject * args, PyObject * kwds)
{
	TRACE();
	
	AerospikeQuery * py_query = self;
	
	as_query_select_init(&py_query->query, 100);

	int nbins = (int) PyTuple_Size(args);
	for ( int i = 0; i < nbins; i++ ) {
		PyObject * py_bin = PyTuple_GetItem(args, i);
		if ( PyString_Check(py_bin) ) {
			TRACE();
			char * bin = PyString_AsString(py_bin);
			as_query_select(&py_query->query, bin);
		}
		else {
			TRACE();
		}
	}

	Py_INCREF(self);
	return (PyObject *) self;
}