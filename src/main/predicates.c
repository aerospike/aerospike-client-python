/*******************************************************************************
 * Copyright 2013-2015 Aerospike, Inc.
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

/**
 * from aerospike import predicates as p
 *
 * q = client.query(ns,set).where(p.equals("bin",1))
 */

#include <Python.h>
#include <aerospike/as_query.h>
#include <aerospike/as_error.h>

#include "conversions.h"

static PyObject * AerospikePredicates_Equals(PyObject * self, PyObject * args)
{
	PyObject * py_bin = NULL;
	PyObject * py_val = NULL;

	if ( PyArg_ParseTuple(args, "OO:equals", 
			&py_bin, &py_val) == false ) {
		goto exit;
	}

	if ( PyInt_Check(py_val) || PyLong_Check(py_val) ) {
		return Py_BuildValue("iiOO", AS_PREDICATE_EQUAL, AS_INDEX_NUMERIC, py_bin, py_val);
	}
	else if ( PyString_Check(py_val) || PyUnicode_Check(py_val) ) {
		return Py_BuildValue("iiOO", AS_PREDICATE_EQUAL, AS_INDEX_STRING, py_bin, py_val);
	}

exit:
	Py_INCREF(Py_None);
	return Py_None;
}

static PyObject * AerospikePredicates_Contains(PyObject * self, PyObject * args)
{
	PyObject * py_bin = NULL;
	PyObject * py_indextype = NULL;
	PyObject * py_val = NULL;
	int index_type;

	if ( PyArg_ParseTuple(args, "OOO:equals", 
			&py_bin, &py_indextype, &py_val) == false ) {
		goto exit;
	}

	if(PyInt_Check(py_indextype)) {
		index_type = PyInt_AsLong(py_indextype);
	} else if (PyLong_Check(py_indextype)) {
		index_type = PyLong_AsLongLong(py_indextype);
	} else {
		goto exit;
	}

	if (PyInt_Check(py_val) || PyLong_Check(py_val)) {
		return Py_BuildValue("iiOOOi", AS_PREDICATE_EQUAL, AS_INDEX_NUMERIC, py_bin, py_val, Py_None, index_type);
	}
	else if (PyString_Check(py_val) || PyUnicode_Check(py_val)) {
		return Py_BuildValue("iiOOOi", AS_PREDICATE_EQUAL, AS_INDEX_STRING, py_bin, py_val, Py_None, index_type);
	}

exit:
	Py_INCREF(Py_None);
	return Py_None;
}

static PyObject * AerospikePredicates_RangeContains(PyObject * self, PyObject * args)
{
	PyObject * py_bin = NULL;
	PyObject * py_indextype = NULL;
	PyObject * py_min = NULL;
	PyObject * py_max= NULL;
	int index_type;

	if ( PyArg_ParseTuple(args, "OOOO:equals",
			&py_bin, &py_indextype, &py_min, &py_max) == false ) {
		goto exit;
	}

	if(PyInt_Check(py_indextype)) {
		index_type = PyInt_AsLong(py_indextype);
	} else if (PyLong_Check(py_indextype)) {
		index_type = PyLong_AsLongLong(py_indextype);
	} else {
		goto exit;
	}

	if ((PyInt_Check(py_min) || PyLong_Check(py_min)) && (PyInt_Check(py_max) || PyLong_Check(py_max))) {
		return Py_BuildValue("iiOOOi", AS_PREDICATE_RANGE, AS_INDEX_NUMERIC, py_bin, py_min, py_max, index_type);
	}

exit:
	Py_INCREF(Py_None);
	return Py_None;
}

static PyObject * AerospikePredicates_Between(PyObject * self, PyObject * args)
{
	PyObject * py_bin = NULL;
	PyObject * py_min = NULL;
	PyObject * py_max = NULL;

	if ( PyArg_ParseTuple(args, "OOO:between",
			&py_bin, &py_min, &py_max) == false ) {
		goto exit;
	}

	if ( (PyInt_Check(py_min) || PyLong_Check(py_min)) && (PyInt_Check(py_max) || PyLong_Check(py_max)) ) {
		return Py_BuildValue("iiOOO", AS_PREDICATE_RANGE, AS_INDEX_NUMERIC, py_bin, py_min, py_max);
	}

exit:
	Py_INCREF(Py_None);
	return Py_None;
}

static PyObject * AerospikePredicates_GeoWithin(PyObject * self, PyObject * args)
{
	PyObject * py_bin = NULL;
	PyObject * py_shape = NULL;

	if ( PyArg_ParseTuple(args, "OO:equals", 
			&py_bin, &py_shape) == false ) {
		goto exit;
	}

	if ( PyString_Check(py_shape) || PyUnicode_Check(py_shape) ) {
		return Py_BuildValue("iiOO", AS_PREDICATE_RANGE, AS_INDEX_GEO2DSPHERE, py_bin, py_shape);
	}

exit:
	Py_INCREF(Py_None);
	return Py_None;
}
static PyMethodDef AerospikePredicates_Methods[] = {
	{"equals",		(PyCFunction) AerospikePredicates_Equals,	METH_VARARGS, "Tests whether a bin's value equals the specified value."},
	{"between",		(PyCFunction) AerospikePredicates_Between,	METH_VARARGS, "Tests whether a bin's value is within the specified range."},
	{"contains",	(PyCFunction) AerospikePredicates_Contains,	METH_VARARGS, "Tests whether a bin's value equals the specified value in a complex data type"},
	{"range",	(PyCFunction) AerospikePredicates_RangeContains,	METH_VARARGS, "Tests whether a bin's value is within the specified range in a complex data type"},
	{"geo_within",		(PyCFunction) AerospikePredicates_GeoWithin,	METH_VARARGS, "Tests whether a bin's value is within the specified shape."},
	{NULL, NULL, 0, NULL}
};


PyObject * AerospikePredicates_New(void)
{
	PyObject * module = Py_InitModule3("aerospike.predicates", AerospikePredicates_Methods, "Query Predicates");
	return module;
}
