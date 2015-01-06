/*******************************************************************************
 * Copyright 2013-2014 Aerospike, Inc.
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
		return Py_BuildValue("iOO", AS_PREDICATE_INTEGER_EQUAL, py_bin, py_val);
	}
	else if ( PyString_Check(py_val) ) {
		return Py_BuildValue("iOO", AS_PREDICATE_STRING_EQUAL, py_bin, py_val);
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
		return Py_BuildValue("iOOO", AS_PREDICATE_INTEGER_RANGE, py_bin, py_min, py_max);
	}

exit:
	Py_INCREF(Py_None);
	return Py_None;
}

static PyMethodDef AerospikePredicates_Methods[] = {
	{"equals",		(PyCFunction) AerospikePredicates_Equals,	METH_VARARGS, "Tests whether a bin's value equals the specified value."},
	{"between",		(PyCFunction) AerospikePredicates_Between,	METH_VARARGS, "Tests whether a bin's value is within the specified range."},
	{NULL, NULL, 0, NULL}
};


PyObject * AerospikePredicates_New(void)
{
	PyObject * module = Py_InitModule3("aerospike.predicates", AerospikePredicates_Methods, "Query Predicates");
	return module;
}
