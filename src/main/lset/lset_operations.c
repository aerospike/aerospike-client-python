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

#include <Python.h>
#include <stdbool.h>

#include <aerospike/aerospike_lset.h>
#include <aerospike/as_error.h>
#include <aerospike/as_ldt.h>

#include "client.h"
#include "conversions.h"
#include "lset.h"
#include "policy.h"

PyObject * AerospikeLSet_Add(AerospikeLSet * self, PyObject * args, PyObject * kwds)
{
    PyObject* py_value = NULL;
    as_error err;
    as_error_init(&err);

	static char * kwlist[] = {"value", NULL};

	// Python Function Argument Parsing
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "O:add", kwlist, 
			&py_value) == false ) {
		return NULL;
	}

    as_val * val = NULL;
    pyobject_to_val(&err, py_value, &val);
    if (err.code != AEROSPIKE_OK) {
        goto CLEANUP;
    }

    aerospike_lset_add(self->client->as, &err, NULL, &self->key, &self->lset, val);

CLEANUP:

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		return NULL;
	}
	return PyLong_FromLong(0);
}

PyObject * AerospikeLSet_Add_All(AerospikeLSet * self, PyObject * args, PyObject * kwds)
{
    PyObject* py_arglist = NULL;
    as_error err;
    as_error_init(&err);

	static char * kwlist[] = {"values", NULL};

	// Python Function Argument Parsing
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "O:add_all", kwlist, 
			&py_arglist)== false ) {
		return NULL;
	}

    /*
     * Convert python list to as list 
     */
    if ( !PyList_Check(py_arglist)) {
        goto CLEANUP;
    }

    as_list* arglist = NULL;
    pyobject_to_list(&err, py_arglist, &arglist);
    if (err.code != AEROSPIKE_OK) {
        goto CLEANUP;
    }

    aerospike_lset_add_all(self->client->as, &err, NULL,
            &self->key, &self->lset, arglist);

CLEANUP:

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		return NULL;
	}

	return PyLong_FromLong(0);
}

PyObject * AerospikeLSet_Get(AerospikeLSet * self, PyObject * args, PyObject * kwds)
{
    PyObject* py_value = NULL;
    as_error err;
    as_error_init(&err);

	static char * kwlist[] = {"value", NULL};

	// Python Function Argument Parsing
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "O:get", kwlist, 
			&py_value) == false ) {
		return NULL;
	}

    as_val * val = NULL;
    pyobject_to_val(&err, py_value, &val);
    if (err.code != AEROSPIKE_OK) {
        goto CLEANUP;
    }

    as_val* return_val_p = NULL;
    aerospike_lset_get(self->client->as, &err, NULL, &self->key, &self->lset, val, &return_val_p);

    if (err.code != AEROSPIKE_OK) {
        goto CLEANUP;
    }

    PyObject * py_return_val = NULL;
    val_to_pyobject(&err, return_val_p, &py_return_val);

CLEANUP:

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		return NULL;
	}
    return py_return_val;
}

PyObject * AerospikeLSet_Filter(AerospikeLSet * self, PyObject * args, PyObject * kwds)
{
    char* filter_name = NULL;
    PyObject * py_args = NULL; 
    as_error err;
    as_error_init(&err);

	static char * kwlist[] = {"udf_function_name", "args", NULL};

	// Python Function Argument Parsing
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "|sO:filter", kwlist, 
			&filter_name, &py_args) == false ) {
		return NULL;
	}

    if ( !PyList_Check(py_args)) {
        goto CLEANUP;
    }

    as_list* arg_list = NULL;
    pyobject_to_list(&err, py_args, &arg_list);

    as_list* elements_list = NULL;
    aerospike_lset_filter(self->client->as, &err, NULL, &self->key,
            &self->lset, filter_name, arg_list, &elements_list);

    if (err.code != AEROSPIKE_OK) {
        goto CLEANUP;
    }

    PyObject* py_list = NULL;
    list_to_pyobject(&err, elements_list, &py_list);
    
CLEANUP:

    if (elements_list) {
        as_list_destroy(elements_list);
    }

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		return NULL;
	}
	return py_list;
}

PyObject * AerospikeLSet_Destroy(AerospikeLSet * self, PyObject * args, PyObject * kwds)
{
    as_error err;
    as_error_init(&err);

    aerospike_lset_destroy(self->client->as, &err, NULL, &self->key, &self->lset);

CLEANUP:

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		return NULL;
	}

	return PyLong_FromLong(0);
}

PyObject * AerospikeLSet_Exists(AerospikeLSet * self, PyObject * args, PyObject * kwds)
{
    bool exists = false;
    as_error err;
    as_error_init(&err);
    PyObject * py_value = NULL;

	static char * kwlist[] = {"value", NULL};

	// Python Function Argument Parsing
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "O:exists", kwlist, 
			&py_value) == false ) {
		return NULL;
	}

    as_val * val = NULL;
    pyobject_to_val(&err, py_value, &val);
    if (err.code != AEROSPIKE_OK) {
        goto CLEANUP;
    }

    aerospike_lset_exists(self->client->as, &err, NULL, &self->key,
            &self->lset, val, &exists);

CLEANUP:
	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		return NULL;
	}
    return PyBool_FromLong(exists);
}

PyObject * AerospikeLSet_Remove(AerospikeLSet * self, PyObject * args, PyObject * kwds)
{
    PyObject* py_value = NULL;
    as_error err;
    as_error_init(&err);

	static char * kwlist[] = {"element", NULL};

	// Python Function Argument Parsing
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "O:remove", kwlist, 
			&py_value) == false ) {
		return NULL;
	}

    as_val * val = NULL;
    pyobject_to_val(&err, py_value, &val);
    if (err.code != AEROSPIKE_OK) {
        goto CLEANUP;
    }

    aerospike_lset_remove(self->client->as, &err, NULL, &self->key, &self->lset, val);

CLEANUP:

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		return NULL;
	}
	return PyLong_FromLong(0);
}

PyObject * AerospikeLSet_Size(AerospikeLSet * self, PyObject * args, PyObject * kwds)
{
    long size = 0;
    as_error err;
    as_error_init(&err);

    aerospike_lset_size(self->client->as, &err, NULL, &self->key, &self->lset, &size);

CLEANUP:

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		return NULL;
	}
    return PyLong_FromLong(size);
}

PyObject * AerospikeLSet_Config(AerospikeLSet * self, PyObject * args, PyObject * kwds)
{
    /*
     * To be implemented.
     */
}
