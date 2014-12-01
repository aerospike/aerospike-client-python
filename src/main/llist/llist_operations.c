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

#include <aerospike/aerospike_llist.h>
#include <aerospike/as_error.h>
#include <aerospike/as_ldt.h>

#include "client.h"
#include "conversions.h"
#include "llist.h"
#include "policy.h"

PyObject * AerospikeLList_Add(AerospikeLList * self, PyObject * args, PyObject * kwds)
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

    aerospike_llist_add(self->client->as, &err, NULL, &self->key, &self->llist, val);

CLEANUP:

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		return NULL;
	}
	return PyLong_FromLong(0);
}

PyObject * AerospikeLList_Add_All(AerospikeLList * self, PyObject * args, PyObject * kwds)
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
    as_list* arglist = NULL;
    pyobject_to_list(&err, py_arglist, &arglist);
    if (err.code != AEROSPIKE_OK) {
        goto CLEANUP;
    }

    aerospike_llist_add_all(self->client->as, &err, NULL,
            &self->key, &self->llist, arglist);

CLEANUP:

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		return NULL;
	}

	return PyLong_FromLong(0);
}

PyObject * AerospikeLList_Get(AerospikeLList * self, PyObject * args, PyObject * kwds)
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

    as_list* list_p = NULL;
    aerospike_llist_find(self->client->as, &err, NULL, &self->key, &self->llist, val, &list_p);

    if (err.code != AEROSPIKE_OK) {
        goto CLEANUP;
    }

    PyObject * py_list = NULL;
    list_to_pyobject(&err, list_p, &py_list);

CLEANUP:

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		return NULL;
	}
    return py_list;
}

PyObject * AerospikeLList_Filter(AerospikeLList * self, PyObject * args, PyObject * kwds)
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

    as_list* arg_list = NULL;
    pyobject_to_list(&err, py_args, &arg_list);

    as_list* elements_list = NULL;
    aerospike_llist_filter(self->client->as, &err, NULL, &self->key,
            &self->llist, filter_name, arg_list, &elements_list);

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

PyObject * AerospikeLList_Destroy(AerospikeLList * self, PyObject * args, PyObject * kwds)
{
    as_error err;
    as_error_init(&err);

    aerospike_llist_destroy(self->client->as, &err, NULL, &self->key, &self->llist);

CLEANUP:

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		return NULL;
	}

	return PyLong_FromLong(0);
}

PyObject * AerospikeLList_Remove(AerospikeLList * self, PyObject * args, PyObject * kwds)
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

    aerospike_llist_remove(self->client->as, &err, NULL, &self->key, &self->llist, val);

CLEANUP:

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		return NULL;
	}
	return PyLong_FromLong(0);
}

PyObject * AerospikeLList_Size(AerospikeLList * self, PyObject * args, PyObject * kwds)
{
    long size = 0;
    as_error err;
    as_error_init(&err);

    aerospike_llist_size(self->client->as, &err, NULL, &self->key, &self->llist, &size);

CLEANUP:

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		return NULL;
	}
    return PyLong_FromLong(size);
}

PyObject * AerospikeLList_Config(AerospikeLList * self, PyObject * args, PyObject * kwds)
{
    /*
     * To be implemented.
     */
}
