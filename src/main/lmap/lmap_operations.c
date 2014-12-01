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

#include <aerospike/aerospike_lmap.h>
#include <aerospike/as_error.h>
#include <aerospike/as_ldt.h>

#include "client.h"
#include "conversions.h"
#include "lmap.h"
#include "policy.h"

PyObject * AerospikeLMap_Add(AerospikeLMap * self, PyObject * args, PyObject * kwds)
{
    PyObject* py_map_key = NULL;
    PyObject* py_map_value = NULL;
    as_error err;
    as_error_init(&err);

	static char * kwlist[] = {"key", "value", NULL};

	// Python Function Argument Parsing
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "OO:add", kwlist, 
			&py_map_key, &py_map_value) == false ) {
		return NULL;
	}

    as_val * map_key = NULL;
    pyobject_to_val(&err, py_map_key, &map_key);
    if (err.code != AEROSPIKE_OK) {
        goto CLEANUP;
    }

    as_val * map_value = NULL;
    pyobject_to_val(&err, py_map_value, &map_value);
    if (err.code != AEROSPIKE_OK) {
        goto CLEANUP;
    }

    aerospike_lmap_put(self->client->as, &err, NULL, &self->key,
            &self->lmap, map_key, map_value);

CLEANUP:

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		return NULL;
	}
	return PyLong_FromLong(0);
}

PyObject * AerospikeLMap_Add_All(AerospikeLMap * self, PyObject * args, PyObject * kwds)
{
    PyObject* py_values = NULL;
    as_error err;
    as_error_init(&err);

	static char * kwlist[] = {"values", NULL};

	// Python Function Argument Parsing
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "O:add_all", kwlist, 
			&py_values)== false ) {
		return NULL;
	}

    if (!PyDict_Check(py_values)) {
        goto CLEANUP;
    }
    /*
     * Convert python map to as map
     */
    as_map* map_values = NULL;
    pyobject_to_map(&err, py_values, &map_values);
    if (err.code != AEROSPIKE_OK) {
        goto CLEANUP;
    }

    aerospike_lmap_put_all(self->client->as, &err, NULL,
            &self->key, &self->lmap, map_values);

CLEANUP:

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		return NULL;
	}

	return PyLong_FromLong(0);
}

PyObject * AerospikeLMap_Get(AerospikeLMap * self, PyObject * args, PyObject * kwds)
{
    PyObject* py_map_key = NULL;
    as_error err;
    as_error_init(&err);

	static char * kwlist[] = {"key", NULL};

	// Python Function Argument Parsing
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "O:get", kwlist, 
			&py_map_key) == false ) {
		return NULL;
	}

    as_val * map_key = NULL;
    pyobject_to_val(&err, py_map_key, &map_key);
    if (err.code != AEROSPIKE_OK) {
        goto CLEANUP;
    }

    as_val* map_value = NULL;
    aerospike_lmap_get(self->client->as, &err, NULL, &self->key,
            &self->lmap, map_key, &map_value);

    if (err.code != AEROSPIKE_OK) {
        goto CLEANUP;
    }

    PyObject * py_map_val = NULL;
    val_to_pyobject(&err, map_value, &py_map_val);

CLEANUP:

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		return NULL;
	}
    return py_map_val;
}

PyObject * AerospikeLMap_Filter(AerospikeLMap * self, PyObject * args, PyObject * kwds)
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

    as_map* elements = NULL;
    aerospike_lmap_filter(self->client->as, &err, NULL, &self->key,
            &self->lmap, filter_name, arg_list, &elements);

    if (err.code != AEROSPIKE_OK) {
        goto CLEANUP;
    }

    PyObject* py_map = NULL;
    map_to_pyobject(&err, elements, &py_map);
    
CLEANUP:

    if (elements) {
        as_map_destroy(elements);
    }

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		return NULL;
	}
	return py_map;
}

PyObject * AerospikeLMap_Destroy(AerospikeLMap * self, PyObject * args, PyObject * kwds)
{
    as_error err;
    as_error_init(&err);

    aerospike_lmap_destroy(self->client->as, &err, NULL, &self->key, &self->lmap);

CLEANUP:

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		return NULL;
	}

	return PyLong_FromLong(0);
}

PyObject * AerospikeLMap_Remove(AerospikeLMap * self, PyObject * args, PyObject * kwds)
{
    PyObject* py_map_key = NULL;
    as_error err;
    as_error_init(&err);

	static char * kwlist[] = {"key", NULL};

	// Python Function Argument Parsing
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "O:remove", kwlist, 
			&py_map_key) == false ) {
		return NULL;
	}

    as_val * map_key = NULL;
    pyobject_to_val(&err, py_map_key, &map_key);
    if (err.code != AEROSPIKE_OK) {
        goto CLEANUP;
    }

    aerospike_lmap_remove(self->client->as, &err, NULL, &self->key,
            &self->lmap, map_key);

CLEANUP:

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		return NULL;
	}
	return PyLong_FromLong(0);
}

PyObject * AerospikeLMap_Size(AerospikeLMap * self, PyObject * args, PyObject * kwds)
{
    long size = 0;
    as_error err;
    as_error_init(&err);

    aerospike_lmap_size(self->client->as, &err, NULL, &self->key, &self->lmap, &size);

CLEANUP:

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		return NULL;
	}
    return PyLong_FromLong(size);
}

PyObject * AerospikeLMap_Config(AerospikeLMap * self, PyObject * args, PyObject * kwds)
{
    /*
     * To be implemented.
     */
}
