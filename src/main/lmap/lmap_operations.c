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

/**
 ********************************************************************************************************
 * Add an object to the map.
 *
 * @param self                  AerospikeLMap object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 * 
 * Returns an integer status. 0(Zero) is success value.
 * In case of error,appropriate exceptions will be raised.
 ********************************************************************************************************
 */
PyObject * AerospikeLMap_Add(AerospikeLMap * self, PyObject * args, PyObject * kwds)
{
    PyObject* py_map_key = NULL;
    PyObject* py_map_value = NULL;
    PyObject* py_policy = NULL;
    as_policy_apply apply_policy;
    as_policy_apply* apply_policy_p = NULL;

    as_error err;
    as_error_init(&err);

	static char * kwlist[] = {"key", "value", "policy", NULL};

	// Python Function Argument Parsing
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "OO|O:add", kwlist, 
			&py_map_key, &py_map_value, &py_policy) == false ) {
		return NULL;
	}

    if (!self || !self->client->as) {
        as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
        goto CLEANUP;
    }

    if (py_policy) {
        validate_policy_apply(&err, py_policy, &apply_policy);
    }
    if (err.code != AEROSPIKE_OK) {
        goto CLEANUP;
    }

	// Convert python policy object to as_policy_apply
	pyobject_to_policy_apply(&err, py_policy, &apply_policy, &apply_policy_p);
	if ( err.code != AEROSPIKE_OK ) {
		goto CLEANUP;
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

    aerospike_lmap_put(self->client->as, &err, apply_policy_p, &self->key,
            &self->lmap, map_key, map_value);

CLEANUP:

    as_val_destroy(map_key);
    as_val_destroy(map_value);
	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
        Py_DECREF(py_err);
		return NULL;
	}
	return PyLong_FromLong(0);
}

/**
 ********************************************************************************************************
 * Add a list of objects to the map.
 *
 * @param self                  AerospikeLMap object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 * 
 * Returns an integer status. 0(Zero) is success value.
 * In case of error,appropriate exceptions will be raised.
 ********************************************************************************************************
 */
PyObject * AerospikeLMap_Add_All(AerospikeLMap * self, PyObject * args, PyObject * kwds)
{
    PyObject* py_values = NULL;
    PyObject* py_policy = NULL;
    as_policy_apply apply_policy;
    as_policy_apply* apply_policy_p = NULL;

    as_error err;
    as_error_init(&err);

	static char * kwlist[] = {"values", "policy", NULL};

	// Python Function Argument Parsing
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "O|O:add_all", kwlist, 
			&py_values, &py_policy)== false ) {
		return NULL;
	}

    if (!self || !self->client->as) {
        as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
        goto CLEANUP;
    }

    if (py_policy) {
        validate_policy_apply(&err, py_policy, &apply_policy);
    }
    if (err.code != AEROSPIKE_OK) {
        goto CLEANUP;
    }

	// Convert python policy object to as_policy_apply
	pyobject_to_policy_apply(&err, py_policy, &apply_policy, &apply_policy_p);
	if ( err.code != AEROSPIKE_OK ) {
		goto CLEANUP;
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

    aerospike_lmap_put_all(self->client->as, &err, apply_policy_p,
            &self->key, &self->lmap, map_values);

CLEANUP:

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
        Py_DECREF(py_err);
		return NULL;
	}

	return PyLong_FromLong(0);
}

/**
 ********************************************************************************************************
 * Get an object from the map.
 *
 * @param self                  AerospikeLMap object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 * 
 * Returns that entry with key,value pair.
 * In case of error,appropriate exceptions will be raised.
 ********************************************************************************************************
 */
PyObject * AerospikeLMap_Get(AerospikeLMap * self, PyObject * args, PyObject * kwds)
{
    PyObject* py_map_key = NULL;
    PyObject* py_policy = NULL;
    as_policy_apply apply_policy;
    as_policy_apply* apply_policy_p = NULL;

    as_error err;
    as_error_init(&err);

	static char * kwlist[] = {"key", "policy", NULL};

	// Python Function Argument Parsing
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "O|O:get", kwlist, 
			&py_map_key, &py_policy) == false ) {
		return NULL;
	}

    if (!self || !self->client->as) {
        as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
        goto CLEANUP;
    }

    if (py_policy) {
        validate_policy_apply(&err, py_policy, &apply_policy);
    }
    if (err.code != AEROSPIKE_OK) {
        goto CLEANUP;
    }

	// Convert python policy object to as_policy_apply
	pyobject_to_policy_apply(&err, py_policy, &apply_policy, &apply_policy_p);
	if ( err.code != AEROSPIKE_OK ) {
		goto CLEANUP;
	}

    as_val * map_key = NULL;
    pyobject_to_val(&err, py_map_key, &map_key);
    if (err.code != AEROSPIKE_OK) {
        goto CLEANUP;
    }

    as_val* map_key_value = NULL;
    aerospike_lmap_get(self->client->as, &err, apply_policy_p, &self->key,
            &self->lmap, map_key, &map_key_value);

    if (err.code != AEROSPIKE_OK) {
        goto CLEANUP;
    }

    PyObject * py_map_val = NULL;
    val_to_pyobject(&err, map_key_value, &py_map_val);

CLEANUP:

    if (map_key_value) {
        as_val_destroy(map_key_value);
    }

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
        Py_DECREF(py_err);
		return NULL;
	}
    return py_map_val;
}

/**
 ********************************************************************************************************
 * Scan the map and apply a predicate filter.
 *
 * @param self                  AerospikeLMap object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 * 
 * Returns a list of elements from the map after applying predicate.
 * In case of error,appropriate exceptions will be raised.
 ********************************************************************************************************
 */
PyObject * AerospikeLMap_Filter(AerospikeLMap * self, PyObject * args, PyObject * kwds)
{
    char* filter_name = NULL;
    PyObject * py_args = NULL; 
    PyObject* py_policy = NULL;
    as_policy_apply apply_policy;
    as_policy_apply* apply_policy_p = NULL;

    as_error err;
    as_error_init(&err);

	static char * kwlist[] = {"udf_function_name", "args", "policy", NULL};

	// Python Function Argument Parsing
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "|sOO:filter", kwlist, 
			&filter_name, &py_args, &py_policy) == false ) {
		return NULL;
	}

    if (!self || !self->client->as) {
        as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
        goto CLEANUP;
    }

    if (py_policy) {
        validate_policy_apply(&err, py_policy, &apply_policy);
    }
    if (err.code != AEROSPIKE_OK) {
        goto CLEANUP;
    }

	// Convert python policy object to as_policy_apply
	pyobject_to_policy_apply(&err, py_policy, &apply_policy, &apply_policy_p);
	if ( err.code != AEROSPIKE_OK ) {
		goto CLEANUP;
	}

    if ( !PyList_Check(py_args)) {
        goto CLEANUP;
    }

    as_list* arg_list = NULL;
    pyobject_to_list(&err, py_args, &arg_list);

    as_map* elements = NULL;
    aerospike_lmap_filter(self->client->as, &err, apply_policy_p, &self->key,
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
        Py_DECREF(py_err);
		return NULL;
	}
	return py_map;
}

/**
 ********************************************************************************************************
 * Delete the entire map(LDT Remove).
 *
 * @param self                  AerospikeLMap object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 * 
 * Returns an integer status. 0(Zero) is success value.
 * In case of error,appropriate exceptions will be raised.
 ********************************************************************************************************
 */
PyObject * AerospikeLMap_Destroy(AerospikeLMap * self, PyObject * args, PyObject * kwds)
{
    PyObject* py_policy = NULL;
    as_policy_apply apply_policy;
    as_policy_apply* apply_policy_p = NULL;

    as_error err;
    as_error_init(&err);

	static char * kwlist[] = {"policy", NULL};

	// Python Function Argument Parsing
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "|O:destroy", kwlist, 
			&py_policy) == false ) {
		return NULL;
	}

    if (!self || !self->client->as) {
        as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
        goto CLEANUP;
    }

    if (py_policy) {
        validate_policy_apply(&err, py_policy, &apply_policy);
    }
    if (err.code != AEROSPIKE_OK) {
        goto CLEANUP;
    }

	// Convert python policy object to as_policy_apply
	pyobject_to_policy_apply(&err, py_policy, &apply_policy, &apply_policy_p);
	if ( err.code != AEROSPIKE_OK ) {
		goto CLEANUP;
	}

    aerospike_lmap_destroy(self->client->as, &err, apply_policy_p, &self->key,
            &self->lmap);

CLEANUP:

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
        Py_DECREF(py_err);
		return NULL;
	}

	return PyLong_FromLong(0);
}

/**
 ********************************************************************************************************
 * Remove an object from the map.
 *
 * @param self                  AerospikeLMap object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 * 
 * Returns an integer status. 0(Zero) is success value.
 * In case of error,appropriate exceptions will be raised.
 ********************************************************************************************************
 */
PyObject * AerospikeLMap_Remove(AerospikeLMap * self, PyObject * args, PyObject * kwds)
{
    PyObject* py_map_key = NULL;
    PyObject* py_policy = NULL;
    as_policy_apply apply_policy;
    as_policy_apply* apply_policy_p = NULL;

    as_error err;
    as_error_init(&err);

	static char * kwlist[] = {"key", "policy", NULL};

	// Python Function Argument Parsing
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "O|O:remove", kwlist, 
			&py_map_key, &py_policy) == false ) {
		return NULL;
	}

    if (!self || !self->client->as) {
        as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
        goto CLEANUP;
    }

    if (py_policy) {
        validate_policy_apply(&err, py_policy, &apply_policy);
    }
    if (err.code != AEROSPIKE_OK) {
        goto CLEANUP;
    }

	// Convert python policy object to as_policy_apply
	pyobject_to_policy_apply(&err, py_policy, &apply_policy, &apply_policy_p);
	if ( err.code != AEROSPIKE_OK ) {
		goto CLEANUP;
	}

    as_val * map_key = NULL;
    pyobject_to_val(&err, py_map_key, &map_key);
    if (err.code != AEROSPIKE_OK) {
        goto CLEANUP;
    }

    aerospike_lmap_remove(self->client->as, &err, apply_policy_p, &self->key,
            &self->lmap, map_key);

CLEANUP:

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
        Py_DECREF(py_err);
		return NULL;
	}
	return PyLong_FromLong(0);
}

/**
 ********************************************************************************************************
 * Get the current item count of the map.
 *
 * @param self                  AerospikeLMap object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 * 
 * Returns the size of map.
 * In case of error,appropriate exceptions will be raised.
 ********************************************************************************************************
 */
PyObject * AerospikeLMap_Size(AerospikeLMap * self, PyObject * args, PyObject * kwds)
{
    long size = 0;
    PyObject* py_policy = NULL;
    as_policy_apply apply_policy;
    as_policy_apply* apply_policy_p = NULL;

    as_error err;
    as_error_init(&err);

	static char * kwlist[] = {"policy", NULL};

	// Python Function Argument Parsing
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "|O:size", kwlist, 
			&py_policy) == false ) {
		return NULL;
	}

    if (!self || !self->client->as) {
        as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
        goto CLEANUP;
    }

    if (py_policy) {
        validate_policy_apply(&err, py_policy, &apply_policy);
    }
    if (err.code != AEROSPIKE_OK) {
        goto CLEANUP;
    }

	// Convert python policy object to as_policy_apply
	pyobject_to_policy_apply(&err, py_policy, &apply_policy, &apply_policy_p);
	if ( err.code != AEROSPIKE_OK ) {
		goto CLEANUP;
	}

    aerospike_lmap_size(self->client->as, &err, apply_policy_p, &self->key,
            &self->lmap, &size);

CLEANUP:

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
        Py_DECREF(py_err);
		return NULL;
	}
    return PyLong_FromLong(size);
}

/**
 ********************************************************************************************************
 * Get the configuration parameters of the map.
 *
 * @param self                  AerospikeLMap object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 * 
 * Returns the configuration parameters of the map.
 * In case of error,appropriate exceptions will be raised.
 ********************************************************************************************************
 */
PyObject * AerospikeLMap_Config(AerospikeLMap * self, PyObject * args, PyObject * kwds)
{
    /*
     * To be implemented.
     */
}
