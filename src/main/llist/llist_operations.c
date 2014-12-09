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

/**
 ********************************************************************************************************
 * Add an object to the list.
 *
 * @param self                  AerospikeLList object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 * 
 * Returns an integer status. 0(Zero) is success value.
 * In case of error,appropriate exceptions will be raised.
 ********************************************************************************************************
 */
PyObject * AerospikeLList_Add(AerospikeLList * self, PyObject * args, PyObject * kwds)
{
    PyObject* py_value = NULL;
    PyObject* py_policy = NULL;
    as_policy_apply apply_policy;
    as_policy_apply* apply_policy_p = NULL;

    as_error err;
    as_error_init(&err);

	static char * kwlist[] = {"value", "policy", NULL};

	// Python Function Argument Parsing
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "O|O:add", kwlist, 
			&py_value, &py_policy) == false ) {
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

    as_val * val = NULL;
    pyobject_to_val(&err, py_value, &val);
    if (err.code != AEROSPIKE_OK) {
        goto CLEANUP;
    }

    aerospike_llist_add(self->client->as, &err, apply_policy_p, &self->key,
            &self->llist, val);

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
 * Add a list of objects to the list.
 *
 * @param self                  AerospikeLList object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 * 
 * Returns an integer status. 0(Zero) is success value.
 * In case of error,appropriate exceptions will be raised.
 ********************************************************************************************************
 */
PyObject * AerospikeLList_Add_All(AerospikeLList * self, PyObject * args, PyObject * kwds)
{
    PyObject* py_arglist = NULL;
    PyObject* py_policy = NULL;
    as_policy_apply apply_policy;
    as_policy_apply* apply_policy_p = NULL;

    as_error err;
    as_error_init(&err);

	static char * kwlist[] = {"values", "policy", NULL};

	// Python Function Argument Parsing
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "O|O:add_all", kwlist, 
			&py_arglist, &py_policy)== false ) {
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

    aerospike_llist_add_all(self->client->as, &err, apply_policy_p,
            &self->key, &self->llist, arglist);

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
 * Get an object from the list.
 *
 * @param self                  AerospikeLList object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 * 
 * Returns an object from the list.
 * In case of error,appropriate exceptions will be raised.
 ********************************************************************************************************
 */
PyObject * AerospikeLList_Get(AerospikeLList * self, PyObject * args, PyObject * kwds)
{
    PyObject* py_value = NULL;
    PyObject* py_policy = NULL;
    as_policy_apply apply_policy;
    as_policy_apply* apply_policy_p = NULL;

    as_error err;
    as_error_init(&err);

	static char * kwlist[] = {"value", "policy", NULL};

	// Python Function Argument Parsing
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "O|O:get", kwlist, 
			&py_value, &py_policy) == false ) {
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

    as_val * val = NULL;
    pyobject_to_val(&err, py_value, &val);
    if (err.code != AEROSPIKE_OK) {
        goto CLEANUP;
    }

    as_list* list_p = NULL;
    aerospike_llist_find(self->client->as, &err, apply_policy_p, &self->key,
            &self->llist, val, &list_p);

    if (err.code != AEROSPIKE_OK) {
        goto CLEANUP;
    }

    PyObject * py_list = NULL;
    list_to_pyobject(&err, list_p, &py_list);

CLEANUP:

    if (list_p) {
        as_list_destroy(list_p);
    }

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
        Py_DECREF(py_err);
		return NULL;
	}
    return py_list;
}

/**
 ********************************************************************************************************
 * Scan the list and apply a predicate filter.
 *
 * @param self                  AerospikeLList object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 * 
 * Returns a list of elements from the list after applying predicate.
 * In case of error,appropriate exceptions will be raised.
 ********************************************************************************************************
 */
PyObject * AerospikeLList_Filter(AerospikeLList * self, PyObject * args, PyObject * kwds)
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

    as_list* elements_list = NULL;
    aerospike_llist_filter(self->client->as, &err, apply_policy_p, &self->key,
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
        Py_DECREF(py_err);
		return NULL;
	}
	return py_list;
}

/**
 ********************************************************************************************************
 * Delete the entire list(LDT Remove).
 *
 * @param self                  AerospikeLList object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 * 
 * Returns an integer status. 0(Zero) is success value.
 * In case of error,appropriate exceptions will be raised.
 ********************************************************************************************************
 */
PyObject * AerospikeLList_Destroy(AerospikeLList * self, PyObject * args, PyObject * kwds)
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

    aerospike_llist_destroy(self->client->as, &err, apply_policy_p, &self->key,
            &self->llist);

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
 * Remove an object from the list.
 *
 * @param self                  AerospikeLList object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 * 
 * Returns an integer status. 0(Zero) is success value.
 * In case of error,appropriate exceptions will be raised.
 ********************************************************************************************************
 */
PyObject * AerospikeLList_Remove(AerospikeLList * self, PyObject * args, PyObject * kwds)
{
    PyObject* py_value = NULL;
    PyObject* py_policy = NULL;
    as_policy_apply apply_policy;
    as_policy_apply* apply_policy_p = NULL;

    as_error err;
    as_error_init(&err);

	static char * kwlist[] = {"element", "policy", NULL};

	// Python Function Argument Parsing
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "O|O:remove", kwlist, 
			&py_value, &py_policy) == false ) {
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

    as_val * val = NULL;
    pyobject_to_val(&err, py_value, &val);
    if (err.code != AEROSPIKE_OK) {
        goto CLEANUP;
    }

    aerospike_llist_remove(self->client->as, &err, apply_policy_p, &self->key,
            &self->llist, val);

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
 * Get the current item count of the list.
 *
 * @param self                  AerospikeLList object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 * 
 * Returns the size of list.
 * In case of error,appropriate exceptions will be raised.
 ********************************************************************************************************
 */
PyObject * AerospikeLList_Size(AerospikeLList * self, PyObject * args, PyObject * kwds)
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

    aerospike_llist_size(self->client->as, &err, apply_policy_p, &self->key,
            &self->llist, &size);

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
 * Get the configuration parameters of the list.
 *
 * @param self                  AerospikeLList object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 * 
 * Returns the configuration parameters of the list.
 * In case of error,appropriate exceptions will be raised.
 ********************************************************************************************************
 */
PyObject * AerospikeLList_Config(AerospikeLList * self, PyObject * args, PyObject * kwds)
{
    /*
     * To be implemented.
     */
}
