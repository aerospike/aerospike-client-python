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

#include <Python.h>
#include <stdbool.h>

#include <aerospike/aerospike_lstack.h>
#include <aerospike/as_error.h>
#include <aerospike/as_ldt.h>

#include "client.h"
#include "conversions.h"
#include "exceptions.h"
#include "lstack.h"
#include "policy.h"

/**
 ********************************************************************************************************
 * Push new object onto the stack.
 *
 * @param self                  AerospikeLStack object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 * 
 * Returns an integer status. 0(Zero) is success value.
 * In case of error,appropriate exceptions will be raised.
 ********************************************************************************************************
 */

PyObject * AerospikeLStack_Push(AerospikeLStack * self, PyObject * args, PyObject * kwds)
{
	PyObject* py_value = NULL;
	PyObject* py_policy = NULL;

	as_policy_apply apply_policy;
	as_policy_apply* apply_policy_p = NULL;
	as_val * val = NULL;

	as_static_pool static_pool;
	memset(&static_pool, 0, sizeof(static_pool));

	//Error Initialization
	as_error err;
	as_error_init(&err);

	static char * kwlist[] = {"value", "policy", NULL};

	// Python Function Argument Parsing
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "O|O:push", kwlist, 
				&py_value, &py_policy) == false ) {
		return NULL;
	}

	if (!self || !self->client || !self->client->as) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
		goto CLEANUP;
	}
	if (!self->client->is_conn_16) {
		as_error_update(&err, AEROSPIKE_ERR_CLUSTER, "No connection to aerospike cluster");
		goto CLEANUP;
	}

	// Convert python policy object to as_policy_apply
	pyobject_to_policy_apply(&err, py_policy, &apply_policy, &apply_policy_p,
			&self->client->as->config.policies.apply);
	if ( err.code != AEROSPIKE_OK ) {
		goto CLEANUP;
	}

	pyobject_to_val(self->client, &err, py_value, &val, &static_pool, SERIALIZER_PYTHON);
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	aerospike_lstack_push(self->client->as, &err, apply_policy_p, &self->key,
			&self->lstack, val);
	if(err.code != AEROSPIKE_OK) {
		as_error_update(&err, err.code, NULL);
	}

CLEANUP:

	if (val) {
		as_val_destroy(val);
	}

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL, *py_key = NULL;
		PyObject *exception_type = raise_exception(&err);
		error_to_pyobject(&err, &py_err);
		if(PyObject_HasAttrString(exception_type, "key")) {
			key_to_pyobject(&err, &self->key, &py_key);
			PyObject_SetAttrString(exception_type, "key", py_key);
			Py_DECREF(py_key);
		} 
		if(PyObject_HasAttrString(exception_type, "bin")) {
			PyObject *py_bins = PyString_FromString((char *)&self->bin_name);
			PyObject_SetAttrString(exception_type, "bin", py_bins);
			Py_DECREF(py_bins);
		}
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return NULL;
	}
	return PyLong_FromLong(0);
}

/**
 ********************************************************************************************************
 * Push a list of objects on the stack.
 *
 * @param self                  AerospikeLStack object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 * 
 * Returns an integer status. 0(Zero) is success value.
 * In case of error,appropriate exceptions will be raised.
 ********************************************************************************************************
 */
PyObject * AerospikeLStack_Push_Many(AerospikeLStack * self, PyObject * args, PyObject * kwds)
{
	PyObject* py_arglist = NULL;
	PyObject* py_policy = NULL;

	as_error err;
	as_error_init(&err);

	as_policy_apply apply_policy;
	as_policy_apply* apply_policy_p = NULL;
	as_list* arglist = NULL;

	as_static_pool static_pool;
	memset(&static_pool, 0, sizeof(static_pool));

	static char * kwlist[] = {"values", "policy", NULL};

	// Python Function Argument Parsing
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "O|O:push_many", kwlist, 
				&py_arglist, &py_policy)== false ) {
		return NULL;
	}

	if (!self || !self->client || !self->client->as) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
		goto CLEANUP;
	}
	if (!self->client->is_conn_16) {
		as_error_update(&err, AEROSPIKE_ERR_CLUSTER, "No connection to aerospike cluster");
		goto CLEANUP;
	}

	// Convert python policy object to as_policy_apply
	pyobject_to_policy_apply(&err, py_policy, &apply_policy, &apply_policy_p,
			&self->client->as->config.policies.apply);
	if ( err.code != AEROSPIKE_OK ) {
		goto CLEANUP;
	}

	/*
	 * Convert python list to as list 
	 */
	if ( !PyList_Check(py_arglist)) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid argument(type)");
		goto CLEANUP;
	}

	pyobject_to_list(self->client, &err, py_arglist, &arglist, &static_pool, SERIALIZER_PYTHON);
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	aerospike_lstack_push_all(self->client->as, &err, apply_policy_p,
			&self->key, &self->lstack, arglist);
	if(err.code != AEROSPIKE_OK) {
		as_error_update(&err, err.code, NULL);
	}

CLEANUP:

	if (arglist) {
		as_list_destroy(arglist);
	}

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL, *py_key = NULL;
		PyObject *exception_type = raise_exception(&err);
		error_to_pyobject(&err, &py_err);
		if(PyObject_HasAttrString(exception_type, "key")) {
			key_to_pyobject(&err, &self->key, &py_key);
			PyObject_SetAttrString(exception_type, "key", py_key);
			Py_DECREF(py_key);
		} 
		if(PyObject_HasAttrString(exception_type, "bin")) {
			PyObject * py_bins = PyString_FromString((char *)&self->bin_name);
			PyObject_SetAttrString(exception_type, "bin", py_bins);
			Py_DECREF(py_bins);
		}
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return NULL;
	}
	return PyLong_FromLong(0);
}

/**
 ********************************************************************************************************
 * Fetch the top N elements from the stack.
 *
 * @param self                  AerospikeLStack object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 * 
 * Returns a list of elements.
 * In case of error,appropriate exceptions will be raised.
 ********************************************************************************************************
 */
PyObject * AerospikeLStack_Peek(AerospikeLStack * self, PyObject * args, PyObject * kwds)
{
	long peek_count = 0;
	PyObject* py_policy = NULL;

	as_policy_apply apply_policy;
	as_policy_apply* apply_policy_p = NULL;
	as_list* list = NULL;

	as_error err;
	as_error_init(&err);

	static char * kwlist[] = {"peek_count", "policy", NULL};

	// Python Function Argument Parsing
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "l|O:peek", kwlist, 
				&peek_count, &py_policy) == false ) {
		return NULL;
	}

	if (!self || !self->client || !self->client->as) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
		goto CLEANUP;
	}
	if (!self->client->is_conn_16) {
		as_error_update(&err, AEROSPIKE_ERR_CLUSTER, "No connection to aerospike cluster");
		goto CLEANUP;
	}

	// Convert python policy object to as_policy_apply
	pyobject_to_policy_apply(&err, py_policy, &apply_policy, &apply_policy_p,
			&self->client->as->config.policies.apply);
	if ( err.code != AEROSPIKE_OK ) {
		goto CLEANUP;
	}

	/*
	 * Peek values from stack
	 */
	aerospike_lstack_peek(self->client->as, &err, apply_policy_p, &self->key,
			&self->lstack, peek_count, &list);
	if (err.code != AEROSPIKE_OK) {
		as_error_update(&err, err.code, NULL);
		goto CLEANUP;
	}

	PyObject* py_list = NULL;
	list_to_pyobject(self->client, &err, list, &py_list);

CLEANUP:

	if (list) {
		as_list_destroy(list);
	}

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL, *py_key = NULL;
		PyObject *exception_type = raise_exception(&err);
		error_to_pyobject(&err, &py_err);
		if(PyObject_HasAttrString(exception_type, "key")) {
			key_to_pyobject(&err, &self->key, &py_key);
			PyObject_SetAttrString(exception_type, "key", py_key);
			Py_DECREF(py_key);
		} 
		if(PyObject_HasAttrString(exception_type, "bin")) {
			PyObject *py_bins = PyString_FromString((char *)&self->bin_name);
			PyObject_SetAttrString(exception_type, "bin", py_bins);
			Py_DECREF(py_bins);
		}
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return NULL;
	}
	return py_list;
}

/**
 ********************************************************************************************************
 * Scan the stack and apply a predicate filter.
 *
 * @param self                  AerospikeLStack object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 * 
 * Returns a list of elements(peek_count) from the stack after applying predicate.
 * In case of error, Appropriate exceptions will be raised.
 ********************************************************************************************************
 */
PyObject * AerospikeLStack_Filter(AerospikeLStack * self, PyObject * args, PyObject * kwds)
{
	long peek_count = 0;
	char* filter_name = NULL;
	PyObject * py_args = NULL; 
	PyObject* py_policy = NULL;

	as_policy_apply apply_policy;
	as_policy_apply* apply_policy_p = NULL;
	as_list* arg_list = NULL;
	as_list* elements_list = NULL;

	as_static_pool static_pool;
	memset(&static_pool, 0, sizeof(static_pool));

	as_error err;
	as_error_init(&err);

	static char * kwlist[] = {"peek_count", "udf_function_name", "args", "policy", NULL};

	// Python Function Argument Parsing
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "|lsOO:filter", kwlist, 
				&peek_count, &filter_name, &py_args, &py_policy) == false ) {
		return NULL;
	}

	if (!self || !self->client || !self->client->as) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
		goto CLEANUP;
	}
	if (!self->client->is_conn_16) {
		as_error_update(&err, AEROSPIKE_ERR_CLUSTER, "No connection to aerospike cluster");
		goto CLEANUP;
	}

	// Convert python policy object to as_policy_apply
	pyobject_to_policy_apply(&err, py_policy, &apply_policy, &apply_policy_p,
			&self->client->as->config.policies.apply);
	if ( err.code != AEROSPIKE_OK ) {
		goto CLEANUP;
	}

	if ( py_args && !filter_name) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Filter arguments without filter name");
		goto CLEANUP;
	}

	if ( py_args && !PyList_Check(py_args)) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid filter argument(type)");
		goto CLEANUP;
	}

	if (py_args) {
		pyobject_to_list(self->client, &err, py_args, &arg_list, &static_pool, SERIALIZER_PYTHON);
	}

	aerospike_lstack_filter(self->client->as, &err, apply_policy_p, &self->key,
			&self->lstack, peek_count, filter_name, arg_list, &elements_list);

	if (err.code != AEROSPIKE_OK) {
		as_error_update(&err, err.code, NULL);
		goto CLEANUP;
	}

	PyObject* py_list = NULL;
	list_to_pyobject(self->client, &err, elements_list, &py_list);

CLEANUP:

	if (elements_list) {
		as_list_destroy(elements_list);
	}

	if (arg_list) {
		as_list_destroy(arg_list);
	}

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL, *py_key = NULL;
		PyObject *exception_type = raise_exception(&err);
		error_to_pyobject(&err, &py_err);
		if(PyObject_HasAttrString(exception_type, "key")) {
			key_to_pyobject(&err, &self->key, &py_key);
			PyObject_SetAttrString(exception_type, "key", py_key);
			Py_DECREF(py_key);
		} 
		if(PyObject_HasAttrString(exception_type, "bin")) {
			PyObject *py_bins = PyString_FromString((char *)&self->bin_name);
			PyObject_SetAttrString(exception_type, "bin", py_bins);
			Py_DECREF(py_bins);
		}
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return NULL;
	}
	return py_list;
}

/**
 ********************************************************************************************************
 * Delete the entire stack(LDT Remove).
 *
 * @param self                  AerospikeLStack object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 * 
 * Returns an integer status. 0(Zero) is success value.
 * In case of error,appropriate exceptions will be raised.
 ********************************************************************************************************
 */
PyObject * AerospikeLStack_Destroy(AerospikeLStack * self, PyObject * args, PyObject * kwds)
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

	if (!self || !self->client || !self->client->as) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
		goto CLEANUP;
	}
	if (!self->client->is_conn_16) {
		as_error_update(&err, AEROSPIKE_ERR_CLUSTER, "No connection to aerospike cluster");
		goto CLEANUP;
	}

	// Convert python policy object to as_policy_apply
	pyobject_to_policy_apply(&err, py_policy, &apply_policy, &apply_policy_p,
			&self->client->as->config.policies.apply);
	if ( err.code != AEROSPIKE_OK ) {
		as_error_update(&err, err.code, NULL);
		goto CLEANUP;
	}

	aerospike_lstack_destroy(self->client->as, &err, apply_policy_p, &self->key,
			&self->lstack);

CLEANUP:

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL, *py_key = NULL;
		PyObject *exception_type = raise_exception(&err);
		error_to_pyobject(&err, &py_err);
		if(PyObject_HasAttrString(exception_type, "key")) {
			key_to_pyobject(&err, &self->key, &py_key);
			PyObject_SetAttrString(exception_type, "key", py_key);
			Py_DECREF(py_key);
		} 
		if(PyObject_HasAttrString(exception_type, "bin")) {
			PyObject * py_bins = PyString_FromString((char *)&self->bin_name);
			PyObject_SetAttrString(exception_type, "bin", py_bins);
			Py_DECREF(py_bins);
		}
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return NULL;
	}
	return PyLong_FromLong(0);
}

/**
 ********************************************************************************************************
 * Get the current capacity limit setting.
 *
 * @param self                  AerospikeLStack object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 * 
 * Returns current capacity of the stack.
 * In case of error,appropriate exceptions will be raised.
 ********************************************************************************************************
 */
PyObject * AerospikeLStack_Get_Capacity(AerospikeLStack * self, PyObject * args, PyObject * kwds)
{
	long capacity = 0;
	PyObject* py_policy = NULL;
	as_policy_apply apply_policy;
	as_policy_apply* apply_policy_p = NULL;

	as_error err;
	as_error_init(&err);

	static char * kwlist[] = {"policy", NULL};

	// Python Function Argument Parsing
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "|O:get_capacity", kwlist, 
				&py_policy) == false ) {
		return NULL;
	}

	if (!self || !self->client || !self->client->as) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
		goto CLEANUP;
	}
	if (!self->client->is_conn_16) {
		as_error_update(&err, AEROSPIKE_ERR_CLUSTER, "No connection to aerospike cluster");
		goto CLEANUP;
	}

	// Convert python policy object to as_policy_apply
	pyobject_to_policy_apply(&err, py_policy, &apply_policy, &apply_policy_p,
			&self->client->as->config.policies.apply);
	if ( err.code != AEROSPIKE_OK ) {
		as_error_update(&err, err.code, NULL);
		goto CLEANUP;
	}

	aerospike_lstack_get_capacity(self->client->as, &err, apply_policy_p, &self->key,
			&self->lstack, (uint32_t *)&capacity);

CLEANUP:

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL, *py_key = NULL;
		PyObject *exception_type = raise_exception(&err);
		error_to_pyobject(&err, &py_err);
		if(PyObject_HasAttrString(exception_type, "key")) {
			key_to_pyobject(&err, &self->key, &py_key);
			PyObject_SetAttrString(exception_type, "key", py_key);
			Py_DECREF(py_key);
		} 
		if(PyObject_HasAttrString(exception_type, "bin")) {
			PyObject *py_bins = PyString_FromString((char *)&self->bin_name);
			PyObject_SetAttrString(exception_type, "bin", py_bins);
			Py_DECREF(py_bins);
		}
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return NULL;
	}

	return PyLong_FromLong(capacity);
}

/**
 ********************************************************************************************************
 * Set the maximum capacity for the stack.
 *
 * @param self                  AerospikeLStack object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 * 
 * Returns an integer status. 0(Zero) is success value.
 * In case of error,appropriate exceptions will be raised.
 ********************************************************************************************************
 */
PyObject * AerospikeLStack_Set_Capacity(AerospikeLStack * self, PyObject * args, PyObject * kwds)
{
	long capacity = 0;
	PyObject* py_policy = NULL;
	as_policy_apply apply_policy;
	as_policy_apply* apply_policy_p = NULL;

	as_error err;
	as_error_init(&err);

	static char * kwlist[] = {"capacity", "policy", NULL};

	// Python Function Argument Parsing
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "l|O:set_capacity", kwlist, 
				&capacity, &py_policy) == false ) {
		return NULL;
	}

	if (!self || !self->client || !self->client->as) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
		goto CLEANUP;
	}
	if (!self->client->is_conn_16) {
		as_error_update(&err, AEROSPIKE_ERR_CLUSTER, "No connection to aerospike cluster");
		goto CLEANUP;
	}

	// Convert python policy object to as_policy_apply
	pyobject_to_policy_apply(&err, py_policy, &apply_policy, &apply_policy_p,
			&self->client->as->config.policies.apply);
	if ( err.code != AEROSPIKE_OK ) {
		goto CLEANUP;
	}

	aerospike_lstack_set_capacity(self->client->as, &err, apply_policy_p, &self->key,
			&self->lstack, capacity);
	if( err.code != AEROSPIKE_OK ) {
		as_error_update(&err, err.code, NULL);
	}

CLEANUP:

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL, *py_key = NULL;
		PyObject *exception_type = raise_exception(&err);
		error_to_pyobject(&err, &py_err);
		if(PyObject_HasAttrString(exception_type, "key")) {
			key_to_pyobject(&err, &self->key, &py_key);
			PyObject_SetAttrString(exception_type, "key", py_key);
			Py_DECREF(py_key);
		} 
		if(PyObject_HasAttrString(exception_type, "bin")) {
			PyObject * py_bins = PyString_FromString((char *)&self->bin_name);
			PyObject_SetAttrString(exception_type, "bin", py_bins);
			Py_DECREF(py_bins);
		}
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return NULL;
	}

	return PyLong_FromLong(0);
}

/**
 ********************************************************************************************************
 * Get the current item count of the stack.
 *
 * @param self                  AerospikeLStack object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 * 
 * Returns the size of stack.
 * In case of error,appropriate exceptions will be raised.
 ********************************************************************************************************
 */
PyObject * AerospikeLStack_Size(AerospikeLStack * self, PyObject * args, PyObject * kwds)
{
	uint32_t size = 0;
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

	if (!self || !self->client || !self->client->as) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
		goto CLEANUP;
	}
	if (!self->client->is_conn_16) {
		as_error_update(&err, AEROSPIKE_ERR_CLUSTER, "No connection to aerospike cluster");
		goto CLEANUP;
	}

	// Convert python policy object to as_policy_apply
	pyobject_to_policy_apply(&err, py_policy, &apply_policy, &apply_policy_p,
			&self->client->as->config.policies.apply);
	if ( err.code != AEROSPIKE_OK ) {
		goto CLEANUP;
	}

	aerospike_lstack_size(self->client->as, &err, apply_policy_p, &self->key,
			&self->lstack, &size);
	if( err.code != AEROSPIKE_OK ) {
		as_error_update(&err, err.code, NULL);
	}

CLEANUP:

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL, *py_key = NULL;
		PyObject *exception_type = raise_exception(&err);
		error_to_pyobject(&err, &py_err);
		if(PyObject_HasAttrString(exception_type, "key")) {
			key_to_pyobject(&err, &self->key, &py_key);
			PyObject_SetAttrString(exception_type, "key", py_key);
			Py_DECREF(py_key);
		} 
		if(PyObject_HasAttrString(exception_type, "bin")) {
			PyObject *py_bins = PyString_FromString((char *)&self->bin_name);
			PyObject_SetAttrString(exception_type, "bin", py_bins);
			Py_DECREF(py_bins);
		}
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return NULL;
	}

	return PyLong_FromLong(size);
}

/**
 ********************************************************************************************************
 * Get the configuration parameters of the stack.
 *
 * @param self                  AerospikeLStack object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 * 
 * Returns the configuration parameters of the stack.
 * In case of error,appropriate exceptions will be raised.
 ********************************************************************************************************
 */
PyObject * AerospikeLStack_Config(AerospikeLStack * self, PyObject * args, PyObject * kwds)
{
	/*
	 * To be implemented
	 * CSDK API is not present.
	 */
	return PyLong_FromLong(0);
}
