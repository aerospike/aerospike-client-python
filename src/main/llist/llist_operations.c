/*******************************************************************************
 * Copyright 2013-2016 Aerospike, Inc.
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
#include "exceptions.h"
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
	as_val * val = NULL;

	as_static_pool static_pool;
	memset(&static_pool, 0, sizeof(static_pool));

	as_error err;
	as_error_init(&err);

	static char * kwlist[] = {"element", "policy", NULL};

	// Python Function Argument Parsing
	if (PyArg_ParseTupleAndKeywords(args, kwds, "O|O:add", kwlist,
				&py_value, &py_policy) == false) {
		return NULL;
	}

	if (!self || !self->client->as) {
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
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	pyobject_to_val(self->client, &err, py_value, &val, &static_pool, SERIALIZER_PYTHON);
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	Py_BEGIN_ALLOW_THREADS
	aerospike_llist_add(self->client->as, &err, apply_policy_p, &self->key,
			&self->llist, val);
	Py_END_ALLOW_THREADS

	if (err.code != AEROSPIKE_OK) {
		as_error_update(&err, err.code, NULL);
	}

CLEANUP:

	if (val) {
		as_val_destroy(val);
	}

	if (err.code != AEROSPIKE_OK) {
		PyObject * py_err = NULL, *py_key = NULL;
		PyObject *exception_type = raise_exception(&err);
		error_to_pyobject(&err, &py_err);
		if (PyObject_HasAttrString(exception_type, "key")) {
			key_to_pyobject(&err, &self->key, &py_key);
			PyObject_SetAttrString(exception_type, "key", py_key);
			Py_DECREF(py_key);
		} 
		if (PyObject_HasAttrString(exception_type, "bin")) {
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
PyObject * AerospikeLList_Add_Many(AerospikeLList * self, PyObject * args, PyObject * kwds)
{
	PyObject* py_arglist = NULL;
	PyObject* py_policy = NULL;
	as_policy_apply apply_policy;
	as_policy_apply* apply_policy_p = NULL;
	as_list* arglist = NULL;

	as_static_pool static_pool;
	memset(&static_pool, 0, sizeof(static_pool));

	as_error err;
	as_error_init(&err);

	static char * kwlist[] = {"elements", "policy", NULL};

	// Python Function Argument Parsing
	if (PyArg_ParseTupleAndKeywords(args, kwds, "O|O:add_many", kwlist,
				&py_arglist, &py_policy)== false) {
		return NULL;
	}

	if (!self || !self->client->as) {
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
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	/*
	 * Convert python list to as list 
	 */
	if (!PyList_Check(py_arglist)) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid argument(type)");
		goto CLEANUP;
	}

	pyobject_to_list(self->client, &err, py_arglist, &arglist, &static_pool, SERIALIZER_PYTHON);
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	Py_BEGIN_ALLOW_THREADS
	aerospike_llist_add_all(self->client->as, &err, apply_policy_p,
			&self->key, &self->llist, arglist);
	Py_END_ALLOW_THREADS
	if (err.code != AEROSPIKE_OK) {
		as_error_update(&err, err.code, NULL);
	}

CLEANUP:

	if (arglist) {
		as_list_destroy(arglist);
	}

	if (err.code != AEROSPIKE_OK) {
		PyObject * py_err = NULL, *py_key = NULL;
		PyObject *exception_type = raise_exception(&err);
		error_to_pyobject(&err, &py_err);
		if (PyObject_HasAttrString(exception_type, "key")) {
			key_to_pyobject(&err, &self->key, &py_key);
			PyObject_SetAttrString(exception_type, "key", py_key);
			Py_DECREF(py_key);
		} 
		if (PyObject_HasAttrString(exception_type, "bin")) {
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
	as_val * val = NULL;
	as_list* list_p = NULL;

	as_static_pool static_pool;
	memset(&static_pool, 0, sizeof(static_pool));

	as_error err;
	as_error_init(&err);

	static char * kwlist[] = {"value", "policy", NULL};

	// Python Function Argument Parsing
	if (PyArg_ParseTupleAndKeywords(args, kwds, "O|O:get", kwlist,
				&py_value, &py_policy) == false) {
		return NULL;
	}

	if (!self || !self->client->as) {
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
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	pyobject_to_val(self->client, &err, py_value, &val, &static_pool, SERIALIZER_PYTHON);
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	Py_BEGIN_ALLOW_THREADS
	aerospike_llist_find(self->client->as, &err, apply_policy_p, &self->key,
			&self->llist, val, &list_p);
	Py_END_ALLOW_THREADS

	if (err.code != AEROSPIKE_OK) {
		as_error_update(&err, err.code, NULL);
		goto CLEANUP;
	}

	PyObject * py_list = NULL;
	list_to_pyobject(self->client, &err, list_p, &py_list);

CLEANUP:

	if (val) {
		as_val_destroy(val);
	}

	if (list_p) {
		as_list_destroy(list_p);
	}

	if (err.code != AEROSPIKE_OK) {
		PyObject * py_err = NULL, *py_key = NULL;
		PyObject *exception_type = raise_exception(&err);
		error_to_pyobject(&err, &py_err);
		if (PyObject_HasAttrString(exception_type, "key")) {
			key_to_pyobject(&err, &self->key, &py_key);
			PyObject_SetAttrString(exception_type, "key", py_key);
			Py_DECREF(py_key);
		} 
		if (PyObject_HasAttrString(exception_type, "bin")) {
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
	as_list* arg_list = NULL;
	as_list* elements_list = NULL;

	as_static_pool static_pool;
	memset(&static_pool, 0, sizeof(static_pool));

	as_error err;
	as_error_init(&err);

	static char * kwlist[] = {"function", "args", "policy", NULL};

	// Python Function Argument Parsing
	if (PyArg_ParseTupleAndKeywords(args, kwds, "|sOO:filter", kwlist,
				&filter_name, &py_args, &py_policy) == false) {
		return NULL;
	}

	if (!self || !self->client->as) {
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
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	if (py_args && !filter_name) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Filter arguments without filter name");
		goto CLEANUP;
	}

	if (py_args && !PyList_Check(py_args)) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid filter argument(type)");
		goto CLEANUP;
	}

	if (py_args) {
		pyobject_to_list(self->client, &err, py_args, &arg_list, &static_pool, SERIALIZER_PYTHON);
	}

	Py_BEGIN_ALLOW_THREADS
	aerospike_llist_filter(self->client->as, &err, apply_policy_p, &self->key,
			&self->llist, filter_name, arg_list, &elements_list);
	Py_END_ALLOW_THREADS

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

	if (err.code != AEROSPIKE_OK) {
		PyObject * py_err = NULL, *py_key = NULL;
		PyObject *exception_type = raise_exception(&err);
		error_to_pyobject(&err, &py_err);
		if (PyObject_HasAttrString(exception_type, "key")) {
			key_to_pyobject(&err, &self->key, &py_key);
			PyObject_SetAttrString(exception_type, "key", py_key);
			Py_DECREF(py_key);
		} 
		if (PyObject_HasAttrString(exception_type, "bin")) {
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
	if (PyArg_ParseTupleAndKeywords(args, kwds, "|O:destroy", kwlist,
				&py_policy) == false) {
		return NULL;
	}

	if (!self || !self->client->as) {
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
	if (err.code != AEROSPIKE_OK) {
		as_error_update(&err, err.code, NULL);
		goto CLEANUP;
	}

	Py_BEGIN_ALLOW_THREADS
	aerospike_llist_destroy(self->client->as, &err, apply_policy_p, &self->key,
			&self->llist);
	Py_END_ALLOW_THREADS

CLEANUP:

	if (err.code != AEROSPIKE_OK) {
		PyObject * py_err = NULL, *py_key = NULL;
		PyObject *exception_type = raise_exception(&err);
		error_to_pyobject(&err, &py_err);
		if (PyObject_HasAttrString(exception_type, "key")) {
			key_to_pyobject(&err, &self->key, &py_key);
			PyObject_SetAttrString(exception_type, "key", py_key);
			Py_DECREF(py_key);
		} 
		if (PyObject_HasAttrString(exception_type, "bin")) {
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
	as_val * val = NULL;

	as_static_pool static_pool;
	memset(&static_pool, 0, sizeof(static_pool));

	as_error err;
	as_error_init(&err);

	static char * kwlist[] = {"value", "policy", NULL};

	// Python Function Argument Parsing
	if (PyArg_ParseTupleAndKeywords(args, kwds, "O|O:remove", kwlist,
				&py_value, &py_policy) == false) {
		return NULL;
	}

	if (!self || !self->client->as) {
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
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	pyobject_to_val(self->client, &err, py_value, &val, &static_pool, SERIALIZER_PYTHON);
	if (err.code != AEROSPIKE_OK) {
		as_error_update(&err, err.code, NULL);
		goto CLEANUP;
	}

	Py_BEGIN_ALLOW_THREADS
	aerospike_llist_remove(self->client->as, &err, apply_policy_p, &self->key,
			&self->llist, val);
	Py_END_ALLOW_THREADS

CLEANUP:

	if (val) {
		as_val_destroy(val);
	}

	if (err.code != AEROSPIKE_OK) {
		PyObject * py_err = NULL, *py_key = NULL;
		PyObject *exception_type = raise_exception(&err);
		error_to_pyobject(&err, &py_err);
		if (PyObject_HasAttrString(exception_type, "key")) {
			key_to_pyobject(&err, &self->key, &py_key);
			PyObject_SetAttrString(exception_type, "key", py_key);
			Py_DECREF(py_key);
		} 
		if (PyObject_HasAttrString(exception_type, "bin")) {
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
	if (PyArg_ParseTupleAndKeywords(args, kwds, "|O:size", kwlist,
				&py_policy) == false) {
		return NULL;
	}

	if (!self || !self->client->as) {
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
	if (err.code != AEROSPIKE_OK) {
		as_error_update(&err, err.code, NULL);
		goto CLEANUP;
	}

	Py_BEGIN_ALLOW_THREADS
	aerospike_llist_size(self->client->as, &err, apply_policy_p, &self->key,
			&self->llist, (uint32_t *)&size);
	Py_END_ALLOW_THREADS

CLEANUP:

	if (err.code != AEROSPIKE_OK) {
		PyObject * py_err = NULL, *py_key = NULL;
		PyObject *exception_type = raise_exception(&err);
		error_to_pyobject(&err, &py_err);
		if (PyObject_HasAttrString(exception_type, "key")) {
			key_to_pyobject(&err, &self->key, &py_key);
			PyObject_SetAttrString(exception_type, "key", py_key);
			Py_DECREF(py_key);
		} 
		if (PyObject_HasAttrString(exception_type, "bin")) {
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
 * Select values from the beginning of list up to a maximum count.
 *
 * @param self                  AerospikeLList object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 * 
 * Returns a list of ldt contents.
 * In case of error,appropriate exceptions will be raised.
 ********************************************************************************************************
 */
PyObject * AerospikeLList_Find_First(AerospikeLList * self, PyObject * args, PyObject * kwds)
{
	PyObject* py_count = NULL;
	PyObject* py_policy = NULL;
	PyObject* py_elements_list = NULL;
	as_policy_apply apply_policy;
	as_policy_apply* apply_policy_p = NULL;
	as_list* elements_list = NULL;

	as_static_pool static_pool;
	memset(&static_pool, 0, sizeof(static_pool));

	as_error err;
	as_error_init(&err);

	static char * kwlist[] = {"count", "policy", NULL};

	// Python Function Argument Parsing
	if (PyArg_ParseTupleAndKeywords(args, kwds, "O|O:find_first", kwlist,
				&py_count, &py_policy)== false) {
		return NULL;
	}

	if (!self || !self->client->as) {
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
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	uint32_t count = 0;
	if (PyInt_Check(py_count)) {
		count = PyInt_AsLong(py_count);
	} else if (PyLong_Check(py_count)) {
		count = PyLong_AsLong(py_count);
	} else {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Count should be an integer or long");
		goto CLEANUP;
	}

	Py_BEGIN_ALLOW_THREADS
	aerospike_llist_find_first(self->client->as, &err, apply_policy_p, &self->key, &self->llist, count, &elements_list);
	Py_END_ALLOW_THREADS
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	list_to_pyobject(self->client, &err, elements_list, &py_elements_list);

CLEANUP:
	if (elements_list) {
		as_list_destroy(elements_list);
	}

	if (err.code != AEROSPIKE_OK) {
		PyObject * py_err = NULL, *py_key = NULL;
		PyObject *exception_type = raise_exception(&err);
		error_to_pyobject(&err, &py_err);
		if (PyObject_HasAttrString(exception_type, "key")) {
			key_to_pyobject(&err, &self->key, &py_key);
			PyObject_SetAttrString(exception_type, "key", py_key);
			Py_DECREF(py_key);
		} 
		if (PyObject_HasAttrString(exception_type, "bin")) {
			PyObject *py_bins = PyString_FromString((char *)&self->bin_name);
			PyObject_SetAttrString(exception_type, "bin", py_bins);
			Py_DECREF(py_bins);
		}
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return NULL;
	}

	return py_elements_list;
}

/**
 ********************************************************************************************************
 * Select values from the beginning of list up to a maximum count after applying
 * lua filter.
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
PyObject * AerospikeLList_Find_First_Filter(AerospikeLList * self, PyObject * args, PyObject * kwds)
{
	char* filter_name = NULL;
	PyObject * py_args = NULL; 
	PyObject* py_policy = NULL;
	PyObject *py_count = NULL;
	as_policy_apply apply_policy;
	as_policy_apply* apply_policy_p = NULL;
	as_list* arg_list = NULL;
	as_list* elements_list = NULL;

	as_static_pool static_pool;
	memset(&static_pool, 0, sizeof(static_pool));

	as_error err;
	as_error_init(&err);

	static char * kwlist[] = {"count", "function", "args", "policy", NULL};

	// Python Function Argument Parsing
	if (PyArg_ParseTupleAndKeywords(args, kwds, "OsO|O:find_first_filter", kwlist,
				&py_count, &filter_name, &py_args, &py_policy) == false) {
		return NULL;
	}

	if (!self || !self->client->as) {
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
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	if (py_args && !filter_name) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Filter arguments without filter name");
		goto CLEANUP;
	}

	if (py_args && !PyList_Check(py_args) && (py_args != Py_None)) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid filter argument(type)");
		goto CLEANUP;
	}

	if (PyList_Check(py_args)) {
		pyobject_to_list(self->client, &err, py_args, &arg_list, &static_pool, SERIALIZER_PYTHON);
	}

	uint32_t count = 0;
	if (PyInt_Check(py_count)) {
		count = PyInt_AsLong(py_count);
	} else if (PyLong_Check(py_count)) {
		count = PyLong_AsLong(py_count);
		if (count == (uint32_t)-1 && PyErr_Occurred()) {
			if (PyErr_ExceptionMatches(PyExc_OverflowError)) {
				as_error_update(&err, AEROSPIKE_ERR_PARAM, "integer value exceeds sys.maxsize");
				goto CLEANUP;
			}
		}
	} else {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Count should be an integer or long");
		goto CLEANUP;
	}

	Py_BEGIN_ALLOW_THREADS
	aerospike_llist_find_first_filter(self->client->as, &err, apply_policy_p, &self->key,
			&self->llist, count, filter_name, arg_list, &elements_list);
	Py_END_ALLOW_THREADS

	if (err.code != AEROSPIKE_OK) {
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

	if (err.code != AEROSPIKE_OK) {
		PyObject * py_err = NULL, *py_key = NULL;
		PyObject *exception_type = raise_exception(&err);
		error_to_pyobject(&err, &py_err);
		if (PyObject_HasAttrString(exception_type, "key")) {
			key_to_pyobject(&err, &self->key, &py_key);
			PyObject_SetAttrString(exception_type, "key", py_key);
			Py_DECREF(py_key);
		} 
		if (PyObject_HasAttrString(exception_type, "bin")) {
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
 * Select values from the end of list up to a maximum count.
 *
 * @param self                  AerospikeLList object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 * 
 * Returns a list of ldt contents.
 * In case of error,appropriate exceptions will be raised.
 ********************************************************************************************************
 */
PyObject * AerospikeLList_Find_Last(AerospikeLList * self, PyObject * args, PyObject * kwds)
{
	PyObject* py_count = NULL;
	PyObject* py_policy = NULL;
	PyObject* py_elements_list = NULL;
	as_policy_apply apply_policy;
	as_policy_apply* apply_policy_p = NULL;
	as_list* elements_list = NULL;

	as_static_pool static_pool;
	memset(&static_pool, 0, sizeof(static_pool));

	as_error err;
	as_error_init(&err);

	static char * kwlist[] = {"count", "policy", NULL};

	// Python Function Argument Parsing
	if (PyArg_ParseTupleAndKeywords(args, kwds, "O|O:find_last", kwlist,
				&py_count, &py_policy)== false) {
		return NULL;
	}

	if (!self || !self->client->as) {
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
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	uint32_t count = 0;
	if (PyInt_Check(py_count)) {
		count = PyInt_AsLong(py_count);
	} else if (PyLong_Check(py_count)) {
		count = PyLong_AsLong(py_count);
		if (count == (uint32_t)-1 && PyErr_Occurred()) {
			if (PyErr_ExceptionMatches(PyExc_OverflowError)) {
				as_error_update(&err, AEROSPIKE_ERR_PARAM, "integer value exceeds sys.maxsize");
				goto CLEANUP;
			}
		}
	} else {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Count should be an integer or long");
		goto CLEANUP;
	}

	Py_BEGIN_ALLOW_THREADS
	aerospike_llist_find_last(self->client->as, &err, apply_policy_p, &self->key, &self->llist, count, &elements_list);
	Py_END_ALLOW_THREADS
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	list_to_pyobject(self->client, &err, elements_list, &py_elements_list);

CLEANUP:
	if (elements_list) {
		as_list_destroy(elements_list);
	}

	if (err.code != AEROSPIKE_OK) {
		PyObject * py_err = NULL, *py_key = NULL;
		PyObject *exception_type = raise_exception(&err);
		error_to_pyobject(&err, &py_err);
		if (PyObject_HasAttrString(exception_type, "key")) {
			key_to_pyobject(&err, &self->key, &py_key);
			PyObject_SetAttrString(exception_type, "key", py_key);
			Py_DECREF(py_key);
		} 
		if (PyObject_HasAttrString(exception_type, "bin")) {
			PyObject *py_bins = PyString_FromString((char *)&self->bin_name);
			PyObject_SetAttrString(exception_type, "bin", py_bins);
			Py_DECREF(py_bins);
		}
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return NULL;
	}

	return py_elements_list;
}
/**
 ********************************************************************************************************
 * Select values from the end of list up to a maximum count after applying
 * lua filter.
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
PyObject * AerospikeLList_Find_Last_Filter(AerospikeLList * self, PyObject * args, PyObject * kwds)
{
	char* filter_name = NULL;
	PyObject * py_args = NULL; 
	PyObject* py_policy = NULL;
	PyObject *py_count = NULL;
	as_policy_apply apply_policy;
	as_policy_apply* apply_policy_p = NULL;
	as_list* arg_list = NULL;
	as_list* elements_list = NULL;

	as_static_pool static_pool;
	memset(&static_pool, 0, sizeof(static_pool));

	as_error err;
	as_error_init(&err);

	static char * kwlist[] = {"count", "function", "args", "policy", NULL};

	// Python Function Argument Parsing
	if (PyArg_ParseTupleAndKeywords(args, kwds, "OsO|O:find_last_filter", kwlist,
				&py_count, &filter_name, &py_args, &py_policy) == false) {
		return NULL;
	}

	if (!self || !self->client->as) {
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
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	if (py_args && !filter_name) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Filter arguments without filter name");
		goto CLEANUP;
	}

	if (py_args && !PyList_Check(py_args) && (py_args != Py_None)) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid filter argument(type)");
		goto CLEANUP;
	}

	if (PyList_Check(py_args)) {
		pyobject_to_list(self->client, &err, py_args, &arg_list, &static_pool, SERIALIZER_PYTHON);
	}

	uint32_t count = 0;
	if (PyInt_Check(py_count)) {
		count = PyInt_AsLong(py_count);
	} else if (PyLong_Check(py_count)) {
		count = PyLong_AsLong(py_count);
		if (count == (uint32_t)-1 && PyErr_Occurred()) {
			if (PyErr_ExceptionMatches(PyExc_OverflowError)) {
				as_error_update(&err, AEROSPIKE_ERR_PARAM, "integer value exceeds sys.maxsize");
				goto CLEANUP;
			}
		}
	} else {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Count should be an integer or long");
		goto CLEANUP;
	}

	Py_BEGIN_ALLOW_THREADS
	aerospike_llist_find_last_filter(self->client->as, &err, apply_policy_p, &self->key,
			&self->llist, count, filter_name, arg_list, &elements_list);
	Py_END_ALLOW_THREADS

	if (err.code != AEROSPIKE_OK) {
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

	if (err.code != AEROSPIKE_OK) {
		PyObject * py_err = NULL, *py_key = NULL;
		PyObject *exception_type = raise_exception(&err);
		error_to_pyobject(&err, &py_err);
		if (PyObject_HasAttrString(exception_type, "key")) {
			key_to_pyobject(&err, &self->key, &py_key);
			PyObject_SetAttrString(exception_type, "key", py_key);
			Py_DECREF(py_key);
		} 
		if (PyObject_HasAttrString(exception_type, "bin")) {
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
 * Select values from a begin key corresponding to a value up to a maximum count.
 *
 * @param self                  AerospikeLList object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 * 
 * Returns a list of ldt contents.
 * In case of error,appropriate exceptions will be raised.
 ********************************************************************************************************
 */
PyObject * AerospikeLList_Find_From(AerospikeLList * self, PyObject * args, PyObject * kwds)
{
	PyObject* py_count = NULL;
	PyObject* py_policy = NULL;
	PyObject* py_value = NULL;
	PyObject* py_elements_list = NULL;
	as_policy_apply apply_policy;
	as_policy_apply* apply_policy_p = NULL;
	as_list* elements_list = NULL;
	as_val* from_val = NULL;

	as_static_pool static_pool;
	memset(&static_pool, 0, sizeof(static_pool));

	as_error err;
	as_error_init(&err);

	static char * kwlist[] = {"value", "count", "policy", NULL};

	// Python Function Argument Parsing
	if (PyArg_ParseTupleAndKeywords(args, kwds, "OO|O:find_from", kwlist,
				&py_value, &py_count, &py_policy)== false) {
		return NULL;
	}

	if (!self || !self->client->as) {
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
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	uint32_t count = 0;
	if (PyInt_Check(py_count)) {
		count = PyInt_AsLong(py_count);
	} else if (PyLong_Check(py_count)) {
		count = PyLong_AsLong(py_count);
		if (count == (uint32_t)-1 && PyErr_Occurred()) {
			if (PyErr_ExceptionMatches(PyExc_OverflowError)) {
				as_error_update(&err, AEROSPIKE_ERR_PARAM, "integer value exceeds sys.maxsize");
				goto CLEANUP;
			}
		}
	} else {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Count should be an integer or long");
		goto CLEANUP;
	}

	pyobject_to_val(self->client, &err, py_value, &from_val, &static_pool, SERIALIZER_PYTHON);

	Py_BEGIN_ALLOW_THREADS
	aerospike_llist_find_from(self->client->as, &err, apply_policy_p, &self->key, &self->llist, from_val, count, &elements_list);
	Py_END_ALLOW_THREADS
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	list_to_pyobject(self->client, &err, elements_list, &py_elements_list);

CLEANUP:
	if (elements_list) {
		as_list_destroy(elements_list);
	}

	if (from_val) {
		as_val_destroy(from_val);
	}

	if (err.code != AEROSPIKE_OK) {
		PyObject * py_err = NULL, *py_key = NULL;
		PyObject *exception_type = raise_exception(&err);
		error_to_pyobject(&err, &py_err);
		if (PyObject_HasAttrString(exception_type, "key")) {
			key_to_pyobject(&err, &self->key, &py_key);
			PyObject_SetAttrString(exception_type, "key", py_key);
			Py_DECREF(py_key);
		} 
		if (PyObject_HasAttrString(exception_type, "bin")) {
			PyObject *py_bins = PyString_FromString((char *)&self->bin_name);
			PyObject_SetAttrString(exception_type, "bin", py_bins);
			Py_DECREF(py_bins);
		}
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return NULL;
	}

	return py_elements_list;
}
/**
 ********************************************************************************************************
 * Select values from a begin key corresponding to a value up to a maximum count applying a lua filter.
 *
 * @param self                  AerospikeLList object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 * 
 * Returns a list of ldt contents.
 * In case of error,appropriate exceptions will be raised.
 ********************************************************************************************************
 */
PyObject * AerospikeLList_Find_From_Filter(AerospikeLList * self, PyObject * args, PyObject * kwds)
{
	char* filter_name = NULL;
	PyObject* py_count = NULL;
	PyObject * py_args = NULL; 
	PyObject* py_policy = NULL;
	PyObject* py_value = NULL;
	PyObject* py_elements_list = NULL;
	as_policy_apply apply_policy;
	as_policy_apply* apply_policy_p = NULL;
	as_list* arg_list = NULL;
	as_list* elements_list = NULL;
	as_val* from_val = NULL;

	as_static_pool static_pool;
	memset(&static_pool, 0, sizeof(static_pool));

	as_error err;
	as_error_init(&err);

	static char * kwlist[] = {"value", "count", "function", "policy", NULL};

	// Python Function Argument Parsing
	if (PyArg_ParseTupleAndKeywords(args, kwds, "OOsO|O:find_from_filter", kwlist,
				&py_value, &py_count, &filter_name, &py_args, &py_policy)== false) {
		return NULL;
	}

	if (!self || !self->client->as) {
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
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	uint32_t count = 0;
	if (PyInt_Check(py_count)) {
		count = PyInt_AsLong(py_count);
	} else if (PyLong_Check(py_count)) {
		count = PyLong_AsLong(py_count);
		if (count == (uint32_t)-1 && PyErr_Occurred()) {
			if (PyErr_ExceptionMatches(PyExc_OverflowError)) {
				as_error_update(&err, AEROSPIKE_ERR_PARAM, "integer value exceeds sys.maxsize");
				goto CLEANUP;
			}
		}
	} else {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Count should be an integer or long");
		goto CLEANUP;
	}

	if (py_args && !filter_name) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Filter arguments without filter name");
		goto CLEANUP;
	}

	if (py_args && !PyList_Check(py_args) && (py_args != Py_None)) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid filter argument(type)");
		goto CLEANUP;
	}

	if (PyList_Check(py_args)) {
		pyobject_to_list(self->client, &err, py_args, &arg_list, &static_pool, SERIALIZER_PYTHON);
	}

	if (py_value != Py_None) {
		pyobject_to_val(self->client, &err, py_value, &from_val, &static_pool, SERIALIZER_PYTHON);
	} else {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Value should not be None");
		goto CLEANUP;
	}

	Py_BEGIN_ALLOW_THREADS
	aerospike_llist_find_from_filter(self->client->as, &err, apply_policy_p, &self->key, &self->llist, from_val, count, filter_name, arg_list, &elements_list);
	Py_END_ALLOW_THREADS
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	list_to_pyobject(self->client, &err, elements_list, &py_elements_list);

CLEANUP:
	if (elements_list) {
		as_list_destroy(elements_list);
	}

	if (from_val) {
		as_val_destroy(from_val);
	}

	if (err.code != AEROSPIKE_OK) {
		PyObject * py_err = NULL, *py_key = NULL;
		PyObject *exception_type = raise_exception(&err);
		error_to_pyobject(&err, &py_err);
		if (PyObject_HasAttrString(exception_type, "key")) {
			key_to_pyobject(&err, &self->key, &py_key);
			PyObject_SetAttrString(exception_type, "key", py_key);
			Py_DECREF(py_key);
		} 
		if (PyObject_HasAttrString(exception_type, "bin")) {
			PyObject *py_bins = PyString_FromString((char *)&self->bin_name);
			PyObject_SetAttrString(exception_type, "bin", py_bins);
			Py_DECREF(py_bins);
		}
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return NULL;
	}

	return py_elements_list;
}
/**
 ********************************************************************************************************
 * Select values from a begin key to end key corresponding to a value up to a maximum count applying a lua filter.
 *
 * @param self                  AerospikeLList object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 * 
 * Returns a list of ldt contents.
 * In case of error,appropriate exceptions will be raised.
 ********************************************************************************************************
 */
PyObject * AerospikeLList_Range_Limit(AerospikeLList * self, PyObject * args, PyObject * kwds)
{
	char* filter_name = NULL;
	PyObject* py_count = NULL;
	PyObject * py_args = NULL; 
	PyObject* py_policy = NULL;
	PyObject* py_from_value = NULL;
	PyObject* py_end_value = NULL;
	PyObject* py_elements_list = NULL;
	PyObject* py_filter_name = NULL;
	as_policy_apply apply_policy;
	as_policy_apply* apply_policy_p = NULL;
	as_list* arg_list = NULL;
	as_list* elements_list = NULL;
	as_val* from_val = NULL;
	as_val* end_val = NULL;

	as_static_pool static_pool;
	memset(&static_pool, 0, sizeof(static_pool));

	as_error err;
	as_error_init(&err);

	static char * kwlist[] = {"start_value", "end_value", "count", "function", "args", "policy", NULL};

	// Python Function Argument Parsing
	if (PyArg_ParseTupleAndKeywords(args, kwds, "OOO|OOO:range_limit", kwlist,
				&py_from_value, &py_end_value, &py_count, &py_filter_name, &py_args, &py_policy) == false) {
		return NULL;
	}

	if (!self || !self->client->as) {
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
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	uint32_t count = 0;
	if (PyInt_Check(py_count)) {
		count = PyInt_AsLong(py_count);
	} else if (PyLong_Check(py_count)) {
		count = PyLong_AsLong(py_count);
		if (count == (uint32_t)-1 && PyErr_Occurred()) {
			if (PyErr_ExceptionMatches(PyExc_OverflowError)) {
				as_error_update(&err, AEROSPIKE_ERR_PARAM, "integer value exceeds sys.maxsize");
				goto CLEANUP;
			}
		}
	} else {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Count should be an integer or long");
		goto CLEANUP;
	}

	if (py_filter_name) {
		if (PyString_Check(py_filter_name)) {
			filter_name = PyString_AsString(py_filter_name);
		} else if (py_filter_name != Py_None) {
			as_error_update(&err, AEROSPIKE_ERR_PARAM, "Filter name should be string or None");
			goto CLEANUP;
		}
	}
	if (py_args && !py_filter_name) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Filter arguments without filter name");
		goto CLEANUP;
	}

	if (!PyList_Check(py_args) && (py_args != Py_None)) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid filter argument(type)");
		goto CLEANUP;
	}

	if (py_args != Py_None) {
		pyobject_to_list(self->client, &err, py_args, &arg_list, &static_pool, SERIALIZER_PYTHON);
	}

	if (py_from_value != Py_None && py_end_value != Py_None) {
		pyobject_to_val(self->client, &err, py_from_value, &from_val, &static_pool, SERIALIZER_PYTHON);
		pyobject_to_val(self->client, &err, py_end_value, &end_val, &static_pool, SERIALIZER_PYTHON);
	} else {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Begin or end key cannot be None");
		goto CLEANUP;
	}

	Py_BEGIN_ALLOW_THREADS
	aerospike_llist_range_limit(self->client->as, &err, apply_policy_p, &self->key, &self->llist, from_val, end_val, count, filter_name, arg_list, &elements_list);
	Py_END_ALLOW_THREADS
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	list_to_pyobject(self->client, &err, elements_list, &py_elements_list);

CLEANUP:
	if (elements_list) {
		as_list_destroy(elements_list);
	}

	if (from_val) {
		as_val_destroy(from_val);
	}
	if (end_val) {
		as_val_destroy(end_val);
	}

	if (err.code != AEROSPIKE_OK) {
		PyObject * py_err = NULL, *py_key = NULL;
		PyObject *exception_type = raise_exception(&err);
		error_to_pyobject(&err, &py_err);
		if (PyObject_HasAttrString(exception_type, "key")) {
			key_to_pyobject(&err, &self->key, &py_key);
			PyObject_SetAttrString(exception_type, "key", py_key);
			Py_DECREF(py_key);
		} 
		if (PyObject_HasAttrString(exception_type, "bin")) {
			PyObject *py_bins = PyString_FromString((char *)&self->bin_name);
			PyObject_SetAttrString(exception_type, "bin", py_bins);
			Py_DECREF(py_bins);
		}
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return NULL;
	}

	return py_elements_list;
}
/**
 ********************************************************************************************************
 * Set page size for llist bin
 *
 * @param self                  AerospikeLList object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 * 
 * Returns a list of ldt contents.
 * In case of error,appropriate exceptions will be raised.
 ********************************************************************************************************
 */
PyObject * AerospikeLList_Set_Page_Size(AerospikeLList * self, PyObject * args, PyObject * kwds)
{
	uint64_t page_size;
	PyObject* py_policy = NULL;
	as_policy_apply apply_policy;
	as_policy_apply* apply_policy_p = NULL;

	as_error err;
	as_error_init(&err);

	static char * kwlist[] = {"size", "policy", NULL};

	// Python Function Argument Parsing
	if (PyArg_ParseTupleAndKeywords(args, kwds, "i|O:set_page_size", kwlist,
				&page_size, &py_policy) == false) {
		return NULL;
	}

	if (!self || !self->client->as) {
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
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	Py_BEGIN_ALLOW_THREADS
	aerospike_llist_set_page_size(self->client->as, &err, apply_policy_p, &self->key, &self->llist, page_size);
	Py_END_ALLOW_THREADS
CLEANUP:

	if (err.code != AEROSPIKE_OK) {
		PyObject * py_err = NULL, *py_key = NULL;
		PyObject *exception_type = raise_exception(&err);
		error_to_pyobject(&err, &py_err);
		if (PyObject_HasAttrString(exception_type, "key")) {
			key_to_pyobject(&err, &self->key, &py_key);
			PyObject_SetAttrString(exception_type, "key", py_key);
			Py_DECREF(py_key);
		} 
		if (PyObject_HasAttrString(exception_type, "bin")) {
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
	return PyLong_FromLong(0);
}
