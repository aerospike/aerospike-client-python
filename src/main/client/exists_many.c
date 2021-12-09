/*******************************************************************************
 * Copyright 2013-2021 Aerospike, Inc.
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

#include <aerospike/aerospike_key.h>
#include <aerospike/aerospike_batch.h>
#include <aerospike/as_key.h>
#include <aerospike/as_error.h>
#include <aerospike/as_record.h>
#include <aerospike/as_batch.h>

#include "client.h"
#include "conversions.h"
#include "exceptions.h"
#include "policy.h"

typedef struct _exists_many_cb_data {
	PyObject *py_recs;
	as_error *cb_err;
} exists_many_cb_data;

static void make_batch_safe_to_free(as_batch *batch, int size);
/**
 *******************************************************************************************************
 * This callback will be called with the results with aerospike_batch_exists().
 *
 * @param results               An array of n as_batch_read entries
 * @param n                     The number of results from the batch request
 * @param udata                 The return value to be filled with result of
 *                              exists_many()
 *
 * Returns boolean value(true or false).
 *******************************************************************************************************
 */
static bool batch_exists_cb(const as_batch_read *results, uint32_t n,
							void *udata)
{
	// Typecast udata back to PyObject
	exists_many_cb_data *local_data = (exists_many_cb_data *)udata;
	PyObject *py_recs = local_data->py_recs;
	as_error local_err;
	as_error_init(&local_err);

	// Lock Python State
	PyGILState_STATE gstate;
	gstate = PyGILState_Ensure();

	// Loop over results array
	for (uint32_t i = 0; i < n; i++) {

		PyObject *py_rec = NULL;
		PyObject *py_meta = NULL;
		PyObject *py_key = NULL;

		if (results[i].result == AEROSPIKE_OK) {
			key_to_pyobject(&local_err, results[i].key, &py_key);
			if (!py_key) {
				py_key = Py_None;
				Py_INCREF(py_key);
			}

			metadata_to_pyobject(&local_err, &results[i].record, &py_meta);
			if (!py_meta) {
				py_meta = Py_None;
				Py_INCREF(py_meta);
			}
			py_rec = Py_BuildValue("OO", py_key, py_meta);
			Py_DECREF(py_key);
			Py_DECREF(py_meta);
			if (!py_rec) {
				as_error_update(local_data->cb_err, AEROSPIKE_ERR_CLIENT,
								"Failed to create metadata tuple");
				PyGILState_Release(gstate);
				return false;
			}
		}
		else {
			key_to_pyobject(&local_err, results[i].key, &py_key);
			if (!py_key) {
				py_key = Py_None;
				Py_INCREF(py_key);
			}
			py_rec = Py_BuildValue("OO", py_key, Py_None);
			Py_DECREF(py_key);
			if (!py_rec) {
				PyGILState_Release(gstate);
				as_error_update(local_data->cb_err, AEROSPIKE_ERR_CLIENT,
								"Failed to create metadata tuple");
				return false;
			}
		}
		if (PyList_SetItem(py_recs, i, py_rec) != 0) {
			Py_XDECREF(py_rec);
			PyGILState_Release(gstate);
			as_error_update(local_data->cb_err, AEROSPIKE_ERR_CLIENT,
							"Failed to add record to metadata tuple");
			return false;
		}
	}
	// Release Python State
	PyGILState_Release(gstate);
	return true;
}

/**
 *******************************************************************************************************
 * This function will get a batch of records from the Aeropike DB.
 *
 * @param err                   as_error object
 * @param self                  AerospikeClient object
 * @param py_keys               The list of keys
 * @param batch_policy_p        as_policy_batch object
 *
 * Returns the record if key exists otherwise NULL.
 *******************************************************************************************************
 */
static PyObject *
batch_exists_aerospike_batch_exists(as_error *err, AerospikeClient *self,
									PyObject *py_keys,
									as_policy_batch *batch_policy_p)
{

	as_batch batch;
	bool batch_initialised = false;
	exists_many_cb_data cb_data;
	as_error local_err;
	as_error_init(&local_err);
	cb_data.py_recs = NULL;
	cb_data.cb_err = &local_err;

	// Convert python keys list to as_key ** and add it to as_batch.keys
	// keys can be specified in PyList or PyTuple
	if (py_keys && PyList_Check(py_keys)) {
		Py_ssize_t size = PyList_Size(py_keys);

		as_batch_init(&batch, size);
		make_batch_safe_to_free(&batch, size);

		cb_data.py_recs = PyList_New(size);
		if (!cb_data.py_recs) {
			as_error_update(err, AEROSPIKE_ERR_PARAM,
							"Failed to allocate return record");
			goto CLEANUP;
		}
		// Batch object initialised
		batch_initialised = true;

		for (int i = 0; i < size; i++) {

			PyObject *py_key = PyList_GetItem(py_keys, i);

			if (!PyTuple_Check(py_key)) {
				as_error_update(err, AEROSPIKE_ERR_PARAM,
								"Key should be a tuple.");
				goto CLEANUP;
			}

			pyobject_to_key(err, py_key, as_batch_keyat(&batch, i));

			if (err->code != AEROSPIKE_OK) {
				goto CLEANUP;
			}
		}
	}
	else if (py_keys && PyTuple_Check(py_keys)) {
		Py_ssize_t size = PyTuple_Size(py_keys);

		cb_data.py_recs = PyList_New(size);
		if (!cb_data.py_recs) {
			as_error_update(err, AEROSPIKE_ERR_PARAM,
							"Failed to allocate return record");
			goto CLEANUP;
		}

		as_batch_init(&batch, size);
		// Batch object initialised
		batch_initialised = true;
		make_batch_safe_to_free(&batch, size);

		for (int i = 0; i < size; i++) {
			PyObject *py_key = PyTuple_GetItem(py_keys, i);

			if (!PyTuple_Check(py_key)) {
				as_error_update(err, AEROSPIKE_ERR_PARAM,
								"Key should be a tuple.");
				goto CLEANUP;
			}

			pyobject_to_key(err, py_key, as_batch_keyat(&batch, i));

			if (err->code != AEROSPIKE_OK) {
				goto CLEANUP;
			}
		}
	}
	else {
		as_error_update(err, AEROSPIKE_ERR_PARAM,
						"Keys should be specified as a list or tuple.");
		goto CLEANUP;
	}

	// Invoke C-client API
	Py_BEGIN_ALLOW_THREADS
	aerospike_batch_exists(self->as, err, batch_policy_p, &batch,
						   (aerospike_batch_read_callback)batch_exists_cb,
						   &cb_data);
	Py_END_ALLOW_THREADS
	if (err->code != AEROSPIKE_OK) {
		as_error_update(err, err->code, NULL);
		Py_CLEAR(cb_data.py_recs);
	}
	if (cb_data.cb_err->code != AEROSPIKE_OK) {
		as_error_update(err, cb_data.cb_err->code, cb_data.cb_err->message);
		Py_CLEAR(cb_data.py_recs);
	}

CLEANUP:
	if (batch_initialised == true) {
		// We should destroy batch object as we are using 'as_batch_init' for initialisation
		// Also, pyobject_to_key is soing strdup() in case of Unicode. So, object destruction
		// is necessary.
		as_batch_destroy(&batch);
	}

	return cb_data.py_recs;
}
/**
 *******************************************************************************************************
 * This function checks if a batch of records are present in DB or not.
 *
 * @param self                  AerospikeClient object
 * @param py_keys               The list of keys
 * @param py_policy             The dictionary of policies
 *
 * Returns the metadata of a record if key exists otherwise NULL.
 *******************************************************************************************************
 */
static PyObject *AerospikeClient_Exists_Many_Invoke(AerospikeClient *self,
													PyObject *py_keys,
													PyObject *py_policy)
{
	// Python Return Value
	PyObject *py_recs = NULL;

	// Aerospike Client Arguments
	as_error err;
	as_policy_batch policy;
	as_policy_batch *batch_policy_p = NULL;

	// For converting expressions.
	as_exp exp_list;
	as_exp *exp_list_p = NULL;

	// For converting predexp.
	as_predexp_list predexp_list;
	as_predexp_list *predexp_list_p = NULL;

	// Initialize error
	as_error_init(&err);

	if (!self || !self->as) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
		goto CLEANUP;
	}

	if (!self->is_conn_16) {
		as_error_update(&err, AEROSPIKE_ERR_CLUSTER,
						"No connection to aerospike cluster");
		goto CLEANUP;
	}

	// Convert python policy object to as_policy_batch
	pyobject_to_policy_batch(self, &err, py_policy, &policy, &batch_policy_p,
							 &self->as->config.policies.batch, &predexp_list,
							 &predexp_list_p, &exp_list, &exp_list_p);
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	py_recs = batch_exists_aerospike_batch_exists(&err, self, py_keys,
												  batch_policy_p);

CLEANUP:

	if (exp_list_p) {
		as_exp_destroy(exp_list_p);
		;
	}

	if (predexp_list_p) {
		as_predexp_list_destroy(&predexp_list);
	}

	if (err.code != AEROSPIKE_OK) {
		PyObject *py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyObject *exception_type = raise_exception(&err);
		if (PyObject_HasAttrString(exception_type, "key")) {
			PyObject_SetAttrString(exception_type, "key", py_keys);
		}
		if (PyObject_HasAttrString(exception_type, "bin")) {
			PyObject_SetAttrString(exception_type, "bin", Py_None);
		}
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return NULL;
	}

	return py_recs;
}

/**
 *******************************************************************************************************
 * Read the meta-data of records from the database in batch.
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns a dictionary of record with key to be primary key and value
 * to be meatadata of a record.
 * In case of error,appropriate exceptions will be raised.
 *******************************************************************************************************
 */
PyObject *AerospikeClient_Exists_Many(AerospikeClient *self, PyObject *args,
									  PyObject *kwds)
{
	// Python Function Arguments
	PyObject *py_keys = NULL;
	PyObject *py_policy = NULL;

	// Python Function Keyword Arguments
	static char *kwlist[] = {"keys", "policy", NULL};

	// Python Function Argument Parsing
	if (PyArg_ParseTupleAndKeywords(args, kwds, "O|O:exists_many", kwlist,
									&py_keys, &py_policy) == false) {
		return NULL;
	}

	// Invoke Operation
	return AerospikeClient_Exists_Many_Invoke(self, py_keys, py_policy);
}

/*
 * This marks each key in the batch's value pointer as null
 * and sets it to not be freed on as_key_destroy.
 * This is needed so that as_batch_destroy does not try to free
 * any uninitialized data.
 */
static void make_batch_safe_to_free(as_batch *batch, int size)
{
	for (int i = 0; i < size; i++) {
		as_key *batch_key = as_batch_keyat(batch, i);
		if (batch_key) {
			batch_key->valuep = NULL;
			batch_key->_free = false;
		}
	}
}
