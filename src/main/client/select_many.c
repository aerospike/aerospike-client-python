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

/**
 *************************************************************************
 * This function will store all the Unicode objects in a pool, created in
 * UnicodePyObject structure.
 * 
 * @param u_obj                   Pool of Unicode objects.
 * @param py_uobj                 Unicode PyObject to be stored in a pool
 * 
 *************************************************************************
 **/
PyObject *store_unicode_bins(UnicodePyObjects *u_obj, PyObject *py_uobj)
{
	if (u_obj->size < MAX_UNICODE_OBJECTS) {
		u_obj->ob[u_obj->size++] = py_uobj;
	}
	return py_uobj;
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
static PyObject *batch_select_aerospike_batch_read(
	as_error *err, AerospikeClient *self, PyObject *py_keys,
	as_policy_batch *batch_policy_p, char **filter_bins, Py_ssize_t bins_size)
{
	PyObject *py_recs = NULL;

	as_batch_read_records records;

	as_batch_read_record *record = NULL;
	bool batch_initialised = false;

	// Convert python keys list to as_key ** and add it to as_batch.keys
	// keys can be specified in PyList or PyTuple
	if (py_keys && PyList_Check(py_keys)) {
		Py_ssize_t size = PyList_Size(py_keys);

		as_batch_read_inita(&records, size);

		// Batch object initialised
		batch_initialised = true;

		for (int i = 0; i < size; i++) {
			PyObject *py_key = PyList_GetItem(py_keys, i);

			if (!PyTuple_Check(py_key)) {
				as_error_update(err, AEROSPIKE_ERR_PARAM,
								"Key should be a tuple.");
				goto CLEANUP;
			}

			record = as_batch_read_reserve(&records);

			pyobject_to_key(err, py_key, &record->key);
			if (bins_size) {
				record->bin_names = filter_bins;
				record->n_bin_names = bins_size;
			}
			else {
				record->read_all_bins = true;
			}

			if (err->code != AEROSPIKE_OK) {
				goto CLEANUP;
			}
		}
	}
	else if (py_keys && PyTuple_Check(py_keys)) {
		Py_ssize_t size = PyTuple_Size(py_keys);

		as_batch_read_inita(&records, size);
		// Batch object initialised
		batch_initialised = true;

		for (int i = 0; i < size; i++) {
			PyObject *py_key = PyTuple_GetItem(py_keys, i);

			if (!PyTuple_Check(py_key)) {
				as_error_update(err, AEROSPIKE_ERR_PARAM,
								"Key should be a tuple.");
				goto CLEANUP;
			}

			record = as_batch_read_reserve(&records);

			pyobject_to_key(err, py_key, &record->key);
			if (bins_size) {
				record->bin_names = filter_bins;
				record->n_bin_names = bins_size;
			}
			else {
				record->read_all_bins = true;
			}

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
	aerospike_batch_read(self->as, err, batch_policy_p, &records);
	Py_END_ALLOW_THREADS
	if (err->code != AEROSPIKE_OK) {
		goto CLEANUP;
	}
	batch_read_records_to_pyobject(self, err, &records, &py_recs);

CLEANUP:
	if (batch_initialised == true) {
		// We should destroy batch object as we are using 'as_batch_init' for initialisation
		// Also, pyobject_to_key is soing strdup() in case of Unicode. So, object destruction
		// is necessary.
		as_batch_read_destroy(&records);
	}

	return py_recs;
}

/**
 *********************************************************************
 * This function will invoke aerospike_batch_get_bins to get filtered
 * bins from all the records in a batches.
 *
 * @param self                    AerospikeClient object
 * @param py_keys                 List of keys passed on by user
 * @param py_bins                 List of filter bins passed on by user
 * @param py_policy               User specified Policy dictionary
 *
 *********************************************************************
 **/
static PyObject *AerospikeClient_Select_Many_Invoke(AerospikeClient *self,
													PyObject *py_keys,
													PyObject *py_bins,
													PyObject *py_policy)
{
	// Python Return Value
	PyObject *py_recs = NULL;

	// Aerospike Client Arguments
	as_error err;
	as_policy_batch policy;
	as_policy_batch *batch_policy_p = NULL;
	Py_ssize_t bins_size = 0;
	char **filter_bins = NULL;

	// For converting predexp.
	as_exp exp_list;
	as_exp *exp_list_p = NULL;

	// For converting predexp.
	as_predexp_list predexp_list;
	as_predexp_list *predexp_list_p = NULL;

	// Unicode object's pool
	UnicodePyObjects u_objs;
	u_objs.size = 0;
	int i = 0;

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

	// Check the type of bins and get it's size
	// i.e. number of bins provided
	if (py_bins && PyList_Check(py_bins)) {
		bins_size = PyList_Size(py_bins);
	}
	else if (py_bins && PyTuple_Check(py_bins)) {
		bins_size = PyTuple_Size(py_bins);
	}
	else {
		as_error_update(&err, AEROSPIKE_ERR_PARAM,
						"Filter bins should be specified as a list or tuple.");
		goto CLEANUP;
	}

	filter_bins = (char **)malloc(sizeof(char *) * bins_size);

	for (i = 0; i < bins_size; i++) {
		PyObject *py_bin = NULL;
		if (PyList_Check(py_bins)) {
			py_bin = PyList_GetItem(py_bins, i);
		}
		if (PyTuple_Check(py_bins)) {
			py_bin = PyTuple_GetItem(py_bins, i);
		}
		if (PyUnicode_Check(py_bin)) {
			// Store the unicode object into a pool
			// It is DECREFed at later stages
			// So, no need of DECREF here.
			filter_bins[i] = PyBytes_AsString(
				store_unicode_bins(&u_objs, PyUnicode_AsUTF8String(py_bin)));
		}
		else if (PyString_Check(py_bin)) {
			filter_bins[i] = PyString_AsString(py_bin);
		}
		else {
			as_error_update(&err, AEROSPIKE_ERR_PARAM,
							"Bin name should be a string or unicode string.");
			goto CLEANUP;
		}
	}

	// Convert python policy object to as_policy_batch
	pyobject_to_policy_batch(self, &err, py_policy, &policy, &batch_policy_p,
							 &self->as->config.policies.batch, &predexp_list,
							 &predexp_list_p, &exp_list, &exp_list_p);
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	py_recs = batch_select_aerospike_batch_read(
		&err, self, py_keys, batch_policy_p, filter_bins, bins_size);

CLEANUP:

	if (filter_bins) {
		free(filter_bins);
	}

	if (exp_list_p) {
		as_exp_destroy(exp_list_p);
		;
	}

	if (predexp_list_p) {
		as_predexp_list_destroy(&predexp_list);
	}

	// DECREFed all the unicode objects stored in Pool
	for (i = 0; i < u_objs.size; i++) {
		Py_DECREF(u_objs.ob[i]);
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

PyObject *AerospikeClient_Select_Many(AerospikeClient *self, PyObject *args,
									  PyObject *kwds)
{
	// Python Function Arguments
	PyObject *py_keys = NULL;
	PyObject *py_bins = NULL;
	PyObject *py_policy = NULL;

	// Python Function Keyword Arguments
	static char *kwlist[] = {"keys", "bins", "policy", NULL};

	// Python Function Argument Parsing
	if (PyArg_ParseTupleAndKeywords(args, kwds, "OO|O:select_many", kwlist,
									&py_keys, &py_bins, &py_policy) == false) {
		return NULL;
	}

	// Invoke Operation
	return AerospikeClient_Select_Many_Invoke(self, py_keys, py_bins,
											  py_policy);
}
