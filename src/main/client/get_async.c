/*******************************************************************************
 * Copyright 2013-2020 Aerospike, Inc.
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
#include <aerospike/as_key.h>
#include <aerospike/as_error.h>
#include <aerospike/as_record.h>

#include "client.h"
#include "conversions.h"
#include "exceptions.h"
#include "policy.h"

// Struct for Python User-Data for the Callback
typedef struct {
	as_key key;
	as_error error;
	PyObject *callback;
	AerospikeClient *client;
	as_policy_read read_policy;
	as_policy_read *read_policy_p;
} LocalData;

LocalData *async_cb_create(void) { return cf_malloc(sizeof(LocalData)); }

void async_cb_destroy(LocalData *uData) { cf_free(uData); }

void read_async_callback_helper(as_error *cmd_error, as_record *record,
								void *udata, as_event_loop *event_loop, int cb)
{
	PyObject *py_rec = NULL;
	PyObject *py_return = NULL;
	PyObject *py_arglist = NULL;
	PyObject *py_err = NULL;
	as_error *error = NULL;
	as_error temp_error;
	PyObject *py_key = NULL;
	PyObject *py_exception = NULL;

	// Extract callback user-data
	LocalData *data = (LocalData *)udata;
	PyObject *py_callback = data->callback;

	error = &data->error;
	if (cmd_error) {
		error = cmd_error;
	}

	// Lock Python State
	PyGILState_STATE gstate;
	if (cb) {
		gstate = PyGILState_Ensure();
	}
	error_to_pyobject(error, &py_err);
	// Convert as_key to python key object
	key_to_pyobject(&temp_error, &data->key, &py_key);

	if (error->code == AEROSPIKE_OK) {

		if (record_to_pyobject(data->client, &temp_error, record, &data->key,
							   &py_rec) != AEROSPIKE_OK) {
			as_error_copy(error, &temp_error);
		}
	}

	if (error->code == AEROSPIKE_OK) {
		if (py_rec && (!data->read_policy_p ||
					   (data->read_policy_p &&
						data->read_policy_p->key == AS_POLICY_KEY_DIGEST))) {
			// This is a special case.
			// C-client returns NULL key, so to the user
			// response will be (<ns>, <set>, None, <digest>)
			// Using the same input key, just making primary key part to be None
			// Only in case of POLICY_KEY_DIGEST or no policy specified
			PyObject *p_key = PyTuple_GetItem(py_rec, 0);
			Py_INCREF(Py_None);
			PyTuple_SetItem(p_key, 2, Py_None);
		}
	}

	if (error->code != AEROSPIKE_OK) {
		py_exception = raise_exception(error);
		if (PyObject_HasAttrString(py_exception, "key")) {
			PyObject_SetAttrString(py_exception, "key", py_key);
		}
		if (PyObject_HasAttrString(py_exception, "bin")) {
			PyObject_SetAttrString(py_exception, "bin", Py_None);
		}
		if (!cb) {
			PyErr_SetObject(py_exception, py_err);
			Py_DECREF(py_err);
		}
	}

	if (cb) {
		// Build Python Function Arguments
		py_arglist = PyTuple_New(4);

		if (!py_rec) {
			Py_INCREF(Py_None);
			py_rec = Py_None;
		}

		if (!py_exception) {
			Py_INCREF(Py_None);
			py_exception = Py_None;
		}

		PyTuple_SetItem(py_arglist, 0, py_key); //0-key tuple (ns, set, key, hash)
		PyTuple_SetItem(py_arglist, 1, py_rec); //1-record tuple (key-tuple, meta, bin)
		PyTuple_SetItem(py_arglist, 2, py_err); //2-error tuple
		PyTuple_SetItem(py_arglist, 3, py_exception); //3-exception
		
		// Invoke Python Callback
		py_return = PyObject_Call(py_callback, py_arglist, NULL);

		// Release Python Function Arguments
		Py_DECREF(py_arglist);

		// handle return value
		if (!py_return) {
			// an exception was raised, handle it (someday)
			// for now, we bail from the loop
			as_error_update(error, AEROSPIKE_ERR_CLIENT,
							"read_async_callback function raised an exception");
		}
		else {
			Py_DECREF(py_return);
		}
	}

	if (record) {
		as_record_destroy(record);
	}

	if (udata) {
		as_key_destroy(&data->key);
		//todo: dont free cb data in case of retry logic
		async_cb_destroy(udata);
	}

	if (cb) {
		PyGILState_Release(gstate);
	}

	return;
}

void read_async_callback(as_error *error, as_record *record, void *udata,
						 as_event_loop *event_loop)
{
	read_async_callback_helper(error, record, udata, event_loop, 1);
}

/**
 *******************************************************************************************************
 * Gets a record from the Aerospike DB.
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns a tuple of record having key, meata and bins sequentially.
 * In case of error,appropriate exceptions will be raised.
 *******************************************************************************************************
 */
PyObject *AerospikeClient_Get_Async(AerospikeClient *self, PyObject *args,
									PyObject *kwds)
{
	// Python Function Arguments
	PyObject *py_callback = NULL;
	PyObject *py_key = NULL;
	PyObject *py_policy = NULL;

	if (!async_support) {
		as_error err;
		as_error_init(&err);
		as_error_update(&err, AEROSPIKE_ERR, "Support for async is disabled, build software with async option");
		PyObject *py_err = NULL, *exception_type = NULL;
		error_to_pyobject(&err, &py_err);
		exception_type = raise_exception(&err);
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return NULL;
	}

	// Python Function Keyword Arguments
	static char *kwlist[] = {"get_callback", "key", "policy", NULL};

	// Python Function Argument Parsing
	if (PyArg_ParseTupleAndKeywords(args, kwds, "OO|O:get_async", kwlist,
									&py_callback, &py_key,
									&py_policy) == false) {
		return NULL;
	}

	// Create and initialize callback user-data
	LocalData *uData = async_cb_create();
	uData->callback = py_callback;
	uData->client = self;
	uData->read_policy_p = NULL;
	memset(&uData->key, 0, sizeof(uData->key));
	as_error_init(&uData->error);

	// For converting expressions.
	as_exp exp_list;
	as_exp *exp_list_p = NULL;

	// For converting predexp.
	as_predexp_list predexp_list;
	as_predexp_list *predexp_list_p = NULL;

	as_status status = AEROSPIKE_OK;

	if (!self || !self->as) {
		as_error_update(&uData->error, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
		goto CLEANUP;
	}

	if (!self->is_conn_16) {
		as_error_update(&uData->error, AEROSPIKE_ERR_CLUSTER,
						"No connection to aerospike cluster");
		goto CLEANUP;
	}

	// Convert python key object to as_key
	pyobject_to_key(&uData->error, py_key, &uData->key);
	if (uData->error.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	// Convert python policy object to as_policy_exists
	pyobject_to_policy_read(self, &uData->error, py_policy, &uData->read_policy,
							&uData->read_policy_p,
							&self->as->config.policies.read, &predexp_list,
							&predexp_list_p, &exp_list, &exp_list_p);
	if (uData->error.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	// Invoke operation
	Py_BEGIN_ALLOW_THREADS
	status = aerospike_key_get_async(uData->client->as, &uData->error,
									 uData->read_policy_p, &uData->key,
									 read_async_callback, uData, NULL, NULL);
	Py_END_ALLOW_THREADS
	if (status != AEROSPIKE_OK || uData->error.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

CLEANUP:

	if (exp_list_p) {
		as_exp_destroy(exp_list_p);
	}

	if (predexp_list_p) {
		as_predexp_list_destroy(&predexp_list);
	}

	if (status != AEROSPIKE_OK || uData->error.code != AEROSPIKE_OK) {
		//todo does raising exception alone or need cab to python?
		read_async_callback_helper(&uData->error, NULL, uData, NULL, 0);
		return NULL;
	}

	Py_INCREF(Py_None);

	return Py_None;
}
