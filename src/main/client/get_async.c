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

LocalData* async_cb_create(void)
{
	return cf_malloc(sizeof(LocalData));
}

void async_cb_destroy(LocalData *uData)
{
	cf_free(uData);
}

void
read_async_callback(as_error* error, as_record* record, void* udata, as_event_loop* event_loop)
{
	PyObject *py_rec = NULL;
	PyObject *py_return = NULL;
	PyObject *py_arglist = NULL;

	// Extract callback user-data
	LocalData *data = (LocalData *)udata;
	as_error *err = &data->error;
	PyObject *py_callback = data->callback;

	// Lock Python State
	PyGILState_STATE gstate;
	gstate = PyGILState_Ensure();

	if (!error || error->code == AEROSPIKE_OK) {

		if (record_to_pyobject(data->client, err, record, &data->key, &py_rec) !=
			AEROSPIKE_OK) {
			goto CLEANUP;
		}

		if (!data->read_policy_p ||
			(data->read_policy_p && data->read_policy_p->key == AS_POLICY_KEY_DIGEST)) {
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
	else {
		PyObject *py_err = NULL, *py_key = NULL;
		error_to_pyobject(err, &py_err);
		PyObject *exception_type = raise_exception(err);
		// Convert as_key to python key object
		key_to_pyobject(err, &data->key, &py_key);
		if (PyObject_HasAttrString(exception_type, "key")) {
			PyObject_SetAttrString(exception_type, "key", py_key);
		}
		if (PyObject_HasAttrString(exception_type, "bin")) {
			PyObject_SetAttrString(exception_type, "bin", Py_None);
		}
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		goto CLEANUP;
	}

	// Build Python Function Arguments
	py_arglist = PyTuple_New(1);
	PyTuple_SetItem(py_arglist, 0, py_rec);

	// Invoke Python Callback
	py_return = PyObject_Call(py_callback, py_arglist, NULL);

	// Release Python Function Arguments
	Py_DECREF(py_arglist);

	// handle return value
	if (!py_return) {
		// an exception was raised, handle it (someday)
		// for now, we bail from the loop
		as_error_update(err, AEROSPIKE_ERR_CLIENT,
						"read_async_callback function raised an exception");
	}
	else {
		Py_DECREF(py_return);
	}


CLEANUP:
	if (record) {
		as_record_destroy(record);
	}

	if (udata) {
		as_key_destroy(&data->key);
		//todo: dont free cb data in case of retry logic
		async_cb_destroy(udata);
	}

	PyGILState_Release(gstate);

	return;
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

	// Python Function Keyword Arguments
	static char *kwlist[] = {"get_callback", "key", "policy", NULL};

	// Lock Python State
	PyGILState_STATE gstate;
	gstate = PyGILState_Ensure();

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
	as_error_init(&uData->error);

	// Aerospike Client Arguments
	as_error err;

	// For converting expressions.
	as_exp exp_list;
	as_exp *exp_list_p = NULL;

	// For converting predexp.
	as_predexp_list predexp_list;
	as_predexp_list *predexp_list_p = NULL;

	// Initialised flags
	bool key_initialised = false;

	as_status status = AEROSPIKE_OK;

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

	// Convert python key object to as_key
	pyobject_to_key(&err, py_key, &uData->key);
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}
	// Key is successfully initialised.
	key_initialised = true;

	// Convert python policy object to as_policy_exists
	pyobject_to_policy_read(self, &err, py_policy, &uData->read_policy, &uData->read_policy_p,
							&self->as->config.policies.read, &predexp_list,
							&predexp_list_p, &exp_list, &exp_list_p);
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	// Invoke operation
	Py_BEGIN_ALLOW_THREADS
	status = aerospike_key_get_async(uData->client->as, &uData->error, 
									uData->read_policy_p, &uData->key, read_async_callback, uData, NULL, NULL);
	Py_END_ALLOW_THREADS
	if (status != AEROSPIKE_OK || err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

CLEANUP:

	if (exp_list_p) {
		as_exp_destroy(exp_list_p);
	}

	if (predexp_list_p) {
		as_predexp_list_destroy(&predexp_list);
	}

	if (status != AEROSPIKE_OK || err.code != AEROSPIKE_OK) {
		PyObject *py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyObject *exception_type = raise_exception(&err);
		if (PyObject_HasAttrString(exception_type, "key")) {
			PyObject_SetAttrString(exception_type, "key", py_key);
		}
		if (PyObject_HasAttrString(exception_type, "bin")) {
			PyObject_SetAttrString(exception_type, "bin", Py_None);
		}
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);

		if (key_initialised == true) {
			// Destroy key only if it is initialised.
			as_key_destroy(&uData->key);
		}

		if (uData) {
			async_cb_destroy(uData);
		}
		return NULL;
	}

	PyGILState_Release(gstate);

	Py_INCREF(Py_None);
	return Py_None;
}
