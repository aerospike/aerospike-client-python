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
} LocalData;

LocalData *put_async_cb_create(void) { return cf_malloc(sizeof(LocalData)); }

void put_async_cb_destroy(LocalData *uData) { cf_free(uData); }

void write_async_callback_helper(as_error *cmd_error, void *udata,
								 as_event_loop *event_loop, int cb)
{
	PyObject *py_key = NULL;
	PyObject *py_return = NULL;
	PyObject *py_arglist = NULL;
	PyObject *py_err = NULL;
	as_error *error = NULL;
	as_error temp_error;
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
		py_arglist = PyTuple_New(3);

		if (!py_exception) {
			Py_INCREF(Py_None);
			py_exception = Py_None;
		}

		PyTuple_SetItem(py_arglist, 0, py_key);
		PyTuple_SetItem(py_arglist, 1, py_err);
		PyTuple_SetItem(py_arglist, 2, py_exception);

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

	if (udata) {
		as_key_destroy(&data->key);
		//todo: dont free cb data in case of retry logic
		put_async_cb_destroy(udata);
	}

	if (cb) {
		PyGILState_Release(gstate);
	}

	return;
}

void write_async_callback(as_error *error, void *udata,
						  as_event_loop *event_loop)
{
	write_async_callback_helper(error, udata, event_loop, 1);
}

/**
 *******************************************************************************************************
 * Puts a record asynchronously to the Aerospike DB.
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns an integer status. 0(Zero) is success value.
 * In case of error,appropriate exceptions will be raised.
 *******************************************************************************************************
 */
PyObject *AerospikeClient_Put_Async(AerospikeClient *self, PyObject *args,
									PyObject *kwds)
{
	// Aerospike Client Arguments
	as_policy_write write_policy;
	as_policy_write *write_policy_p = NULL;
	as_record rec;

	// For converting predexp.
	as_exp exp_list;
	as_exp *exp_list_p = NULL;

	// For converting predexp.
	as_predexp_list predexp_list;
	as_predexp_list *predexp_list_p = NULL;

	// Initialisation flags
	bool record_initialised = false;

	// Initialize record
	as_record_init(&rec, 0);
	record_initialised = true;

	as_static_pool static_pool;
	memset(&static_pool, 0, sizeof(static_pool));

	// Python Function Arguments
	PyObject *py_callback = NULL;
	PyObject *py_key = NULL;
	PyObject *py_bins = NULL;
	PyObject *py_meta = NULL;
	PyObject *py_policy = NULL;
	PyObject *py_serializer_option = NULL;
	long serializer_option = SERIALIZER_PYTHON;

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
	static char *kwlist[] = {"put_callback", "key",		   "bins", "meta",
							 "policy",		 "serializer", NULL};

	// Python Function Argument Parsing
	if (PyArg_ParseTupleAndKeywords(
			args, kwds, "OOO|OOO:put_async", kwlist, &py_callback, &py_key,
			&py_bins, &py_meta, &py_policy, &py_serializer_option) == false) {
		return NULL;
	}

	// Create and initialize callback user-data
	LocalData *uData = put_async_cb_create();
	uData->callback = py_callback;
	uData->client = self;
	memset(&uData->key, 0, sizeof(uData->key));

	as_error_init(&uData->error);

	as_status status = AEROSPIKE_OK;

	if (py_serializer_option) {
		if (PyLong_Check(py_serializer_option)) {
			self->is_client_put_serializer = true;
			serializer_option = PyLong_AsLong(py_serializer_option);
		}
	}
	else {
		self->is_client_put_serializer = false;
	}

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

	// Convert python bins and metadata objects to as_record
	pyobject_to_record(self, &uData->error, py_bins, py_meta, &rec, serializer_option,
					   &static_pool);
	if (uData->error.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	// Convert python policy object to as_policy_write
	pyobject_to_policy_write(self, &uData->error, py_policy, &write_policy,
							 &write_policy_p, &self->as->config.policies.write,
							 &predexp_list, &predexp_list_p, &exp_list,
							 &exp_list_p);
	if (uData->error.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	// Invoke operation
	Py_BEGIN_ALLOW_THREADS
	status = aerospike_key_put_async(self->as, &uData->error, write_policy_p,
									 &uData->key, &rec, write_async_callback,
									 uData, NULL, NULL);
	Py_END_ALLOW_THREADS
	if (status != AEROSPIKE_OK || uData->error.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

CLEANUP:
	POOL_DESTROY(&static_pool);

	if (exp_list_p) {
		as_exp_destroy(exp_list_p);
	}

	if (predexp_list_p) {
		as_predexp_list_destroy(&predexp_list);
	}

	if (record_initialised == true) {
		// Destroy the record if it is initialised.
		as_record_destroy(&rec);
	}

	// If an error occurred, tell Python.
	if (uData->error.code != AEROSPIKE_OK) {
		write_async_callback_helper(&uData->error, uData, NULL, 0);
		return NULL;
	}

	Py_INCREF(Py_None);
	return Py_None;
}
