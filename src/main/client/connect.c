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

#include <aerospike/aerospike.h>
#include <aerospike/as_error.h>

#include "client.h"
#include "conversions.h"
#include "global_hosts.h"
#include "exceptions.h"

#define MAX_PORT_SIZE 6
/**
 *******************************************************************************************************
 * Establishes a connection to the Aerospike DB instance.
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns an instance of aerospike.Client, Which can be used later to do usual
 * database operations.
 * In case of error,appropriate exceptions will be raised.
 *******************************************************************************************************
 */
PyObject *AerospikeClient_Connect(AerospikeClient *self, PyObject *args,
								  PyObject *kwds)
{
	as_error err;
	as_error_init(&err);
	char *alias_to_search = NULL;
	bool free_alias_to_search = false;
	PyObject *py_username = NULL;
	PyObject *py_password = NULL;

	if (PyArg_ParseTuple(args, "|OO:connect", &py_username, &py_password) ==
		false) {
		return NULL;
	}

	if (py_username && PyString_Check(py_username) && py_password &&
		PyString_Check(py_password)) {
		char *username = PyString_AsString(py_username);
		char *password = PyString_AsString(py_password);
		as_config_set_user(&self->as->config, username, password);
	}

	if (!self || !self->as || !self->as->config.hosts ||
		!self->as->config.hosts->size) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM,
						"Invalid aerospike object or hosts not configured");
		goto CLEANUP;
	}

	alias_to_search = return_search_string(self->as);
	free_alias_to_search = true;

	if (self->use_shared_connection) {
		PyObject *py_persistent_item =
			PyDict_GetItemString(py_global_hosts, alias_to_search);
		if (py_persistent_item) {
			aerospike *as = ((AerospikeGlobalHosts *)py_persistent_item)->as;
			//Destroy the initial aerospike object as it has to point to the one in
			//the persistent list now
			if (as != self->as) {
				// If the client has previously connected
				// Other clients may share its aerospike* pointer
				// So it is not safe to destroy it
				if (!self->has_connected) {
					aerospike_destroy(self->as);
				}
				self->as = as;
				self->as->config.shm_key =
					((AerospikeGlobalHosts *)py_persistent_item)->shm_key;

				//Increase ref count of global host entry
				((AerospikeGlobalHosts *)py_persistent_item)->ref_cnt++;
			}
			else {
				// If there is a matching global host entry,
				// and this client was disconnected, increment the ref_cnt of the global.
				// If the client is already connected, do nothing.
				if (!self->is_conn_16) {
					((AerospikeGlobalHosts *)py_persistent_item)->ref_cnt++;
				}
			}
			goto CLEANUP;
		}
	}
	//Generate unique shm_key
	PyObject *py_key, *py_value;
	Py_ssize_t pos = 0;
	int flag = 0;
	int shm_key;
	if (self->as->config.use_shm) {
		if (user_shm_key) {
			shm_key = self->as->config.shm_key;
			user_shm_key = false;
		}
		else {
			shm_key = counter;
		}
		while (1) {
			flag = 0;
			while (PyDict_Next(py_global_hosts, &pos, &py_key, &py_value)) {
				if (((AerospikeGlobalHosts *)py_value)->as->config.use_shm) {
					if (((AerospikeGlobalHosts *)py_value)->shm_key ==
						shm_key) {
						flag = 1;
						break;
					}
				}
			}
			if (!flag) {
				self->as->config.shm_key = shm_key;
				break;
			}
			shm_key = shm_key + 1;
		}
		self->as->config.shm_key = shm_key;
	}

	Py_BEGIN_ALLOW_THREADS
	aerospike_connect(self->as, &err);
	Py_END_ALLOW_THREADS
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}
	if (self->use_shared_connection) {
		PyObject *py_newobject = (PyObject *)AerospikeGobalHosts_New(self->as);
		PyDict_SetItemString(py_global_hosts, alias_to_search, py_newobject);
	}

CLEANUP:
	if (free_alias_to_search && alias_to_search) {
		PyMem_Free(alias_to_search);
		alias_to_search = NULL;
	}

	if (err.code != AEROSPIKE_OK) {
		PyObject *py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyObject *exception_type = raise_exception(&err);
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);

		return NULL;
	}
	self->is_conn_16 = true;
	self->has_connected = true;
	Py_INCREF(self);
	return (PyObject *)self;
}

/**
 *******************************************************************************************************
 * Tests the connection to the Aerospike DB
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns true or false.
 *******************************************************************************************************
 */
PyObject *AerospikeClient_is_connected(AerospikeClient *self, PyObject *args,
									   PyObject *kwds)
{
	if (!self || !self->is_conn_16) {
		Py_INCREF(Py_False);
		return Py_False;
	}

	if (self->as && aerospike_cluster_is_connected(self->as)) {
		Py_INCREF(Py_True);
		return Py_True;
	}

	Py_INCREF(Py_False);
	return Py_False;
}

/**
 *******************************************************************************************************
 * Get shm_key configured with the Aerospike DB
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns true or false.
 *******************************************************************************************************
 */
PyObject *AerospikeClient_shm_key(AerospikeClient *self, PyObject *args,
								  PyObject *kwds)
{
	as_error err;
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

	if (self->as->config.use_shm && self->as->config.shm_key) {
		return PyLong_FromUnsignedLong((unsigned int)self->as->config.shm_key);
	}

CLEANUP:
	if (err.code != AEROSPIKE_OK) {
		PyObject *py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyObject *exception_type = raise_exception(&err);
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return NULL;
	}

	Py_INCREF(Py_None);
	return Py_None;
}
