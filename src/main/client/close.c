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
#include "exceptions.h"
#include "global_hosts.h"

#define MAX_PORT_SIZE 6
#define MAX_SHM_SIZE 19

/**
 *******************************************************************************************************
 * Closes already opened connection to the database.
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns None.
 * In case of error,appropriate exceptions will be raised.
 *******************************************************************************************************
 */
PyObject *AerospikeClient_Close(AerospikeClient *self, PyObject *args,
								PyObject *kwds)
{
	as_error err;
	char *alias_to_search = NULL;
	PyObject *py_persistent_item = NULL;
	AerospikeGlobalHosts *global_host = NULL;

	// Initialize error
	as_error_init(&err);

	if (!self || !self->as) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
		goto CLEANUP;
	}

	if (!self->is_conn_16) {
		goto CLEANUP;
	}

	if (self->use_shared_connection) {
		alias_to_search = return_search_string(self->as);
		py_persistent_item =
			PyDict_GetItemString(py_global_hosts, alias_to_search);

		if (py_persistent_item) {
			global_host = (AerospikeGlobalHosts *)py_persistent_item;
			// It is only safe to do a reference counted close if the
			// local as is pointing to the global as
			if (self->as == global_host->as) {
				close_aerospike_object(self->as, &err, alias_to_search,
									   py_persistent_item, false);
			}
		}

		PyMem_Free(alias_to_search);
		alias_to_search = NULL;
	}
	else {
		Py_BEGIN_ALLOW_THREADS
		aerospike_close(self->as, &err);
		Py_END_ALLOW_THREADS
	}
	self->is_conn_16 = false;

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

char *return_search_string(aerospike *as)
{
	char port_str[MAX_PORT_SIZE];

	int tot_address_size = 0;
	int tot_port_size = 0;
	int delimiter_size = 0;
	int tot_user_size = 0;
	int tot_shm_size = 0;
	uint32_t i = 0;

	//Calculate total size for search string
	for (i = 0; i < as->config.hosts->size; i++) {
		as_host *host = (as_host *)as_vector_get(as->config.hosts, i);
		tot_address_size = tot_address_size + strlen(host->name);
		tot_port_size = tot_port_size + MAX_PORT_SIZE;
		delimiter_size = delimiter_size + 3;
		tot_user_size = tot_user_size + strlen(as->config.user);
	}
	if (as->config.use_shm) {
		tot_shm_size = MAX_SHM_SIZE;
	}

	char *alias_to_search =
		(char *)PyMem_Malloc(tot_address_size + tot_user_size + tot_port_size +
							 delimiter_size + tot_shm_size);
	alias_to_search[0] = '\0';

	for (i = 0; i < as->config.hosts->size; i++) {
		as_host *host = (as_host *)as_vector_get(as->config.hosts, i);
		int port = host->port;
		sprintf(port_str, "%d", port);
		strcat(alias_to_search, host->name);
		strcat(alias_to_search, ":");
		strcat(alias_to_search, port_str);
		strcat(alias_to_search, ":");
		strcat(alias_to_search, as->config.user);
		strcat(alias_to_search, ";");
	}

	if (as->config.use_shm) {
		char shm_str[MAX_SHM_SIZE];
		sprintf(shm_str, "%x", as->config.shm_key);
		strcat(alias_to_search, shm_str);
	}

	return alias_to_search;
}

void close_aerospike_object(aerospike *as, as_error *err, char *alias_to_search,
							PyObject *py_persistent_item, bool do_destroy)
{
	if (((AerospikeGlobalHosts *)py_persistent_item)->ref_cnt == 1) {
		PyDict_DelItemString(py_global_hosts, alias_to_search);
		AerospikeGlobalHosts_Del(py_persistent_item);
		Py_BEGIN_ALLOW_THREADS
		aerospike_close(as, err);
		Py_END_ALLOW_THREADS
	}
	else {
		((AerospikeGlobalHosts *)py_persistent_item)->ref_cnt--;
	}
}
