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

#include <aerospike/aerospike.h>
#include <aerospike/as_error.h>

#include "client.h"
#include "conversions.h"
#include "exceptions.h"
#include "global_hosts.h"

#define MAX_PORT_SIZE 6
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
PyObject * AerospikeClient_Close(AerospikeClient * self, PyObject * args, PyObject * kwds)
{
	as_error err;
    char *alias_to_search = NULL;
    char port_str[MAX_PORT_SIZE];

	// Initialize error
	as_error_init(&err);

	if (!self || !self->as) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
		goto CLEANUP;
	}

	if (!self->is_conn_16) {
		as_error_update(&err, AEROSPIKE_ERR_CLUSTER, "No connection to aerospike cluster");
		goto CLEANUP;
	}

    PyObject *py_persistent_item = NULL;
    int i=0;
    for (i=0; i<self->as->config.hosts_size; i++)
    {
        int port = self->as->config.hosts[i].port;
        alias_to_search = (char*) PyMem_Malloc(strlen(self->as->config.hosts[i].addr) + strlen(self->as->config.user) + MAX_PORT_SIZE + 2);
        sprintf(port_str, "%d", port);
        strcpy(alias_to_search, self->as->config.hosts[i].addr);
        strcat(alias_to_search, ":");
        strcat(alias_to_search, port_str);
        strcat(alias_to_search, ":");
        strcat(alias_to_search, self->as->config.user);
        py_persistent_item = PyDict_GetItemString(py_global_hosts, alias_to_search); 
        if (py_persistent_item) {
            if (((AerospikeGlobalHosts*)py_persistent_item)->ref_cnt == 1) {
                printf("\nRef count delete");
     //           PyDict_DelItemString(py_global_hosts, alias_to_search);
            }
        }
        PyMem_Free(alias_to_search);
        alias_to_search = NULL;
    }
    ((AerospikeGlobalHosts*)py_persistent_item)->ref_cnt--;

    if (!((AerospikeGlobalHosts*)py_persistent_item)->ref_cnt)
    {
        printf("\nIn close");
	    aerospike_close(self->as, &err);
        AerospikeGlobalHosts_Del(py_persistent_item);
    }
    

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyObject *exception_type = raise_exception(&err);
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return NULL;
	}
	self->is_conn_16 = false;

	/*
	 * Need to free memory allocated to host address string
	 * in AerospikeClient_Type_Init.
	 */ 
	for( int i = 0; i < self->as->config.hosts_size; i++) {
		free((void *) self->as->config.hosts[i].addr);
	}

	aerospike_destroy(self->as);
	self->as = NULL;

	Py_INCREF(Py_None);
CLEANUP:
	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyObject *exception_type = raise_exception(&err);
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return NULL;
	}
	return Py_None;
}
