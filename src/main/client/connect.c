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
PyObject * AerospikeClient_Connect(AerospikeClient * self, PyObject * args, PyObject * kwds)
{
	as_error err;
	as_error_init(&err);
    int8_t i = 0;
    char *alias_to_search = NULL;
    char *alias_to_hash = NULL;
    char port_str[MAX_PORT_SIZE];

	PyObject * py_username = NULL;
	PyObject * py_password = NULL;

	if ( PyArg_ParseTuple(args, "|OO:connect", &py_username, &py_password) == false ) {
		return NULL;
	}

	if ( py_username && PyString_Check(py_username) && py_password && PyString_Check(py_password) ) {
		char * username = PyString_AsString(py_username);
		char * password = PyString_AsString(py_password);
		as_config_set_user(&self->as->config, username, password);
	}

    //alias_to_search = (char*) PyMem_Malloc(strlen(self->as->config.hosts[i].addr) + strlen(self->as->config.user) + self->as->config.hostMAX_PORT_SIZE + 2);
    if (self->as->config.hosts_size) {
    int tot_address_size = 0;
    int tot_port_size = 0;
    int delimiter_size = 0;
    //Calculate total size for search string
    for (i=0; i<self->as->config.hosts_size; i++)
    {
        tot_address_size = tot_address_size + strlen(self->as->config.hosts[i].addr);
        tot_port_size = tot_port_size + MAX_PORT_SIZE;
        delimiter_size = delimiter_size + 3;
    }

    alias_to_search = (char*) PyMem_Malloc(tot_address_size + strlen(self->as->config.user) + tot_port_size + delimiter_size);

    //Create search string
    strcpy(alias_to_search, self->as->config.hosts[0].addr);
    int port = self->as->config.hosts[0].port;
    sprintf(port_str, "%d", port);
    strcat(alias_to_search, ":");
    strcat(alias_to_search, port_str);
    strcat(alias_to_search, ":");
    strcat(alias_to_search, self->as->config.user);
    strcat(alias_to_search, ";");

    for (i=1; i<self->as->config.hosts_size; i++) {
        strcat(alias_to_search, self->as->config.hosts[i].addr);
        port = self->as->config.hosts[i].port;
        sprintf(port_str, "%d", port);
        strcat(alias_to_search, ":");
        strcat(alias_to_search, port_str);
        strcat(alias_to_search, ":");
        strcat(alias_to_search, self->as->config.user);
        strcat(alias_to_search, ";");
    }
    PyObject * py_persistent_item = PyDict_GetItemString(py_global_hosts, alias_to_search); 
    if (py_persistent_item) {

        aerospike *as = ((AerospikeGlobalHosts*)py_persistent_item)->as;
        //Destroy the initial aeorpsike object as it has to point to the one in
        //the persistent list now
        aerospike_destroy(self->as);
        printf("\n In this if");

        self->as = as;
        self->as->config.shm_key = ((AerospikeGlobalHosts*)py_persistent_item)->shm_key;

        //Increase ref count of object containing same *as object
        ((AerospikeGlobalHosts*)py_persistent_item)->ref_cnt++;
        printf("\nRef cnt is: %d",((AerospikeGlobalHosts*)py_persistent_item)->ref_cnt);
        goto CLEANUP;
        PyMem_Free(alias_to_search);
        alias_to_search = NULL;
    }

    //Generate unique shm_key
    PyObject *py_key, *py_value;
    Py_ssize_t pos = 0;
    int flag = 0;
    int shm_key;
    if (user_shm_key) {
        shm_key = self->as->config.shm_key;
        user_shm_key = false;
    } else {
        shm_key = counter;
    }
    while(1) {
        flag = 0;
        while (PyDict_Next(py_global_hosts, &pos, &py_key, &py_value))
        {
            printf("\n In dictionary");
            if ((((AerospikeGlobalHosts*)py_value)->shm_key == shm_key)) {
                flag = 1;
                break;
            }
        }
        if (!flag) {
            self->as->config.shm_key = shm_key;
            printf("\n Counter is: %ld", shm_key);
            break;
        }
        shm_key = shm_key + 1;
    }
    self->as->config.shm_key = shm_key;
	aerospike_connect(self->as, &err);
    if (err.code != AEROSPIKE_OK) {
        goto CLEANUP;
    }
    PyObject * py_newobject = AerospikeGobalHosts_New(self->as);
    PyDict_SetItemString(py_global_hosts, alias_to_search, py_newobject);
    PyMem_Free(alias_to_search);
    alias_to_search = NULL;
    }

CLEANUP:
	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyObject *exception_type = raise_exception(&err);
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return NULL;
	}
	self->is_conn_16 = true;
	Py_INCREF(self);
	return (PyObject *) self;
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
PyObject * AerospikeClient_is_connected(AerospikeClient * self, PyObject * args, PyObject * kwds)
{

	if (1 == self->is_conn_16) //Need to define a macro AEROSPIKE_CONN_STATE
	{
      Py_INCREF(Py_True);
	  return Py_True;
	}

    Py_INCREF(Py_False);
	return Py_False;

}
