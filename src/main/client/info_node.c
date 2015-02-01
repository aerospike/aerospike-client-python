/*******************************************************************************
 * Copyright 2013-2014 Aerospike, Inc.
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

#include <aerospike/aerospike_info.h>
#include <aerospike/as_key.h>
#include <aerospike/as_error.h>
#include <aerospike/as_node.h>
#include <aerospike/as_record.h>

#include "client.h"
#include "conversions.h"
#include "policy.h"

#define MAX_HOST_COUNT 128
#define INFO_REQUEST_RESPONSE_DELIMITER "\t"
#define INFO_RESPONSE_END "\n"
#define HOST_DELIMITER ";"
#define IP_PORT_DELIMITER ":"

/**
 ******************************************************************************************************
 * Returns data for a particular request string to AerospikeClient_InfoNode
 *
 * @param self AerospikeClient object
 * @param request_str_p Request string sent from the python client
 * @param py_host Optional host sent from the python client
 * @param py_policy The policy sent from the python client
 *
 * Returns information about a host.
 ********************************************************************************************************/
static PyObject * AerospikeClient_InfoNode_Invoke(
	AerospikeClient * self,
	char* request_str_p, PyObject * py_host, PyObject * py_policy) {

	PyObject * py_response = NULL;
	as_policy_info              info_policy;
	as_policy_info*             info_policy_p = NULL;
	char*                       address = (char *) self->as->config.hosts[0].addr;
	long                        port_no = self->as->config.hosts[0].port;
	char*                       response_p = NULL;
	PyObject *key_op = NULL, *value = NULL;

	as_error err;
	as_error_init(&err);

	if (py_policy) {
		if( PyDict_Check(py_policy) ) {
			pyobject_to_policy_info(&err, py_policy, &info_policy, &info_policy_p);
			if (err.code != AEROSPIKE_OK) {
				goto CLEANUP;
			}
		} else {
			as_error_update(&err, AEROSPIKE_ERR_PARAM, "Policy should be a dictionary");
			goto CLEANUP;
		}
	}

	if ( py_host ) {
		if ( PyDict_Check(py_host) ) {
			Py_ssize_t pos = 0;
			while (PyDict_Next(py_host, &pos, &key_op, &value)) {
				if ( ! PyString_Check(key_op) ) {
					as_error_update(&err, AEROSPIKE_ERR_PARAM, "Hosts key must be a string.");
					goto CLEANUP;
				} else {
					char * name = PyString_AsString(key_op);
					if(!strcmp(name,"addr") && (PyString_Check(value))) {
						address =  PyString_AsString(value);
					} else if (!strcmp(name,"port") && (PyLong_Check(value) || PyInt_Check(value))) {
						port_no = PyLong_AsLong(value);
					} else {
						as_error_update(&err, AEROSPIKE_ERR_PARAM, "Hosts dictionary incorrect");
						goto CLEANUP;
					}
				}
			}
		} else {
			as_error_update(&err, AEROSPIKE_ERR_PARAM, "Hosts should be a dictionary");
			goto CLEANUP;
		}
	}
	aerospike_info_host(self->as, &err, info_policy_p,
		(const char *) address, (uint16_t) port_no, request_str_p,
		&response_p);
	if( err.code == AEROSPIKE_OK ) {
		if (response_p)	{
			py_response = PyString_FromString(response_p);
			free(response_p);
		} else {
			as_error_update(&err, AEROSPIKE_ERR_CLIENT, "Info operation failed");
			goto CLEANUP;
		}
	} else {
		goto CLEANUP;
	}

CLEANUP:

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		Py_DECREF(py_err);
		return NULL;
	}
	return py_response;
}

/**
 ******************************************************************************************************
 * Returns data about a particular node in the database depending upon the request string.
 *
 * @param self AerospikeClient object
 * @param args The args is a tuple object containing an argument
 * list passed from Python to a C function
 * @param kwds Dictionary of keywords
 *
 * Returns information about a host.
 ********************************************************************************************************/
PyObject * AerospikeClient_InfoNode(AerospikeClient * self, PyObject * args, PyObject * kwds)
{
	PyObject * py_host = NULL;
	PyObject * py_policy = NULL;

	char* request = NULL;

	static char * kwlist[] = {"req", "host", "policy", NULL};

	if ( PyArg_ParseTupleAndKeywords(args, kwds, "s|OO:info", kwlist, &request, &py_host, &py_policy) == false ) {
		return NULL;
	}

	return AerospikeClient_InfoNode_Invoke(self, request, py_host, py_policy);

}
/**
 ******************************************************************************************************
 * Iterates over the hosts in the cluster and creates the list to be returned to the python client.
 *
 * @param err as_error object
 * @param command Request string sent from the python client
 * @param nodes_dict Dictionary containing details of each host
 * @param return_value List t o be returned back to the python client
 * @param host_index Index of the dictionary nodes_dict
 * @param index Index of the list to be returned.
 *
 * Returns information about a host.
 ********************************************************************************************************/
static PyObject * AerospikeClient_GetNodes_Returnlist(as_error* err,
	PyObject * command, PyObject * nodes_dict[], PyObject * return_value,
	uint32_t host_index, Py_ssize_t index) {

	char* tok = NULL;
	char* saved = NULL;
	PyObject * value_tok = NULL;
	bool break_flag = false;

	tok = strtok_r(PyString_AsString(command), INFO_REQUEST_RESPONSE_DELIMITER, &saved);
	if (tok == NULL) {
		as_error_update(err, AEROSPIKE_ERR_CLIENT, "Unable to get addr in service");
		goto CLEANUP;
	}
	while (tok != NULL && (host_index < MAX_HOST_COUNT)) {
		tok = strtok_r(NULL, IP_PORT_DELIMITER, &saved);
		if (tok == NULL) {
			goto CLEANUP;
		}

		nodes_dict[host_index] = PyDict_New();

		value_tok = PyString_FromString(tok);
		PyDict_SetItemString(nodes_dict[host_index], "addr" , value_tok);
		Py_DECREF(value_tok);

		if(strcmp(PyString_AsString(command),"response_services_p")) {
			tok = strtok_r(NULL, HOST_DELIMITER, &saved);
			if (tok == NULL) {
				as_error_update(err, AEROSPIKE_ERR_CLIENT, "Unable to get port");
				goto CLEANUP;
			}

			if (strstr(tok, INFO_RESPONSE_END)) {
				tok = strtok_r(tok, INFO_RESPONSE_END, &saved);
				break_flag = true;
			}
		} else {
			tok = strtok_r(NULL, INFO_RESPONSE_END, &saved);
			if (tok == NULL) {
				as_error_update(err, AEROSPIKE_ERR_CLIENT, "Unable to get port in service");
				goto CLEANUP;
			}
		}

		value_tok = PyString_FromString(tok);
		PyDict_SetItemString(nodes_dict[host_index], "port" , value_tok);
		Py_DECREF(value_tok);
		PyList_Insert(return_value, index , nodes_dict[host_index]);
		Py_DECREF(nodes_dict[host_index]);
		index++;
		host_index++;

		if (break_flag == true) {
			goto CLEANUP;
		}

	}
CLEANUP:

	if ( err->code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		Py_DECREF(py_err);
		return NULL;
	}
	return return_value;
}
/**
 ******************************************************************************************************
 * Returns data about the nodes to AerospikeClient_GetNodes.
 *
 * @param self AerospikeClient object
 *
 * Returns a list containing the details of the nodes.
 ********************************************************************************************************/
static PyObject * AerospikeClient_GetNodes_Invoke(
	AerospikeClient * self) {

	PyObject * response_services_p = NULL;
	PyObject * response_service_p = NULL;
	PyObject * nodes_dict[MAX_HOST_COUNT] = {0};
	PyObject * return_value = PyList_New(0);

	as_error err;
	as_error_init(&err);

	response_services_p = AerospikeClient_InfoNode_Invoke(self, "services", NULL, NULL);
	if(!response_services_p) {
		as_error_update(&err, AEROSPIKE_ERR_CLIENT, "Services call returned an error");
		goto CLEANUP;
	}

	response_service_p = AerospikeClient_InfoNode_Invoke(self, "service", NULL, NULL);
	if(!response_service_p) {
		as_error_update(&err, AEROSPIKE_ERR_CLIENT, "Service call returned an error");
		goto CLEANUP;
	}

	return_value = AerospikeClient_GetNodes_Returnlist(&err, response_service_p, nodes_dict, return_value, 0, 0);
	if( return_value )
		return_value = AerospikeClient_GetNodes_Returnlist(&err, response_services_p, nodes_dict, return_value, 1, 1);

CLEANUP:

	if(response_services_p) {
		Py_DECREF(response_services_p);
	}

	if(response_service_p) {
		Py_DECREF(response_service_p);
	}

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		Py_DECREF(py_err);
		return NULL;
	}

	return return_value;
}

/**
 ******************************************************************************************************
 * Returns data about the nodes in a cluster of the database.
 *
 * @param self AerospikeClient object
 * @param args The args is a tuple object containing an argument
 * list passed from Python to a C function
 * @param kwds Dictionary of keywords
 *
 * Returns a list containing the details of the nodes.
 ********************************************************************************************************/
PyObject * AerospikeClient_GetNodes(AerospikeClient * self, PyObject * args, PyObject * kwds)
{
	return AerospikeClient_GetNodes_Invoke(self);
}
