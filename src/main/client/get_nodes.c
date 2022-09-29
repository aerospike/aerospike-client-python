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

#include <aerospike/aerospike_info.h>
#include <aerospike/as_key.h>
#include <aerospike/as_error.h>
#include <aerospike/as_node.h>
#include <aerospike/as_cluster.h>

#include "client.h"
#include "conversions.h"
#include "exceptions.h"
#include "policy.h"

static char *get_unbracketed_ip_and_length(char *ip_start, char *split_point,
										   int *length);

/**
 ******************************************************************************************************
	* Returns data about the nodes to AerospikeClient_GetNodes.
	*
	* @param self                  AerospikeClient object
	*
	* Returns a list containing the IP and port tuple of each node.
	********************************************************************************************************/
static PyObject *AerospikeClient_GetNodes_Invoke(AerospikeClient *self)
{

	PyObject *py_hostname = NULL;
	PyObject *py_port = NULL;
	PyObject *return_value = PyList_New(0);

	as_nodes *nodes = NULL;
	char *hostname = NULL;
	char *split_point = NULL;

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

	aerospike *as = self->as;
	as_cluster *cluster = as->cluster;
	// If the cluster goes down between the last call and this call, this could theoretically occur.
	if (!cluster) {
		as_error_update(&err, AEROSPIKE_ERR_CLUSTER,
						"invalid aerospike cluster");
		goto CLEANUP;
	}

	nodes = as_nodes_reserve(cluster);

	if (!nodes) {
		as_error_update(&err, AEROSPIKE_ERR_CLUSTER, "Cluster is empty");
		goto CLEANUP;
	}

	for (uint32_t i = 0; i < nodes->size; i++) {
		as_node *node = nodes->array[i];
		hostname = (char *)as_node_get_address_string(node);
		split_point = strrchr(hostname, ':');
		if (!split_point) {
			as_error_update(&err, AEROSPIKE_ERR_CLIENT,
							"Malformed host name string");
			goto CLEANUP;
		}

		char *real_hostname_start = NULL;
		int real_length;
		/* Since hostname might be '[::1]' and we want to return only '::1' , we need to set
			* a pointer
			*/
		real_hostname_start =
			get_unbracketed_ip_and_length(hostname, split_point, &real_length);
		Py_ssize_t py_host_length = (Py_ssize_t)real_length;
		py_hostname =
			PyString_FromStringAndSize(real_hostname_start, py_host_length);

		if (!py_hostname) {
			as_error_update(&err, AEROSPIKE_ERR_CLIENT,
							"Failed to create python hostname");
			goto CLEANUP;
		}

		// convert "3000" -> 3000, using base 10 | use long since it works in 2 & 3
		py_port = PyLong_FromString(split_point + 1, NULL, 10);
		if (!py_port || PyErr_Occurred()) {
			// py_port exists
			Py_XDECREF(py_hostname);
			as_error_update(&err, AEROSPIKE_ERR_CLIENT,
							"Non numeric port found");
			goto CLEANUP;
		}
		PyObject *py_host_pair = Py_BuildValue("OO", py_hostname, py_port);

		Py_XDECREF(py_port);
		Py_XDECREF(py_hostname);

		if (!py_host_pair) {
			as_error_update(&err, AEROSPIKE_ERR_CLIENT,
							"Failed to build node info tuple");
			goto CLEANUP;
		}

		PyList_Append(return_value, py_host_pair);
		Py_DECREF(py_host_pair);
	}
CLEANUP:
	if (nodes) {
		as_nodes_release(nodes);
	}
	if (err.code != AEROSPIKE_OK) {
		// Clear the return value if it exists
		Py_XDECREF(return_value);
		PyObject *py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyObject *exception_type = raise_exception(&err);
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);

		return NULL;
	}

	return return_value;
}

/******************************************************************************************************
 * Returns data about the nodes in a cluster of the database.
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns a list containing the IP and port tuple of each node.
 ********************************************************************************************************/
PyObject *AerospikeClient_GetNodes(AerospikeClient *self, PyObject *args,
								   PyObject *kwds)
{
	return AerospikeClient_GetNodes_Invoke(self);
}

/**
 ******************************************************************************************************
	* Returns data about the nodes to AerospikeClient_GetNodeNames.
	*
	* @param self                  AerospikeClient object
	*
	* Returns a list containing the IP, port, name dict of each node.
	********************************************************************************************************/
static PyObject *AerospikeClient_GetNodeNames_Invoke(AerospikeClient *self)
{

	PyObject *py_node_name = NULL;
	PyObject *py_hostname = NULL;
	PyObject *py_port = NULL;
	PyObject *py_return_dict = NULL;
	PyObject *return_value = PyList_New(0);

	as_nodes *nodes = NULL;
	char *hostname = NULL;
	char *split_point = NULL;

	as_error err;
	as_error_init(&err);

	if (!self || !self->as) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object.");
		goto CLEANUP;
	}

	if (!self->is_conn_16) {
		as_error_update(&err, AEROSPIKE_ERR_CLUSTER,
						"No connection to aerospike cluster.");
		goto CLEANUP;
	}

	aerospike *as = self->as;
	as_cluster *cluster = as->cluster;
	// If the cluster goes down between the last call and this call, this could theoretically occur.
	if (!cluster) {
		as_error_update(&err, AEROSPIKE_ERR_CLUSTER,
						"invalid aerospike cluster.");
		goto CLEANUP;
	}

	nodes = as_nodes_reserve(cluster);

	if (!nodes) {
		as_error_update(&err, AEROSPIKE_ERR_CLUSTER, "Cluster is empty.");
		goto CLEANUP;
	}

	for (uint32_t i = 0; i < nodes->size; i++) {
		as_node *node = nodes->array[i];
		hostname = (char *)as_node_get_address_string(node);
		split_point = strrchr(hostname, ':');
		if (!split_point) {
			as_error_update(&err, AEROSPIKE_ERR_CLIENT,
							"Malformed host name string.");
			goto CLEANUP;
		}

		char *real_hostname_start = NULL;
		int real_length;
		/* Since hostname might be '[::1]' and we want to return only '::1' , we need to set
			* a pointer
			*/
		real_hostname_start =
			get_unbracketed_ip_and_length(hostname, split_point, &real_length);
		Py_ssize_t py_host_length = (Py_ssize_t)real_length;
		py_hostname =
			PyUnicode_FromStringAndSize(real_hostname_start, py_host_length);

		if (!py_hostname) {
			as_error_update(&err, AEROSPIKE_ERR_CLIENT,
							"Failed to create python hostname.");
			goto CLEANUP;
		}

		// convert "3000" -> 3000, using base 10 | use long since it works in 2 & 3
		py_port = PyLong_FromString(split_point + 1, NULL, 10);
		if (!py_port || PyErr_Occurred()) {
			as_error_update(&err, AEROSPIKE_ERR_CLIENT,
							"Non numeric port found.");
			goto CLEANUP;
		}

		py_node_name = PyUnicode_FromString(node->name);
		if (py_node_name == NULL) {
			as_error_update(&err, AEROSPIKE_ERR_CLIENT,
							"Failed to get node name.");
			goto CLEANUP;
		}

		const char *hostname_key = "address";
		const char *port_key = "port";
		const char *node_name_key = "node_name";
		py_return_dict = PyDict_New();
		if (!py_return_dict) {
			as_error_update(&err, AEROSPIKE_ERR_CLIENT,
							"Failed to build node info dictionary.");
			goto CLEANUP;
		}

		if (PyDict_SetItemString(py_return_dict, hostname_key, py_hostname) ==
				-1 ||
			PyDict_SetItemString(py_return_dict, port_key, py_port) == -1 ||
			PyDict_SetItemString(py_return_dict, node_name_key, py_node_name) ==
				-1) {
			as_error_update(&err, AEROSPIKE_ERR_CLIENT,
							"Failed to add dictionary item.");
			goto CLEANUP;
		}

		if (PyList_Append(return_value, py_return_dict) == -1) {
			as_error_update(&err, AEROSPIKE_ERR_CLIENT,
							"Failed to append py_return_dict to return_value.");
			goto CLEANUP;
		}
	}
CLEANUP:
	if (nodes) {
		as_nodes_release(nodes);
	}

	Py_XDECREF(py_port);
	Py_XDECREF(py_hostname);
	Py_XDECREF(py_node_name);
	Py_XDECREF(py_return_dict);

	if (err.code != AEROSPIKE_OK) {
		// Clear the return value if it exists
		Py_XDECREF(return_value);
		PyObject *py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyObject *exception_type = raise_exception(&err);
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);

		return NULL;
	}

	return return_value;
}

/******************************************************************************************************
 * Returns data about the nodes in a cluster of the database.
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns a list containing the IP, port, name dict of each node.
 ********************************************************************************************************/
PyObject *AerospikeClient_GetNodeNames(AerospikeClient *self, PyObject *args,
									   PyObject *kwds)
{
	return AerospikeClient_GetNodeNames_Invoke(self);
}

/*
 * This will return the correct starting point of an ipv6 address wrapped in [] and the length of the actual ip
 * split_point is the address of the rightmost : in the ip_start string
 * so something like ip_start = "[::1]:3000"
 * where ip_start is 0xf0 and split_point is 0xf5
 * len will be set to 3 and the return value will be (char*)0xf1
 *
 * If this is ipv4, length is the length of the address, and returned value = ip_start
 */
static char *get_unbracketed_ip_and_length(char *ip_start, char *split_point,
										   int *length)
{
	*length = split_point - ip_start;
	if (*length < 2) {
		return ip_start;
	}
	if (ip_start[0] == '[' && ip_start[*length - 1] == ']') {
		*length = *length - 2;
		return (char *)(ip_start + 1);
	}
	return ip_start;
}