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
#include <aerospike/as_config.h>

#include "client.h"
#include "conversions.h"
#include "policy.h"
#include <arpa/inet.h>

typedef struct foreach_callback_info_udata_t {
	PyObject       *udata_p;
	PyObject       *host_lookup_p;
	as_error 		error;
} foreach_callback_info_udata;

#define INET_ADDRSTRLEN 16
#define INET6_ADDRSTRLEN 46
#define INET_PORT 5
#define IP_PORT_SEPARATOR_LEN 1
#define IP_PORT_MAX_LEN INET6_ADDRSTRLEN + INET_PORT + IP_PORT_SEPARATOR_LEN

static bool AerospikeClient_Info_each(const as_error * err, const as_node * node, const char * req, char * res, void * udata)
{
	PyObject * py_err = NULL;
	PyObject * py_ustr = NULL;
	PyObject * py_out = NULL;
	foreach_callback_info_udata*        udata_ptr = (foreach_callback_info_udata *) udata;
	struct sockaddr_in*                 addr = NULL;


	if ( err && err->code != AEROSPIKE_OK ) {
		error_to_pyobject(err, &py_err);
	}
	else if ( res != NULL ) {
		char * out = strchr(res,'\t');
		if ( out != NULL ) {
			out++;
			py_out = PyString_FromString(out);
		}
		else {
			py_out = PyString_FromString(res);
		}
	}

	if ( py_err == NULL ) {
		Py_INCREF(Py_None);
		py_err = Py_None;
	}

	if ( py_out == NULL ) {
		Py_INCREF(Py_None);
		py_out = Py_None;
	}

	PyObject * py_res = PyTuple_New(2);
	PyTuple_SetItem(py_res, 0, py_err);
	PyTuple_SetItem(py_res, 1, py_out);

	if(udata_ptr->host_lookup_p) {
		PyObject * py_hosts = (PyObject *)udata_ptr->host_lookup_p;
		if ( py_hosts && PyList_Check(py_hosts) ) {
			addr = as_node_get_address((as_node *)node);
			int size = (int) PyList_Size(py_hosts);
			for ( int i = 0; i < size && i < AS_CONFIG_HOSTS_SIZE; i++ ) {
				char * host_addr = NULL;
				int port = -1;
				PyObject * py_host = PyList_GetItem(py_hosts, i);
				if ( PyTuple_Check(py_host) && PyTuple_Size(py_host) == 2 ) {
					PyObject * py_addr = PyTuple_GetItem(py_host,0);
					PyObject * py_port = PyTuple_GetItem(py_host,1);
					if (PyUnicode_Check(py_addr)) {
						py_ustr = PyUnicode_AsUTF8String(py_addr);
						host_addr = PyString_AsString(py_ustr);
					} else if ( PyString_Check(py_addr) ) {
						host_addr = PyString_AsString(py_addr);
					} else {
						as_error_update(&udata_ptr->error, AEROSPIKE_ERR_PARAM, "Host address is of type incorrect");
						if (py_res) {
							Py_DECREF(py_res);
						}
						return false;
					}
					if ( PyInt_Check(py_port) ) {
						port = (uint16_t) PyInt_AsLong(py_port);
					}
					else if ( PyLong_Check(py_port) ) {
						port = (uint16_t) PyLong_AsLong(py_port);
					} else {
						break;
					}
					char ip_port[IP_PORT_MAX_LEN];
					inet_ntop(addr->sin_family, &(addr->sin_addr), ip_port, INET_ADDRSTRLEN);
					if( (!strcmp(host_addr,ip_port)) && (port
						== ntohs(addr->sin_port))) {
						PyObject * py_nodes = (PyObject *) udata_ptr->udata_p;
						PyDict_SetItemString(py_nodes, node->name, py_res);
					}
					/*if ( PyString_Check(py_addr) ) {
						host_addr = PyString_AsString(py_addr);
						if ( PyInt_Check(py_port) ) {
							port = (uint16_t) PyInt_AsLong(py_port);
						}
						else if ( PyLong_Check(py_port) ) {
							port = (uint16_t) PyLong_AsLong(py_port);
						} else {
							break;
						}
						char ip_port[IP_PORT_MAX_LEN];
						inet_ntop(addr->sin_family, &(addr->sin_addr), ip_port, INET_ADDRSTRLEN);
						if( (!strcmp(host_addr,ip_port)) && (port
									== ntohs(addr->sin_port))) {
							PyObject * py_nodes = (PyObject *) udata_ptr->udata_p;
							PyDict_SetItemString(py_nodes, node->name, py_res);
						}
					}*/
				}
			}
		} else if ( !PyList_Check( py_hosts )){
			as_error_update(&udata_ptr->error, AEROSPIKE_ERR_PARAM, "Hosts should be specified in a list.");
			goto CLEANUP;
		}
	} else {
		PyObject * py_nodes = (PyObject *) udata_ptr->udata_p;
		PyDict_SetItemString(py_nodes, node->name, py_res);
	}
	Py_DECREF(py_res);
CLEANUP:
	if ( udata_ptr->error.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject( &udata_ptr->error, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		Py_DECREF(py_err);
		return NULL;
	}
	if ( err->code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		Py_DECREF(py_err);
		return NULL;
	}
	return true;
}

PyObject * AerospikeClient_Info(AerospikeClient * self, PyObject * args, PyObject * kwds)
{
	PyObject * py_req = NULL;
	PyObject * py_policy = NULL;
	PyObject * py_hosts = NULL;
	PyObject * py_nodes = NULL;
	PyObject * py_ustr = NULL;
	foreach_callback_info_udata info_callback_udata;

	static char * kwlist[] = {"command", "hosts", "policy", NULL};

	if ( PyArg_ParseTupleAndKeywords(args, kwds, "O|OO:info", kwlist, &py_req, &py_hosts, &py_policy) == false ) {
		return NULL;
	}

	as_error err;
	as_error_init(&err);

	as_policy_info info_policy;
	as_policy_info* info_policy_p = NULL;
	py_nodes = PyDict_New();
	info_callback_udata.udata_p = py_nodes;
	info_callback_udata.host_lookup_p = py_hosts;
	as_error_init(&info_callback_udata.error);
	if (!self || !self->as) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
		goto CLEANUP;
	}
	if (!self->is_conn_16) {
		as_error_update(&err, AEROSPIKE_ERR_CLUSTER, "No connection to aerospike cluster");
		goto CLEANUP;
	}
	// Convert python policy object to as_policy_info
	pyobject_to_policy_info(&err, py_policy, &info_policy, &info_policy_p,
	&self->as->config.policies.info);
	if ( err.code != AEROSPIKE_OK ) {
		goto CLEANUP;
	}
	/*if( PyString_Check(py_req) ) {
		char * req = PyString_AsString(py_req);

		py_nodes = PyDict_New();

		info_callback_udata.udata_p = py_nodes;
		info_callback_udata.host_lookup_p = py_config;

		aerospike_info_foreach(self->as, &err, NULL, req, AerospikeClient_Info_each, &info_callback_udata);
	} else {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Request must be a string");
	}

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		Py_DECREF(py_err);
		if (py_nodes)
			Py_DECREF(py_nodes);
		return NULL;
	}*/
	char * req = NULL;
	if ( PyUnicode_Check(py_req)) {
		py_ustr = PyUnicode_AsUTF8String(py_req);
		req = PyString_AsString(py_ustr);
	} else if( PyString_Check(py_req) ) {
		req = PyString_AsString(py_req);
	} else {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Request must be a string");
		goto CLEANUP;
	}
	aerospike_info_foreach(self->as, &err, info_policy_p, req,
		(aerospike_info_foreach_callback)AerospikeClient_Info_each,
		&info_callback_udata);

	if (&info_callback_udata.error.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}
CLEANUP:
	if (py_ustr) {
		Py_DECREF(py_ustr);
	}
	if ( info_callback_udata.error.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&info_callback_udata.error, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		Py_DECREF(py_err);
		if (py_nodes) {
			Py_DECREF(py_nodes);
		}
		return NULL;
	}
	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		Py_DECREF(py_err);
		if (py_nodes) {
			Py_DECREF(py_nodes);
		}
		return NULL;
	}

	return info_callback_udata.udata_p;
}
