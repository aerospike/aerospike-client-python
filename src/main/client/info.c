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
#include <aerospike/aerospike_key.h>
#include <aerospike/as_key.h>
#include <aerospike/as_error.h>
#include <aerospike/as_node.h>
#include <aerospike/as_record.h>
#include <aerospike/as_config.h>

#include "client.h"
#include "policy.h"
#include "conversions.h"
#include "exceptions.h"
#include <arpa/inet.h>

#include "tls_info_host.h"

typedef struct foreach_callback_info_udata_t {
	PyObject *udata_p;
	PyObject *host_lookup_p;
	as_error error;
} foreach_callback_info_udata;

static PyObject *AerospikeClient_InfoAll_Invoke(AerospikeClient *self,
												PyObject *py_request,
												PyObject *py_policy);

static PyObject *get_formatted_info_response(const char *response);
/**
 ********************************************************************************************************
 * Macros for Info API.
 ********************************************************************************************************
 */
#define INET_ADDRSTRLEN 16
#define INET6_ADDRSTRLEN 46
#define INET_PORT 5
#define IP_PORT_SEPARATOR_LEN 1
// Add 2 to the length to facilitate [] around the edges
#define IP_PORT_MAX_LEN INET6_ADDRSTRLEN + INET_PORT + IP_PORT_SEPARATOR_LEN + 2

/**
 *******************************************************************************************************
 * Callback for as_info_foreach().
 *
 * @param err                   The as_error to be populated by the function
 *                              with the encountered error if any.
 * @param node                  The current as_node object for which the
 *                              callback is fired by c client.
 * @param req                   The info request string.
 * @param res                   The info response string for current node.
 * @pram udata                  The callback udata containing the host_lookup
 *                              array and the return zval to be populated with
 *                              an entry for current node's info response with
 *                              the node's ID as the key.
 *
 * Returns true if callback is successful, Otherwise false.
 *******************************************************************************************************
 */
static bool AerospikeClient_Info_each(as_error *err, const as_node *node,
									  const char *req, char *res, void *udata)
{
	PyObject *py_err = NULL;
	PyObject *py_ustr = NULL;
	PyObject *py_out = NULL;
	foreach_callback_info_udata *udata_ptr =
		(foreach_callback_info_udata *)udata;
	as_address *addr = NULL;

	// Need to make sure we have the GIL since we're back in python land now
	PyGILState_STATE gil_state = PyGILState_Ensure();

	if (err && err->code != AEROSPIKE_OK) {
		as_error_update(err, err->code, NULL);
		goto CLEANUP;
	}
	else if (res) {
		char *out = strchr(res, '\t');
		if (out) {
			out++;
			py_out = PyString_FromString(out);
		}
		else {
			py_out = PyString_FromString(res);
		}
	}

	if (!py_err) {
		Py_INCREF(Py_None);
		py_err = Py_None;
	}

	if (!py_out) {
		Py_INCREF(Py_None);
		py_out = Py_None;
	}

	PyObject *py_res = PyTuple_New(2);
	PyTuple_SetItem(py_res, 0, py_err);
	PyTuple_SetItem(py_res, 1, py_out);

	if (udata_ptr->host_lookup_p) {
		PyObject *py_hosts = (PyObject *)udata_ptr->host_lookup_p;
		if (py_hosts && PyList_Check(py_hosts)) {
			addr = as_node_get_address((as_node *)node);
			int size = (int)PyList_Size(py_hosts);
			for (int i = 0; i < size; i++) {
				char *host_addr = NULL;
				int port = -1;
				PyObject *py_host = PyList_GetItem(py_hosts, i);
				if (PyTuple_Check(py_host) && PyTuple_Size(py_host) == 2) {
					PyObject *py_addr = PyTuple_GetItem(py_host, 0);
					PyObject *py_port = PyTuple_GetItem(py_host, 1);
					if (PyUnicode_Check(py_addr)) {
						py_ustr = PyUnicode_AsUTF8String(py_addr);
						host_addr = PyBytes_AsString(py_ustr);
					}
					else if (PyString_Check(py_addr)) {
						host_addr = PyString_AsString(py_addr);
					}
					else {
						as_error_update(&udata_ptr->error, AEROSPIKE_ERR_PARAM,
										"Host address is of type incorrect");
						if (py_res) {
							Py_DECREF(py_res);
						}
						PyGILState_Release(gil_state);
						return false;
					}
					if (PyInt_Check(py_port)) {
						port = (uint16_t)PyInt_AsLong(py_port);
					}
					else if (PyLong_Check(py_port)) {
						port = (uint16_t)PyLong_AsLong(py_port);
					}
					else {
						break;
					}
					char ip_port[IP_PORT_MAX_LEN];
					// If the address is longer than the max length of an ipv6 address, raise an error and exit
					if (strnlen(host_addr, INET6_ADDRSTRLEN) >=
						INET6_ADDRSTRLEN) {
						as_error_update(&udata_ptr->error, AEROSPIKE_ERR_PARAM,
										"Host address is too long");
						if (py_res) {
							Py_DECREF(py_res);
						}
						goto CLEANUP;
					}
					sprintf(ip_port, "%s:%d", host_addr, port);
					if (!strcmp(ip_port, addr->name)) {
						PyObject *py_nodes = (PyObject *)udata_ptr->udata_p;
						PyDict_SetItemString(py_nodes, node->name, py_res);
					}
					else {
						sprintf(ip_port, "[%s]:%d", host_addr, port);
						if (!strcmp(ip_port, addr->name)) {
							PyObject *py_nodes = (PyObject *)udata_ptr->udata_p;
							PyDict_SetItemString(py_nodes, node->name, py_res);
						}
					}
				}

				if (py_ustr) {
					Py_DECREF(py_ustr);
					py_ustr = NULL;
				}
			}
		}
		else if (!PyList_Check(py_hosts)) {
			as_error_update(&udata_ptr->error, AEROSPIKE_ERR_PARAM,
							"Hosts should be specified in a list.");
			goto CLEANUP;
		}
	}
	else {
		PyObject *py_nodes = (PyObject *)udata_ptr->udata_p;
		PyDict_SetItemString(py_nodes, node->name, py_res);
	}

	Py_DECREF(py_res);

CLEANUP:
	if (py_ustr) {
		Py_DECREF(py_ustr);
	}
	if (udata_ptr->error.code != AEROSPIKE_OK) {
		PyObject *py_err = NULL;
		error_to_pyobject(&udata_ptr->error, &py_err);
		PyObject *exception_type = raise_exception(&udata_ptr->error);
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		PyGILState_Release(gil_state);
		return NULL;
	}
	if (err->code != AEROSPIKE_OK) {
		PyObject *py_err = NULL;
		error_to_pyobject(err, &py_err);
		PyObject *exception_type = raise_exception(err);
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		PyGILState_Release(gil_state);
		return NULL;
	}

	PyGILState_Release(gil_state);
	return true;
}

/**
 *******************************************************************************************************
 * Callback for as_info_foreach(). Used by aerospike.info_all
 *
 * @param err                   The as_error to be populated by the function
 *                              with the encountered error if any.
 * @param node                  The current as_node object for which the
 *                              callback is fired by c client.
 * @param req                   The info request string.
 * @param res                   The info response string for current node.
 * @pram udata                  Data containing an err entry, and a dictionary to be filled with respones
 *                              an entry for current node's info response with
 *                              the node's ID as the key.
 *
 * Returns true if callback is successful, Otherwise false.
 *******************************************************************************************************
 */

static bool AerospikeClient_InfoAll_each(as_error *err, const as_node *node,
										 const char *req, char *res,
										 void *udata)
{
	PyObject *py_err = NULL;
	PyObject *py_ustr = NULL;
	PyObject *py_out = NULL;
	foreach_callback_info_udata *udata_ptr =
		(foreach_callback_info_udata *)udata;

	// Need to make sure we have the GIL since we're back in python land now
	PyGILState_STATE gil_state = PyGILState_Ensure();

	if (err && err->code != AEROSPIKE_OK) {
		as_error_update(err, err->code, NULL);
		goto CLEANUP;
	}

	py_out = get_formatted_info_response(res);
	/* Since this is called from aerospike_info_foreach, we do not own res, so there is no need to free it */

	if (!py_err) {
		Py_INCREF(Py_None);
		py_err = Py_None;
	}

	PyObject *py_res = PyTuple_New(2);
	PyTuple_SetItem(py_res, 0, py_err);
	PyTuple_SetItem(py_res, 1, py_out);

	PyObject *py_nodes = (PyObject *)udata_ptr->udata_p;
	PyDict_SetItemString(py_nodes, node->name, py_res);

	Py_DECREF(py_res);

CLEANUP:
	if (py_ustr) {
		Py_DECREF(py_ustr);
	}
	if (udata_ptr->error.code != AEROSPIKE_OK) {
		PyObject *py_err = NULL;
		error_to_pyobject(&udata_ptr->error, &py_err);
		PyObject *exception_type = raise_exception(&udata_ptr->error);
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		PyGILState_Release(gil_state);
		return false;
	}
	if (err->code != AEROSPIKE_OK) {
		PyObject *py_err = NULL;
		error_to_pyobject(err, &py_err);
		PyObject *exception_type = raise_exception(err);
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		PyGILState_Release(gil_state);
		return false;
	}

	PyGILState_Release(gil_state);
	return true;
}

/**
 *******************************************************************************************************
 * Sends an info request to all the nodes in a cluster.
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns a server response for the particular request string.
 * In case of error,appropriate exceptions will be raised.
 *******************************************************************************************************
 */
PyObject *AerospikeClient_Info(AerospikeClient *self, PyObject *args,
							   PyObject *kwds)
{
	PyObject *py_req = NULL;
	PyObject *py_policy = NULL;
	PyObject *py_hosts = NULL;
	PyObject *py_nodes = NULL;
	PyObject *py_ustr = NULL;
	foreach_callback_info_udata info_callback_udata;

	static char *kwlist[] = {"command", "hosts", "policy", NULL};

	if (PyArg_ParseTupleAndKeywords(args, kwds, "O|OO:info", kwlist, &py_req,
									&py_hosts, &py_policy) == false) {
		return NULL;
	}

	as_error err;
	as_error_init(&err);

	as_policy_info info_policy;
	as_policy_info *info_policy_p = NULL;
	py_nodes = PyDict_New();
	info_callback_udata.udata_p = py_nodes;
	info_callback_udata.host_lookup_p = py_hosts;
	as_error_init(&info_callback_udata.error);

	if (!self || !self->as) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
		goto CLEANUP;
	}
	if (!self->is_conn_16) {
		as_error_update(&err, AEROSPIKE_ERR_CLUSTER,
						"No connection to aerospike cluster");
		goto CLEANUP;
	}

	// Convert python policy object to as_policy_info
	pyobject_to_policy_info(&err, py_policy, &info_policy, &info_policy_p,
							&self->as->config.policies.info);
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}
	char *req = NULL;
	if (PyUnicode_Check(py_req)) {
		py_ustr = PyUnicode_AsUTF8String(py_req);
		req = PyBytes_AsString(py_ustr);
	}
	else if (PyString_Check(py_req)) {
		req = PyString_AsString(py_req);
	}
	else {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Request must be a string");
		goto CLEANUP;
	}

	Py_BEGIN_ALLOW_THREADS
	aerospike_info_foreach(
		self->as, &err, info_policy_p, req,
		(aerospike_info_foreach_callback)AerospikeClient_Info_each,
		&info_callback_udata);
	Py_END_ALLOW_THREADS

	if (info_callback_udata.error.code != AEROSPIKE_OK) {
		as_error_update(&err, err.code, NULL);
		goto CLEANUP;
	}
CLEANUP:
	if (py_ustr) {
		Py_DECREF(py_ustr);
	}
	if (info_callback_udata.error.code != AEROSPIKE_OK) {
		PyObject *py_err = NULL;
		error_to_pyobject(&info_callback_udata.error, &py_err);
		PyObject *exception_type = raise_exception(&info_callback_udata.error);
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		if (py_nodes) {
			Py_DECREF(py_nodes);
		}
		return NULL;
	}
	if (err.code != AEROSPIKE_OK) {
		PyObject *py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyObject *exception_type = raise_exception(&err);
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		if (py_nodes) {
			Py_DECREF(py_nodes);
		}
		return NULL;
	}

	return info_callback_udata.udata_p;
}

/**
 *******************************************************************************************************
 * Sends an info request to all the nodes in a cluster.
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns a server response for the particular request string.
 * In case of error,appropriate exceptions will be raised.
 * py_hosts should be null at this point
 *******************************************************************************************************
 */
PyObject *AerospikeClient_InfoAll(AerospikeClient *self, PyObject *args,
								  PyObject *kwds)
{
	PyObject *py_req = NULL;
	PyObject *py_policy = NULL;

	static char *kwlist[] = {"command", "policy", NULL};

	if (PyArg_ParseTupleAndKeywords(args, kwds, "O|O:info_all", kwlist, &py_req,
									&py_policy) == false) {
		return NULL;
	}

	return AerospikeClient_InfoAll_Invoke(self, py_req, py_policy);
}

static PyObject *AerospikeClient_InfoAll_Invoke(AerospikeClient *self,
												PyObject *py_request,
												PyObject *py_policy)
{
	PyObject *py_nodes = NULL;
	PyObject *py_ustr = NULL;
	foreach_callback_info_udata info_callback_udata;

	as_error err;
	as_error_init(&err);

	as_policy_info info_policy;
	as_policy_info *info_policy_p = NULL;
	py_nodes = PyDict_New();
	info_callback_udata.udata_p = py_nodes;
	info_callback_udata.host_lookup_p = NULL;
	as_error_init(&info_callback_udata.error);

	if (!self || !self->as) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
		goto CLEANUP;
	}
	if (!self->is_conn_16) {
		as_error_update(&err, AEROSPIKE_ERR_CLUSTER,
						"No connection to aerospike cluster");
		goto CLEANUP;
	}

	// Convert python policy object to as_policy_info
	pyobject_to_policy_info(&err, py_policy, &info_policy, &info_policy_p,
							&self->as->config.policies.info);
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	char *request = NULL;
	if (PyUnicode_Check(py_request)) {
		py_ustr = PyUnicode_AsUTF8String(py_request);
		request = PyBytes_AsString(py_ustr);
	}
	else if (PyString_Check(py_request)) {
		request = PyString_AsString(py_request);
	}
	else {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Request must be a string");
		goto CLEANUP;
	}

	Py_BEGIN_ALLOW_THREADS
	aerospike_info_foreach(
		self->as, &err, info_policy_p, request,
		(aerospike_info_foreach_callback)AerospikeClient_InfoAll_each,
		&info_callback_udata);
	Py_END_ALLOW_THREADS

	if (info_callback_udata.error.code != AEROSPIKE_OK) {
		as_error_update(&err, err.code, NULL);
		goto CLEANUP;
	}
CLEANUP:
	if (py_ustr) {
		Py_DECREF(py_ustr);
	}
	if (info_callback_udata.error.code != AEROSPIKE_OK) {
		PyObject *py_err = NULL;
		error_to_pyobject(&info_callback_udata.error, &py_err);
		PyObject *exception_type = raise_exception(&info_callback_udata.error);
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		if (py_nodes) {
			Py_DECREF(py_nodes);
		}
		return NULL;
	}
	if (err.code != AEROSPIKE_OK) {
		PyObject *py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyObject *exception_type = raise_exception(&err);
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		if (py_nodes) {
			Py_DECREF(py_nodes);
		}
		return NULL;
	}

	return info_callback_udata.udata_p;
}

/*
 * Generally a response is of the form: request\tresponse
 * this returns the response portion only. If response is null, returns Py_None
 * This returns either a new reference, or Py_None with Py_None's reference count increased by 1
 */
static PyObject *get_formatted_info_response(const char *response)
{
	PyObject *py_response = NULL;
	char *formatted_output = NULL;

	if (response) {
		/* Remove the echoed request from the start of the response */
		formatted_output = strchr(response, '\t');
		if (formatted_output) {
			/* Advance one character past the '\t' */
			formatted_output++;
			py_response = PyString_FromString(formatted_output);
		}
		else {
			py_response = PyString_FromString(response);
		}
	}
	else {
		Py_INCREF(Py_None);
		py_response = Py_None;
	}

	return py_response;
}
