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
#include <aerospike/as_partition.h>
#include <aerospike/as_cluster.h>

#include "client.h"
#include "conversions.h"
#include "exceptions.h"
#include "policy.h"

PyObject *AerospikeClient_Get_Key_PartitionID_Invoke(AerospikeClient *self,
													 PyObject *py_ns,
													 PyObject *py_set,
													 PyObject *py_key)
{
	// Python Return Value
	PyObject *py_keydict = NULL;
	PyObject *py_value = NULL;

	// Aerospike Client Arguments
	as_error err;
	as_key key;
	as_digest *digest;

	// Initialised flags
	bool key_initialised = false;
	// Initialize error
	as_error_init(&err);

	if (!PyUnicode_Check(py_ns)) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM,
						"Namespace should be a string.");
		goto CLEANUP;
	}
	if (!PyUnicode_Check(py_set)) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM,
						"Set should be a string or unicode.");
		goto CLEANUP;
	}
	if (!PyUnicode_Check(py_key) && !PyLong_Check(py_key) &&
		!PyByteArray_Check(py_key)) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Key is invalid.");
		goto CLEANUP;
	}

	if (!self || !self->as) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
		goto CLEANUP;
	}

	if (!self->is_conn_16) {
		as_error_update(&err, AEROSPIKE_ERR_CLUSTER,
						"No connection to aerospike cluster");
		goto CLEANUP;
	}

	py_keydict = PyDict_New();
	if (PyDict_SetItemString(py_keydict, "ns", py_ns) == -1) {
		as_error_update(&err, AEROSPIKE_ERR_CLIENT,
						"Failed to add dictionary item ns.");
		goto CLEANUP;
	}
	if (PyDict_SetItemString(py_keydict, "set", py_set) == -1) {
		as_error_update(&err, AEROSPIKE_ERR_CLIENT,
						"Failed to add dictionary item set.");
		goto CLEANUP;
	}
	if (PyDict_SetItemString(py_keydict, "key", py_key) == -1) {
		as_error_update(&err, AEROSPIKE_ERR_CLIENT,
						"Failed to add dictionary item key.");
		goto CLEANUP;
	}

	// Convert python key object to as_key
	pyobject_to_key(&err, py_keydict, &key);
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}
	// Key is successfully initialised.
	key_initialised = true;

	// Invoke operation
	digest = as_key_digest(&key);
	if (!digest->init) {
		as_error_update(&err, AEROSPIKE_ERR_CLIENT,
						"Digest could not be calculated");
		goto CLEANUP;
	}

	uint32_t id =
		as_partition_getid(key.digest.value, self->as->cluster->n_partitions);
	py_value = PyLong_FromLong(id);

	if (key_initialised == true) {
		// Destroy key only if it is initialised.
		as_key_destroy(&key);
		key_initialised = false;
	}

CLEANUP:
	if (key_initialised == true) {
		// Destroy key only if it is initialised.
		as_key_destroy(&key);
	}
	if (py_keydict) {
		Py_DECREF(py_keydict);
	}

	if (err.code != AEROSPIKE_OK) {
		PyObject *py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyObject *exception_type = raise_exception(&err);
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return NULL;
	}

	return py_value;
}

PyObject *AerospikeClient_Get_Key_PartitionID(AerospikeClient *self,
											  PyObject *args, PyObject *kwds)
{
	// Python Function Arguments
	PyObject *py_ns = NULL;
	PyObject *py_set = NULL;
	PyObject *py_key = NULL;

	// Python Function Keyword Arguments
	static char *kwlist[] = {"ns", "set", "key", NULL};

	// Python Function Argument Parsing
	if (PyArg_ParseTupleAndKeywords(args, kwds, "OOO:get_key_partition_id",
									kwlist, &py_ns, &py_set,
									&py_key) == false) {
		return NULL;
	}

	// Invoke Operation
	return AerospikeClient_Get_Key_PartitionID_Invoke(self, py_ns, py_set,
													  py_key);
}
