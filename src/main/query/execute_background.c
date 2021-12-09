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
#include <pthread.h>
#include <stdbool.h>

#include <aerospike/aerospike_query.h>
#include <aerospike/as_error.h>
#include <aerospike/as_scan.h>

#include "client.h"
#include "conversions.h"
#include "exceptions.h"
#include "policy.h"
#include "query.h"

PyObject *AerospikeQuery_ExecuteBackground(AerospikeQuery *self, PyObject *args,
										   PyObject *kwds)
{
	PyObject *py_policy = NULL;

	as_policy_write write_policy;
	as_policy_write *write_policy_p = NULL;

	uint64_t query_id = 0;

	static char *kwlist[] = {"policy", NULL};

	// For converting expressions.
	as_exp exp_list;
	as_exp *exp_list_p = NULL;

	// For converting predexp.
	as_predexp_list predexp_list;
	as_predexp_list *predexp_list_p = NULL;

	if (PyArg_ParseTupleAndKeywords(args, kwds, "|O:execute_background", kwlist,
									&py_policy) == false) {
		return NULL;
	}

	as_error err;
	as_error_init(&err);

	if (!self || !self->client->as) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
		goto CLEANUP;
	}
	if (!self->client->is_conn_16) {
		as_error_update(&err, AEROSPIKE_ERR_CLUSTER,
						"No connection to aerospike cluster");
		goto CLEANUP;
	}

	if (pyobject_to_policy_write(
			self->client, &err, py_policy, &write_policy, &write_policy_p,
			&self->client->as->config.policies.write, &predexp_list,
			&predexp_list_p, &exp_list, &exp_list_p) != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	Py_BEGIN_ALLOW_THREADS
	aerospike_query_background(self->client->as, &err, write_policy_p,
							   &self->query, &query_id);
	Py_END_ALLOW_THREADS

CLEANUP:

	if (exp_list_p) {
		as_exp_destroy(exp_list_p);
		;
	}

	if (predexp_list_p) {
		as_predexp_list_destroy(&predexp_list);
	}

	if (err.code != AEROSPIKE_OK) {
		PyObject *py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyObject *exception_type = raise_exception(&err);
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return NULL;
	}

	return PyLong_FromUnsignedLongLong(query_id);
}
