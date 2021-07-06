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

#include "client.h"
#include "query.h"
#include "conversions.h"
#include "exceptions.h"
#include "policy.h"

#include <aerospike/aerospike_query.h>
#include <aerospike/as_job.h>

#define PROGRESS_PCT "progress_pct"
#define RECORDS_READ "records_read"
#define STATUS "status"
/**
 *******************************************************************************************************
 * This function allocates memory to self.
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns self on success.
 * In case of error,appropriate exceptions will be raised.
 *******************************************************************************************************
 */
AerospikeQuery *AerospikeClient_Query(AerospikeClient *self, PyObject *args,
									  PyObject *kwds)
{
	return AerospikeQuery_New(self, args, kwds);
}
static int query_where_add(as_query **query, as_predicate_type predicate,
						   as_index_datatype in_datatype, PyObject *py_bin,
						   PyObject *py_val1, PyObject *py_val2, int index_type,
						   as_error *err)

{
	int64_t min;
	int64_t max;
	char *val = NULL, *bin = NULL;
	PyObject *py_ubin = NULL;
	switch (predicate) {
	case AS_PREDICATE_EQUAL: {
		if (in_datatype == AS_INDEX_STRING) {
			if (PyUnicode_Check(py_bin)) {
				py_ubin = PyUnicode_AsUTF8String(py_bin);
				bin = PyBytes_AsString(py_ubin);
			}
			else if (PyString_Check(py_bin)) {
				bin = PyString_AsString(py_bin);
			}
			else if (PyByteArray_Check(py_bin)) {
				bin = PyByteArray_AsString(py_bin);
			}
			else {
				as_error_update(err, AEROSPIKE_ERR_PARAM,
								"Bin must be a string or unicode");
				return 1;
			}

			if (PyUnicode_Check(py_val1)) {
				val = strdup(PyBytes_AsString(PyUnicode_AsUTF8String(py_val1)));
			}
			else if (PyString_Check(py_val1)) {
				val = strdup(PyString_AsString(py_val1));
			}
			else {
				as_error_update(
					err, AEROSPIKE_ERR_PARAM,
					"Comparison value for string equality must be a string");
				return 1;
			}

			as_query_where_init(*query, 1);
			if (index_type == 0) {
				as_query_where(*query, bin, as_equals(STRING, val));
			}
			else if (index_type == 1) {
				as_query_where(*query, bin, as_contains(LIST, STRING, val));
			}
			else if (index_type == 2) {
				as_query_where(*query, bin, as_contains(MAPKEYS, STRING, val));
			}
			else if (index_type == 3) {
				as_query_where(*query, bin,
							   as_contains(MAPVALUES, STRING, val));
			}
			else {
				as_error_update(err, AEROSPIKE_ERR_PARAM,
								"Invalid query index type");
				return 1;
			}
			if (py_ubin) {
				Py_DECREF(py_ubin);
				py_ubin = NULL;
			}
		}
		else if (in_datatype == AS_INDEX_NUMERIC) {
			if (PyUnicode_Check(py_bin)) {
				py_ubin = PyUnicode_AsUTF8String(py_bin);
				bin = PyBytes_AsString(py_ubin);
			}
			else if (PyString_Check(py_bin)) {
				bin = PyString_AsString(py_bin);
			}
			else if (PyByteArray_Check(py_bin)) {
				bin = PyByteArray_AsString(py_bin);
			}
			else {
				as_error_update(err, AEROSPIKE_ERR_PARAM,
								"Bin must be a string or unicode");
				return 1;
			}
			int64_t val = pyobject_to_int64(py_val1);

			as_query_where_init(*query, 1);
			if (index_type == 0) {
				as_query_where(*query, bin, as_equals(NUMERIC, val));
			}
			else if (index_type == 1) {
				as_query_where(*query, bin, as_contains(LIST, NUMERIC, val));
			}
			else if (index_type == 2) {
				as_query_where(*query, bin, as_contains(MAPKEYS, NUMERIC, val));
			}
			else if (index_type == 3) {
				as_query_where(*query, bin,
							   as_contains(MAPVALUES, NUMERIC, val));
			}
			else {
				as_error_update(err, AEROSPIKE_ERR_PARAM,
								"Invalid query index types");
				return 1;
			}
			if (py_ubin) {
				Py_DECREF(py_ubin);
				py_ubin = NULL;
			}
		}
		else {
			// If it ain't expected, raise and error
			as_error_update(
				err, AEROSPIKE_ERR_PARAM,
				"predicate 'equals' expects a string or integer value.");
			return 1;
		}

		break;
	}
	case AS_PREDICATE_RANGE: {
		if (in_datatype == AS_INDEX_NUMERIC) {
			if (PyUnicode_Check(py_bin)) {
				py_ubin = PyUnicode_AsUTF8String(py_bin);
				bin = PyBytes_AsString(py_ubin);
			}
			else if (PyString_Check(py_bin)) {
				bin = PyString_AsString(py_bin);
			}
			else if (PyByteArray_Check(py_bin)) {
				bin = PyByteArray_AsString(py_bin);
			}
			else {
				as_error_update(err, AEROSPIKE_ERR_PARAM,
								"Bin must be a string or unicode");
				return 1;
			}

			if (py_val1 == Py_None || py_val2 == Py_None) {
				as_error_update(
					err, AEROSPIKE_ERR_PARAM,
					"Min and max must be provided for a range query");
				return 1;
			}
			if (PyInt_Check(py_val1) || PyLong_Check(py_val1)) {
				min = pyobject_to_int64(py_val1);
			}
			else {
				as_error_update(err, AEROSPIKE_ERR_PARAM,
								"Min value must be an integer or long");
				return 1;
			}

			if (PyInt_Check(py_val2) || PyLong_Check(py_val2)) {
				max = pyobject_to_int64(py_val2);
			}
			else {
				as_error_update(err, AEROSPIKE_ERR_PARAM,
								"Max value must be an integer or long");
				return 1;
			}

			as_query_where_init(*query, 1);
			if (index_type == 0) {
				as_query_where(*query, bin,
							   as_range(DEFAULT, NUMERIC, min, max));
			}
			else if (index_type == 1) {
				as_query_where(*query, bin, as_range(LIST, NUMERIC, min, max));
			}
			else if (index_type == 2) {
				as_query_where(*query, bin,
							   as_range(MAPKEYS, NUMERIC, min, max));
			}
			else if (index_type == 3) {
				as_query_where(*query, bin,
							   as_range(MAPVALUES, NUMERIC, min, max));
			}
			else {
				return 1;
			}
			if (py_ubin) {
				Py_DECREF(py_ubin);
				py_ubin = NULL;
			}
		}
		else if (in_datatype == AS_INDEX_STRING) {
			as_error_update(err, AEROSPIKE_ERR_PARAM,
							"Range predicate not supported for strings");
			return 1;
		}
		else {
			// If it ain't right, raise and error
			as_error_update(err, AEROSPIKE_ERR_PARAM,
							"predicate 'between' expects two integer values.");
			return 1;
		}
		break;
	}
	default: {
		// If it ain't supported, raise and error
		as_error_update(err, AEROSPIKE_ERR_PARAM, "unknown predicate type");
		return 1;
	}
	}
	return 0;
}
/**
 * Queries a set in the Aerospike DB and applies UDF on it.
 *
 * @param self                  The c client's aerospike object.
 * @param namespace_p           The namespace to scan.
 * @param set_p                 The set to scan.
 * @param module_p              The name of UDF module containing the
 *                              function to execute.
 * @param function_p            The name of the function to be applied
 *                              to the record.
 * @param py_args               An array of arguments for the UDF.
 * @py_policy                   The optional policy.
 * @py_options                  The optional scan options to set.
 */
static PyObject *AerospikeClient_QueryApply_Invoke(
	AerospikeClient *self, char *namespace_p, PyObject *py_set,
	PyObject *py_predicate, PyObject *py_module, PyObject *py_function,
	PyObject *py_args, PyObject *py_policy, bool block)
{
	as_list *arglist = NULL;
	as_policy_write write_policy;
	as_policy_write *write_policy_p = NULL;
	as_policy_info info_policy;
	as_policy_info *info_policy_p = NULL;
	as_error err;
	as_query query;
	uint64_t query_id = 0;
	bool is_query_init = false;
	int rc = 0;

	PyObject *py_ustr1 = NULL;
	PyObject *py_ustr2 = NULL;
	PyObject *py_ustr3 = NULL;

	// For converting expressions.
	as_exp exp_list;
	as_exp *exp_list_p = NULL;

	// For converting predexp.
	as_predexp_list predexp_list;
	as_predexp_list *predexp_list_p = NULL;

	as_static_pool static_pool;
	memset(&static_pool, 0, sizeof(static_pool));

	// Initialize error
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

	self->is_client_put_serializer = false;

	if (!(namespace_p) || !(py_set) || !(py_predicate) || !(py_module) ||
		!(py_function)) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM,
						"Parameter should not be null");
		goto CLEANUP;
	}

	if (!PyList_Check(py_args)) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM,
						"Arguments should be a list");
		goto CLEANUP;
	}

	char *set_p = NULL;
	if (PyUnicode_Check(py_set)) {
		py_ustr1 = PyUnicode_AsUTF8String(py_set);
		set_p = PyBytes_AsString(py_ustr1);
	}
	else if (PyString_Check(py_set)) {
		set_p = PyString_AsString(py_set);
	}
	else if (Py_None != py_set) {
		// Scan whole namespace if set is 'None' else error
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Set name should be string");
		goto CLEANUP;
	}

	as_query_init(&query, namespace_p, set_p);
	is_query_init = true;

	if (py_policy) {
		pyobject_to_policy_write(
			self, &err, py_policy, &write_policy, &write_policy_p,
			&self->as->config.policies.write, &predexp_list, &predexp_list_p,
			&exp_list, &exp_list_p);

		if (err.code != AEROSPIKE_OK) {
			goto CLEANUP;
		}
	}

	char *module_p = NULL;
	if (PyUnicode_Check(py_module)) {
		py_ustr2 = PyUnicode_AsUTF8String(py_module);
		module_p = PyBytes_AsString(py_ustr2);
	}
	else if (PyString_Check(py_module)) {
		module_p = PyString_AsString(py_module);
	}
	else {
		as_error_update(&err, AEROSPIKE_ERR_PARAM,
						"Module name should be string");
		goto CLEANUP;
	}

	char *function_p = NULL;
	if (PyUnicode_Check(py_function)) {
		py_ustr3 = PyUnicode_AsUTF8String(py_function);
		function_p = PyBytes_AsString(py_ustr3);
	}
	else if (PyString_Check(py_function)) {
		function_p = PyString_AsString(py_function);
	}
	else {
		as_error_update(&err, AEROSPIKE_ERR_PARAM,
						"Function name should be string");
		goto CLEANUP;
	}

	pyobject_to_list(self, &err, py_args, &arglist, &static_pool,
					 SERIALIZER_PYTHON);
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	as_query *query_ptr = &query;
	if (PyTuple_Check(py_predicate)) {

		Py_ssize_t size = PyTuple_Size(py_predicate);
		if (size < 2) {
			as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid predicate");
			goto CLEANUP;
		}

		PyObject *py_op = PyTuple_GetItem(py_predicate, 0);
		PyObject *py_op_data = PyTuple_GetItem(py_predicate, 1);

		if (!py_op || !py_op_data) {
			as_error_update(&err, AEROSPIKE_ERR_CLIENT,
							"Failed to get predicate elements");
			goto CLEANUP;
		}
		if (!PyInt_Check(py_op) || !PyInt_Check(py_op_data)) {
			as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid Predicate");
			goto CLEANUP;
		}

		as_predicate_type op = (as_predicate_type)PyInt_AsLong(py_op);
		as_index_datatype op_data = (as_index_datatype)PyInt_AsLong(py_op_data);
		rc = query_where_add(
			&query_ptr, op, op_data,
			size > 2 ? PyTuple_GetItem(py_predicate, 2) : Py_None,
			size > 3 ? PyTuple_GetItem(py_predicate, 3) : Py_None,
			size > 4 ? PyTuple_GetItem(py_predicate, 4) : Py_None,
			size > 5 ? PyInt_AsLong(PyTuple_GetItem(py_predicate, 5)) : 0,
			&err);

		if (rc) {
			goto CLEANUP;
		}
	}
	else {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Predicate must be a tuple");
		goto CLEANUP;
	}

	if (!as_query_apply(&query, module_p, function_p, arglist)) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM,
						"Unable to apply UDF on the scan");
		goto CLEANUP;
	}

	Py_BEGIN_ALLOW_THREADS
	aerospike_query_background(self->as, &err, write_policy_p, &query,
							   &query_id);
	Py_END_ALLOW_THREADS
	arglist = NULL;
	if (err.code == AEROSPIKE_OK) {
		if (block) {
			if (py_policy) {
				pyobject_to_policy_info(&err, py_policy, &info_policy,
										&info_policy_p,
										&self->as->config.policies.info);
				if (err.code != AEROSPIKE_OK) {
					goto CLEANUP;
				}
			}
			Py_BEGIN_ALLOW_THREADS
			aerospike_query_wait(self->as, &err, info_policy_p, &query,
								 query_id, 0);
			Py_END_ALLOW_THREADS
			if (err.code != AEROSPIKE_OK) {
				as_error_update(&err, AEROSPIKE_ERR_PARAM,
								"Unable to perform query_wait on the query");
			}
		}
	}
	else {
		goto CLEANUP;
	}

CLEANUP:
	if (exp_list_p) {
		as_exp_destroy(exp_list_p);
		;
	}

	if (predexp_list_p) {
		as_predexp_list_destroy(&predexp_list);
	}

	if (py_ustr1) {
		Py_DECREF(py_ustr1);
	}

	if (py_ustr2) {
		Py_DECREF(py_ustr2);
	}

	if (py_ustr3) {
		Py_DECREF(py_ustr3);
	}

	if (arglist) {
		as_list_destroy(arglist);
	}

	if (is_query_init) {
		as_query_destroy(&query);
	}

	if (err.code != AEROSPIKE_OK) {
		PyObject *py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyObject *exception_type = raise_exception(&err);
		PyErr_SetObject(exception_type, py_err);
		Py_XDECREF(py_err);
		return NULL;
	}

	return PyLong_FromLong(query_id);
}

/**
 ******************************************************************************************************
 * Apply a record UDF to each record in a background query.
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns  integer handle for the initiated background query.
 *******************************************************************************************************
 */
PyObject *AerospikeClient_QueryApply(AerospikeClient *self, PyObject *args,
									 PyObject *kwds)
{
	// Python Function Arguments
	PyObject *py_args = NULL;
	PyObject *py_policy = NULL;

	// Python Function Keyword Arguments
	static char *kwlist[] = {"ns",		 "set",	 "predicate", "module",
							 "function", "args", "policy",	  NULL};
	char *namespace = NULL;
	PyObject *py_set = NULL;
	PyObject *py_module = NULL;
	PyObject *py_function = NULL;
	PyObject *py_predicate = NULL;

	// Python Function Argument Parsing
	if (PyArg_ParseTupleAndKeywords(args, kwds, "sOOOO|OO:query_apply", kwlist,
									&namespace, &py_set, &py_predicate,
									&py_module, &py_function, &py_args,
									&py_policy) == false) {
		return NULL;
	}

	// Invoke Operation
	return AerospikeClient_QueryApply_Invoke(
		self, namespace, py_set, py_predicate, py_module, py_function, py_args,
		py_policy, true);
}
/**
 *******************************************************************************************************
 * Gets the status of a background query triggered by queryApply()
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns status of the background query returned as a tuple containing
 * progress_pct, records_read, status.
 *******************************************************************************************************
 */
PyObject *AerospikeClient_JobInfo(AerospikeClient *self, PyObject *args,
								  PyObject *kwds)
{
	// Initialize error
	as_error err;
	as_error_init(&err);

	// Python Function Arguments
	PyObject *py_policy = NULL;
	PyObject *retObj = PyDict_New();

	uint64_t ujobId = 0;
	char *module = NULL;

	as_policy_info info_policy;
	as_policy_info *info_policy_p = NULL;
	as_job_info job_info;

	// Python Function Keyword Arguments
	static char *kwlist[] = {"job_id", "module", "policy", NULL};

	// Python Function Argument Parsing
	if (PyArg_ParseTupleAndKeywords(args, kwds, "Ks|O:job_info", kwlist,
									&ujobId, &module, &py_policy) == false) {
		return NULL;
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

	// Convert python object to policy_info
	pyobject_to_policy_info(&err, py_policy, &info_policy, &info_policy_p,
							&self->as->config.policies.info);
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	if (strcmp(module, "scan") && strcmp(module, "query")) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM,
						"Module can have only two values: aerospike.JOB_SCAN "
						"or aerospike.JOB_QUERY");
		goto CLEANUP;
	}

	Py_BEGIN_ALLOW_THREADS
	aerospike_job_info(self->as, &err, info_policy_p, module, ujobId, false,
					   &job_info);
	Py_END_ALLOW_THREADS

	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	if (retObj) {
		PyObject *py_longobject = NULL;
		py_longobject = PyLong_FromLong(job_info.progress_pct);
		PyDict_SetItemString(retObj, PROGRESS_PCT, py_longobject);
		Py_XDECREF(py_longobject);
		py_longobject = PyLong_FromLong(job_info.records_read);
		PyDict_SetItemString(retObj, RECORDS_READ, py_longobject);
		Py_XDECREF(py_longobject);
		py_longobject = PyLong_FromLong(job_info.status);
		PyDict_SetItemString(retObj, STATUS, py_longobject);
		Py_XDECREF(py_longobject);
	}

CLEANUP:

	if (err.code != AEROSPIKE_OK) {
		PyObject *py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyObject *exception_type = raise_exception(&err);
		PyErr_SetObject(exception_type, py_err);
		Py_XDECREF(py_err);
		return NULL;
	}

	return retObj;
}
