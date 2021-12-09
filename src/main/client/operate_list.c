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
#include <stdlib.h>
#include <string.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/as_key.h>
#include <aerospike/as_error.h>
#include <aerospike/as_record.h>
#include <aerospike/as_operations.h>
#include <aerospike/aerospike_info.h>
#include "client.h"
#include "conversions.h"
#include "exceptions.h"
#include "policy.h"
#include "serializer.h"
#include "geo.h"

#include <aerospike/as_double.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_geojson.h>
#include <aerospike/as_nil.h>

#define BASE_VARIABLES                                                         \
	as_error err;                                                              \
	as_error_init(&err);                                                       \
	PyObject *py_key = NULL;                                                   \
	PyObject *py_bin = NULL;                                                   \
	PyObject *py_policy = NULL;                                                \
	PyObject *py_meta = NULL;                                                  \
	as_policy_operate operate_policy;                                          \
	as_policy_operate *operate_policy_p = NULL;                                \
	as_key key;                                                                \
	bool key_created = false;                                                  \
	char *bin = NULL;

#define CHECK_CONNECTED_AND_CDT_SUPPORT()                                      \
	if (!self || !self->as) {                                                  \
		as_error_update(&err, AEROSPIKE_ERR_PARAM,                             \
						"Invalid aerospike object");                           \
		goto CLEANUP;                                                          \
	}                                                                          \
	if (!self->is_conn_16) {                                                   \
		as_error_update(&err, AEROSPIKE_ERR_CLUSTER,                           \
						"No connection to aerospike cluster");                 \
		goto CLEANUP;                                                          \
	}                                                                          \
	if (!has_cdt_list(self->as, &err)) {                                       \
		as_error_update(&err, AEROSPIKE_ERR_UNSUPPORTED_FEATURE,               \
						"CDT list feature is not supported");                  \
		goto CLEANUP;                                                          \
	}

#define POLICY_KEY_META_BIN()                                                  \
	if (py_policy) {                                                           \
		if (pyobject_to_policy_operate(                                        \
				self, &err, py_policy, &operate_policy, &operate_policy_p,     \
				&self->as->config.policies.operate, NULL, NULL, NULL,          \
				NULL) != AEROSPIKE_OK) {                                       \
			goto CLEANUP;                                                      \
		}                                                                      \
	}                                                                          \
	if (pyobject_to_key(&err, py_key, &key) != AEROSPIKE_OK) {                 \
		goto CLEANUP;                                                          \
	}                                                                          \
	else {                                                                     \
		key_created = true;                                                    \
	}                                                                          \
	if (py_meta) {                                                             \
		if (check_for_meta(py_meta, &ops, &err) != AEROSPIKE_OK) {             \
			goto CLEANUP;                                                      \
		}                                                                      \
	}                                                                          \
	if (bin_strict_type_checking(self, &err, py_bin, &bin) != AEROSPIKE_OK) {  \
		goto CLEANUP;                                                          \
	}

#define DO_OPERATION(__rec)                                                    \
	Py_BEGIN_ALLOW_THREADS                                                     \
	aerospike_key_operate(self->as, &err, operate_policy_p, &key, &ops,        \
						  __rec);                                              \
	Py_END_ALLOW_THREADS

#define EXCEPTION_ON_ERROR()                                                   \
	if (key_created) {                                                         \
		as_key_destroy(&key);                                                  \
	}                                                                          \
	if (err.code != AEROSPIKE_OK) {                                            \
		PyObject *py_err = NULL;                                               \
		error_to_pyobject(&err, &py_err);                                      \
		PyObject *exception_type = raise_exception(&err);                      \
		if (PyObject_HasAttrString(exception_type, "key")) {                   \
			PyObject_SetAttrString(exception_type, "key", py_key);             \
		}                                                                      \
		if (PyObject_HasAttrString(exception_type, "bin")) {                   \
			PyObject_SetAttrString(exception_type, "bin", py_bin);             \
		}                                                                      \
		PyErr_SetObject(exception_type, py_err);                               \
		Py_DECREF(py_err);                                                     \
		return NULL;                                                           \
	}

/**
  *******************************************************************************************************
  * Check whether Aerospike server supports CDT feature or not.
  *******************************************************************************************************
  */
#define INFO_CALL "features"
static bool has_cdt_list(aerospike *as, as_error *err)
{
	char *res = NULL;

	int rc = aerospike_info_any(as, err, NULL, INFO_CALL, &res);

	if (rc == AEROSPIKE_OK) {
		char *st = strstr(res, "cdt-list");
		free(res);
		if (st) {
			return true;
		}
	}
	return false;
}

/**
 *******************************************************************************************************
 * Append a single value to the list value in bin.
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns an integer status. 0(Zero) is success value.
 * In case of error,appropriate exceptions will be raised.
 *******************************************************************************************************
 */
PyObject *AerospikeClient_ListAppend(AerospikeClient *self, PyObject *args,
									 PyObject *kwds)
{
	BASE_VARIABLES

	PyObject *py_append_val = NULL;
	as_operations ops;
	as_operations_inita(&ops, 1);

	// Python Function Keyword Arguments
	static char *kwlist[] = {"key", "bin", "val", "meta", "policy", NULL};
	if (PyArg_ParseTupleAndKeywords(args, kwds, "OOO|OO:list_append", kwlist,
									&py_key, &py_bin, &py_append_val, &py_meta,
									&py_policy) == false) {
		return NULL;
	}

	CHECK_CONNECTED_AND_CDT_SUPPORT();

	as_static_pool static_pool;
	memset(&static_pool, 0, sizeof(static_pool));

	POLICY_KEY_META_BIN();

	as_val *put_val = NULL;
	if (pyobject_to_val(self, &err, py_append_val, &put_val, &static_pool,
						SERIALIZER_PYTHON) != AEROSPIKE_OK) {
		goto CLEANUP;
	}
	as_operations_add_list_append(&ops, bin, put_val);

	DO_OPERATION(NULL);

CLEANUP:
	as_operations_destroy(&ops);
	EXCEPTION_ON_ERROR();

	return PyLong_FromLong(0);
}

/**
 *******************************************************************************************************
 * Extend the list value in bin with the given items.
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns an integer status. 0(Zero) is success value.
 * In case of error,appropriate exceptions will be raised.
 *******************************************************************************************************
 */
PyObject *AerospikeClient_ListExtend(AerospikeClient *self, PyObject *args,
									 PyObject *kwds)
{
	BASE_VARIABLES

	PyObject *py_append_val = NULL;
	as_operations ops;
	as_operations_inita(&ops, 1);

	// Python Function Keyword Arguments
	static char *kwlist[] = {"key", "bin", "items", "meta", "policy", NULL};
	if (PyArg_ParseTupleAndKeywords(args, kwds, "OOO|OO:list_extend", kwlist,
									&py_key, &py_bin, &py_append_val, &py_meta,
									&py_policy) == false) {
		return NULL;
	}

	CHECK_CONNECTED_AND_CDT_SUPPORT();

	as_static_pool static_pool;
	memset(&static_pool, 0, sizeof(static_pool));

	if (!PyList_Check(py_append_val)) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM,
						"Items should be of type list");
		goto CLEANUP;
	}

	POLICY_KEY_META_BIN();

	as_val *put_val = NULL;
	pyobject_to_val(self, &err, py_append_val, &put_val, &static_pool,
					SERIALIZER_PYTHON);
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}
	as_operations_add_list_append_items(&ops, bin, (as_list *)put_val);

	DO_OPERATION(NULL);

CLEANUP:
	as_operations_destroy(&ops);
	EXCEPTION_ON_ERROR();

	return PyLong_FromLong(0);
}

/**
 *******************************************************************************************************
 * Inserts val at the specified index of the list value in bin.
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns an integer status. 0(Zero) is success value.
 * In case of error,appropriate exceptions will be raised.
 *******************************************************************************************************
 */
PyObject *AerospikeClient_ListInsert(AerospikeClient *self, PyObject *args,
									 PyObject *kwds)
{
	BASE_VARIABLES

	PyObject *py_insert_val = NULL;
	uint64_t index;
	as_operations ops;
	as_operations_inita(&ops, 1);
	// Python Function Keyword Arguments
	static char *kwlist[] = {"key",	 "bin",	   "index", "val",
							 "meta", "policy", NULL};
	if (PyArg_ParseTupleAndKeywords(args, kwds, "OOlO|OO:list_insert", kwlist,
									&py_key, &py_bin, &index, &py_insert_val,
									&py_meta, &py_policy) == false) {
		return NULL;
	}

	CHECK_CONNECTED_AND_CDT_SUPPORT();

	as_static_pool static_pool;
	memset(&static_pool, 0, sizeof(static_pool));

	POLICY_KEY_META_BIN();

	as_val *put_val = NULL;
	pyobject_to_val(self, &err, py_insert_val, &put_val, &static_pool,
					SERIALIZER_PYTHON);
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	as_operations_add_list_insert(&ops, bin, index, put_val);

	DO_OPERATION(NULL);

CLEANUP:
	as_operations_destroy(&ops);
	EXCEPTION_ON_ERROR();

	return PyLong_FromLong(0);
}

/**
 *******************************************************************************************************
 * Insert the items at the specified index of a list value in bin.
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns an integer status. 0(Zero) is success value.
 * In case of error,appropriate exceptions will be raised.
 *******************************************************************************************************
 */
PyObject *AerospikeClient_ListInsertItems(AerospikeClient *self, PyObject *args,
										  PyObject *kwds)
{
	BASE_VARIABLES

	PyObject *py_insert_val = NULL;
	uint64_t index;
	as_operations ops;
	as_operations_inita(&ops, 1);
	// Python Function Keyword Arguments
	static char *kwlist[] = {"key",	 "bin",	   "index", "items",
							 "meta", "policy", NULL};
	if (PyArg_ParseTupleAndKeywords(
			args, kwds, "OOlO|OO:list_insert_items", kwlist, &py_key, &py_bin,
			&index, &py_insert_val, &py_meta, &py_policy) == false) {
		return NULL;
	}

	CHECK_CONNECTED_AND_CDT_SUPPORT();

	as_static_pool static_pool;
	memset(&static_pool, 0, sizeof(static_pool));

	if (!PyList_Check(py_insert_val)) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM,
						"Items should be of type list");
		goto CLEANUP;
	}

	POLICY_KEY_META_BIN();

	as_val *put_val = NULL;
	pyobject_to_val(self, &err, py_insert_val, &put_val, &static_pool,
					SERIALIZER_PYTHON);
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	as_operations_add_list_insert_items(&ops, bin, index, (as_list *)put_val);

	DO_OPERATION(NULL);

CLEANUP:
	as_operations_destroy(&ops);
	EXCEPTION_ON_ERROR();

	return PyLong_FromLong(0);
}

/**
 *******************************************************************************************************
 * Count the elements of the list value in bin.
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns count of elements in the list.
 * In case of error,appropriate exceptions will be raised.
 *******************************************************************************************************
 */
PyObject *AerospikeClient_ListSize(AerospikeClient *self, PyObject *args,
								   PyObject *kwds)
{
	BASE_VARIABLES

	int64_t list_size = 0;
	as_record *rec = NULL;
	as_operations ops;
	as_operations_inita(&ops, 1);

	// Python Function Keyword Arguments
	static char *kwlist[] = {"key", "bin", "meta", "policy", NULL};
	if (PyArg_ParseTupleAndKeywords(args, kwds, "OO|OO:list_size", kwlist,
									&py_key, &py_bin, &py_meta,
									&py_policy) == false) {
		return NULL;
	}

	CHECK_CONNECTED_AND_CDT_SUPPORT();

	POLICY_KEY_META_BIN();

	as_operations_add_list_size(&ops, bin);

	// Initialize record
	as_record_init(rec, 0);

	DO_OPERATION(&rec);

	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	if (rec) {
		list_size = as_record_get_int64(rec, bin, 0);
	}

CLEANUP:
	as_operations_destroy(&ops);
	if (rec) {
		as_record_destroy(rec);
	}
	EXCEPTION_ON_ERROR();

	return PyLong_FromLong(list_size);
}

/**
 *******************************************************************************************************
 * Remove and get back a list element at a given index of a list value in bin.
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns an element at that index.
 * In case of error,appropriate exceptions will be raised.
 *******************************************************************************************************
 */
PyObject *AerospikeClient_ListPop(AerospikeClient *self, PyObject *args,
								  PyObject *kwds)
{
	BASE_VARIABLES

	uint64_t index;
	as_record *rec = NULL;
	as_operations ops;
	as_operations_inita(&ops, 1);
	// Python Function Keyword Arguments
	static char *kwlist[] = {"key", "bin", "index", "meta", "policy", NULL};
	if (PyArg_ParseTupleAndKeywords(args, kwds, "OOl|OO:list_pop", kwlist,
									&py_key, &py_bin, &index, &py_meta,
									&py_policy) == false) {
		return NULL;
	}

	CHECK_CONNECTED_AND_CDT_SUPPORT();

	POLICY_KEY_META_BIN();

	as_operations_add_list_pop(&ops, bin, index);

	DO_OPERATION(&rec);

	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}
	PyObject *py_val = NULL;
	if (rec && rec->bins.size) {
		val_to_pyobject(self, &err, (as_val *)(rec->bins.entries[0].valuep),
						&py_val);
	}
	else {
		py_val = Py_None;
		Py_INCREF(py_val);
	}

CLEANUP:
	as_operations_destroy(&ops);
	if (rec) {
		as_record_destroy(rec);
	}
	EXCEPTION_ON_ERROR();

	return py_val;
}

/**
 *******************************************************************************************************
 * Remove and get back list elements at a given index of a list value in bin.
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns a list of elements.
 * In case of error,appropriate exceptions will be raised.
 *******************************************************************************************************
 */
PyObject *AerospikeClient_ListPopRange(AerospikeClient *self, PyObject *args,
									   PyObject *kwds)
{
	BASE_VARIABLES

	PyObject *py_list = NULL;
	uint64_t index;
	uint64_t count = -1;
	as_record *rec = NULL;
	as_operations ops;
	as_operations_inita(&ops, 1);
	// Python Function Keyword Arguments
	static char *kwlist[] = {"key",	 "bin",	   "index", "count",
							 "meta", "policy", NULL};
	if (PyArg_ParseTupleAndKeywords(args, kwds, "OOll|OO:list_pop_range",
									kwlist, &py_key, &py_bin, &index, &count,
									&py_meta, &py_policy) == false) {
		return NULL;
	}

	if (!self || !self->as) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
		goto CLEANUP;
	}

	CHECK_CONNECTED_AND_CDT_SUPPORT();

	POLICY_KEY_META_BIN();

	as_operations_add_list_pop_range(&ops, bin, index, count);

	DO_OPERATION(&rec);

	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}
	if (rec && rec->bins.size) {
		list_to_pyobject(self, &err, as_record_get_list(rec, bin), &py_list);
	}
	else {
		py_list = Py_None;
		Py_INCREF(py_list);
	}

CLEANUP:
	as_operations_destroy(&ops);
	if (rec) {
		as_record_destroy(rec);
	}
	EXCEPTION_ON_ERROR();

	return py_list;
}

/**
 *******************************************************************************************************
 * Remove a list element at a given index of a list value in bin.
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns a list of elements.
 * In case of error,appropriate exceptions will be raised.
 *******************************************************************************************************
 */
PyObject *AerospikeClient_ListRemove(AerospikeClient *self, PyObject *args,
									 PyObject *kwds)
{
	BASE_VARIABLES

	uint64_t index;
	as_operations ops;
	as_operations_inita(&ops, 1);

	// Python Function Keyword Arguments
	static char *kwlist[] = {"key", "bin", "index", "meta", "policy", NULL};
	if (PyArg_ParseTupleAndKeywords(args, kwds, "OOl|OO:list_remove", kwlist,
									&py_key, &py_bin, &index, &py_meta,
									&py_policy) == false) {
		return NULL;
	}

	CHECK_CONNECTED_AND_CDT_SUPPORT();

	POLICY_KEY_META_BIN();

	as_operations_add_list_remove(&ops, bin, index);

	DO_OPERATION(NULL);

CLEANUP:
	as_operations_destroy(&ops);
	EXCEPTION_ON_ERROR();

	return PyLong_FromLong(0);
}

/**
 *******************************************************************************************************
 * Remove list elements at a given index of a list value in bin.
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns a list of elements.
 * In case of error,appropriate exceptions will be raised.
 *******************************************************************************************************
 */
PyObject *AerospikeClient_ListRemoveRange(AerospikeClient *self, PyObject *args,
										  PyObject *kwds)
{
	BASE_VARIABLES

	uint64_t index;
	uint64_t count = -1;
	as_operations ops;
	as_operations_inita(&ops, 1);

	// Python Function Keyword Arguments
	static char *kwlist[] = {"key",	 "bin",	   "index", "count",
							 "meta", "policy", NULL};
	if (PyArg_ParseTupleAndKeywords(args, kwds, "OOll|OO:list_remove_range",
									kwlist, &py_key, &py_bin, &index, &count,
									&py_meta, &py_policy) == false) {
		return NULL;
	}

	CHECK_CONNECTED_AND_CDT_SUPPORT();

	POLICY_KEY_META_BIN();

	as_operations_add_list_remove_range(&ops, bin, index, count);

	DO_OPERATION(NULL);

CLEANUP:
	as_operations_destroy(&ops);
	EXCEPTION_ON_ERROR();

	return PyLong_FromLong(0);
}

/**
 *******************************************************************************************************
 * Remove all the elements from a list value in bin.
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns a list of elements.
 * In case of error,appropriate exceptions will be raised.
 *******************************************************************************************************
 */
PyObject *AerospikeClient_ListClear(AerospikeClient *self, PyObject *args,
									PyObject *kwds)
{
	BASE_VARIABLES
	as_operations ops;
	as_operations_inita(&ops, 1);
	// Python Function Keyword Arguments
	static char *kwlist[] = {"key", "bin", "meta", "policy", NULL};
	if (PyArg_ParseTupleAndKeywords(args, kwds, "OO|OO:list_clear", kwlist,
									&py_key, &py_bin, &py_meta,
									&py_policy) == false) {
		return NULL;
	}

	CHECK_CONNECTED_AND_CDT_SUPPORT();

	POLICY_KEY_META_BIN();

	as_operations_add_list_clear(&ops, bin);

	DO_OPERATION(NULL);

CLEANUP:
	as_operations_destroy(&ops);
	EXCEPTION_ON_ERROR();

	return PyLong_FromLong(0);
}

/**
 *******************************************************************************************************
 * Set list element val at the specified index of a list value in bin.
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns a list of elements.
 * In case of error,appropriate exceptions will be raised.
 *******************************************************************************************************
 */
PyObject *AerospikeClient_ListSet(AerospikeClient *self, PyObject *args,
								  PyObject *kwds)
{
	BASE_VARIABLES

	PyObject *py_val = NULL;
	uint64_t index;
	as_operations ops;
	as_operations_inita(&ops, 1);
	// Python Function Keyword Arguments
	static char *kwlist[] = {"key",	 "bin",	   "index", "val",
							 "meta", "policy", NULL};
	if (PyArg_ParseTupleAndKeywords(args, kwds, "OOlO|OO:list_set", kwlist,
									&py_key, &py_bin, &index, &py_val, &py_meta,
									&py_policy) == false) {
		return NULL;
	}

	CHECK_CONNECTED_AND_CDT_SUPPORT();

	as_static_pool static_pool;
	memset(&static_pool, 0, sizeof(static_pool));

	POLICY_KEY_META_BIN();

	as_val *put_val = NULL;
	pyobject_to_val(self, &err, py_val, &put_val, &static_pool,
					SERIALIZER_PYTHON);
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}
	as_operations_add_list_set(&ops, bin, index, put_val);

	DO_OPERATION(NULL);

CLEANUP:
	as_operations_destroy(&ops);
	EXCEPTION_ON_ERROR();

	return PyLong_FromLong(0);
}

/**
 *******************************************************************************************************
 * Get the list element at the specified index of a list value in bin.
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns a list of elements.
 * In case of error,appropriate exceptions will be raised.
 *******************************************************************************************************
 */
PyObject *AerospikeClient_ListGet(AerospikeClient *self, PyObject *args,
								  PyObject *kwds)
{
	BASE_VARIABLES

	uint64_t index;
	as_record *rec = NULL;
	as_operations ops;
	as_operations_inita(&ops, 1);
	// Python Function Keyword Arguments
	static char *kwlist[] = {"key", "bin", "index", "meta", "policy", NULL};
	if (PyArg_ParseTupleAndKeywords(args, kwds, "OOl|OO:list_get", kwlist,
									&py_key, &py_bin, &index, &py_meta,
									&py_policy) == false) {
		return NULL;
	}

	CHECK_CONNECTED_AND_CDT_SUPPORT();

	POLICY_KEY_META_BIN();

	as_operations_add_list_get(&ops, bin, index);

	DO_OPERATION(&rec);

	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	PyObject *py_val = NULL;
	if (rec && rec->bins.size) {
		val_to_pyobject(self, &err, (as_val *)(rec->bins.entries[0].valuep),
						&py_val);
	}
	else {
		py_val = Py_None;
		Py_INCREF(py_val);
	}

CLEANUP:
	as_operations_destroy(&ops);
	if (rec) {
		as_record_destroy(rec);
	}

	EXCEPTION_ON_ERROR();

	return py_val;
}

/**
 *******************************************************************************************************
 * Get the list of count elements starting at a specified index of a list value in bin.
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns a list of elements.
 * In case of error,appropriate exceptions will be raised.
 *******************************************************************************************************
 */
PyObject *AerospikeClient_ListGetRange(AerospikeClient *self, PyObject *args,
									   PyObject *kwds)
{
	BASE_VARIABLES

	PyObject *py_list = NULL;
	uint64_t index;
	uint64_t count;
	as_record *rec = NULL;
	as_operations ops;
	as_operations_inita(&ops, 1);
	// Python Function Keyword Arguments
	static char *kwlist[] = {"key",	 "bin",	   "index", "count",
							 "meta", "policy", NULL};

	// Python Function Argument Parsing
	if (PyArg_ParseTupleAndKeywords(args, kwds, "OOll|OO:list_get_range",
									kwlist, &py_key, &py_bin, &index, &count,
									&py_meta, &py_policy) == false) {
		return NULL;
	}

	CHECK_CONNECTED_AND_CDT_SUPPORT();

	POLICY_KEY_META_BIN();

	as_operations_add_list_get_range(&ops, bin, index, count);

	DO_OPERATION(&rec);

	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	if (rec && rec->bins.size) {
		list_to_pyobject(self, &err, as_record_get_list(rec, bin), &py_list);
	}
	else if (rec && rec->bins.size == 0) {
		as_list *list = NULL;
		list_to_pyobject(self, &err, list, &py_list);
	}

CLEANUP:
	as_operations_destroy(&ops);
	if (rec) {
		as_record_destroy(rec);
	}

	EXCEPTION_ON_ERROR();

	return py_list;
}

/**
 *******************************************************************************************************
 * Remove elements from the list which are not within the range starting at the given index plus count.
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns an integer status. 0(Zero) is success value.
 * In case of error,appropriate exceptions will be raised.
 *******************************************************************************************************
 */
PyObject *AerospikeClient_ListTrim(AerospikeClient *self, PyObject *args,
								   PyObject *kwds)
{
	BASE_VARIABLES

	uint64_t index;
	uint64_t count;
	as_record *rec = NULL;
	as_operations ops;
	as_operations_inita(&ops, 1);
	// Python Function Keyword Arguments
	static char *kwlist[] = {"key",	 "bin",	   "index", "count",
							 "meta", "policy", NULL};

	// Python Function Argument Parsing
	if (PyArg_ParseTupleAndKeywords(args, kwds, "OOll|OO:list_trim", kwlist,
									&py_key, &py_bin, &index, &count, &py_meta,
									&py_policy) == false) {
		return NULL;
	}

	CHECK_CONNECTED_AND_CDT_SUPPORT();

	POLICY_KEY_META_BIN();

	as_operations_add_list_trim(&ops, bin, index, count);

	DO_OPERATION(&rec);

CLEANUP:
	as_operations_destroy(&ops);
	if (rec) {
		as_record_destroy(rec);
	}
	EXCEPTION_ON_ERROR();

	return PyLong_FromLong(0);
}
