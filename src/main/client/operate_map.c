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
#include <aerospike/as_map_operations.h>
#include <aerospike/aerospike_info.h>
#include "client.h"
#include "conversions.h"
#include "exceptions.h"
#include "policy.h"
#include "serializer.h"

#define BASE_VARIABLES                                                         \
	as_error err;                                                              \
	as_error_init(&err);                                                       \
	as_operations ops;                                                         \
	as_operations_inita(&ops, 1);                                              \
	PyObject *py_key = NULL;                                                   \
	PyObject *py_bin = NULL;                                                   \
	char *bin = NULL;                                                          \
	bool key_created = false;                                                  \
	as_key key;

#define CHECK_CONNECTED()                                                      \
	if (!self || !self->as) {                                                  \
		as_error_update(&err, AEROSPIKE_ERR_PARAM,                             \
						"Invalid aerospike object");                           \
		goto CLEANUP;                                                          \
	}                                                                          \
	if (!self->is_conn_16) {                                                   \
		as_error_update(&err, AEROSPIKE_ERR_CLUSTER,                           \
						"No connection to aerospike cluster");                 \
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

#define CHECK_BIN_AND_KEY()                                                    \
	if (bin_strict_type_checking(self, &err, py_bin, &bin) != AEROSPIKE_OK) {  \
		goto CLEANUP;                                                          \
	}                                                                          \
	if (pyobject_to_key(&err, py_key, &key) != AEROSPIKE_OK) {                 \
		goto CLEANUP;                                                          \
	}                                                                          \
	else {                                                                     \
		key_created = true;                                                    \
	}

#define SETUP_MAP_POLICY()                                                     \
	if (py_mapPolicy) {                                                        \
		if (pyobject_to_map_policy(&err, py_mapPolicy, &map_policy) !=         \
			AEROSPIKE_OK) {                                                    \
			goto CLEANUP;                                                      \
		}                                                                      \
	}

#define DO_OPERATION()                                                         \
	Py_BEGIN_ALLOW_THREADS                                                     \
	aerospike_key_operate(self->as, &err, operate_policy_p, &key, &ops, &rec); \
	Py_END_ALLOW_THREADS                                                       \
	if (err.code != AEROSPIKE_OK) {                                            \
		goto CLEANUP;                                                          \
	}

#define SETUP_RETURN_VAL()                                                     \
	if (rec && rec->bins.size) {                                               \
		if (returnType == AS_MAP_RETURN_KEY_VALUE) {                           \
			val_to_pyobject_cnvt_list_to_map(                                  \
				self, &err, (as_val *)(rec->bins.entries[0].valuep),           \
				&py_result);                                                   \
		}                                                                      \
		else {                                                                 \
			val_to_pyobject(self, &err,                                        \
							(as_val *)(rec->bins.entries[0].valuep),           \
							&py_result);                                       \
		}                                                                      \
	}                                                                          \
	else {                                                                     \
		as_error_update(&err, AEROSPIKE_ERR_CLIENT,                            \
						"Unexpected empty return");                            \
	}

#define CLEANUP_AND_EXCEPTION_ON_ERROR(__err)                                  \
	as_operations_destroy(&ops);                                               \
	as_record_destroy(rec);                                                    \
	if (key_created) {                                                         \
		as_key_destroy(&key);                                                  \
	}                                                                          \
	if (__err.code != AEROSPIKE_OK) {                                          \
		PyObject *py_err = NULL;                                               \
		error_to_pyobject(&__err, &py_err);                                    \
		PyObject *exception_type = raise_exception(&__err);                    \
		PyErr_SetObject(exception_type, py_err);                               \
		Py_DECREF(py_err);                                                     \
		return NULL;                                                           \
	}

/* Forward declaration for function which inverts an operation */
static as_status invertIfSpecified(as_error *err, PyObject *py_inverted,
								   uint64_t *returnType);

PyObject *AerospikeClient_MapSetPolicy(AerospikeClient *self, PyObject *args,
									   PyObject *kwds)
{
	BASE_VARIABLES

	PyObject *py_mapPolicy = NULL;
	as_map_policy map_policy;
	as_record *rec = NULL;
	bool error_occured = false;

	CHECK_CONNECTED();

	static char *kwlist[] = {"key", "bin", "map_policy", NULL};
	if (PyArg_ParseTupleAndKeywords(args, kwds, "OOO:map_set_policy", kwlist,
									&py_key, &py_bin, &py_mapPolicy) == false) {
		error_occured = true;
		goto CLEANUP;
	}

	CHECK_BIN_AND_KEY();
	SETUP_MAP_POLICY();

	as_operations_add_map_set_policy(&ops, bin, &map_policy);

	Py_BEGIN_ALLOW_THREADS
	aerospike_key_operate(self->as, &err, NULL, &key, &ops, &rec);
	Py_END_ALLOW_THREADS

CLEANUP:
	CLEANUP_AND_EXCEPTION_ON_ERROR(err);

	if (error_occured) {
		return NULL;
	}
	return PyLong_FromLong(0);
}

PyObject *AerospikeClient_MapPut(AerospikeClient *self, PyObject *args,
								 PyObject *kwds)
{
	BASE_VARIABLES

	as_static_pool static_pool;
	memset(&static_pool, 0, sizeof(static_pool));

	PyObject *py_mapKey = NULL;
	PyObject *py_mapValue = NULL;
	PyObject *py_meta = NULL;
	PyObject *py_policy = NULL;
	PyObject *py_mapPolicy = NULL;
	as_map_policy map_policy;
	as_map_policy_init(&map_policy);
	as_val *put_key = NULL;
	as_val *put_val = NULL;
	as_record *rec = NULL;
	as_policy_operate operate_policy;
	as_policy_operate *operate_policy_p = NULL;
	bool error_occured = false;

	CHECK_CONNECTED();

	static char *kwlist[] = {"key",		   "bin",  "map_key", "val",
							 "map_policy", "meta", "policy",  NULL};
	if (PyArg_ParseTupleAndKeywords(args, kwds, "OOOO|OOO:map_put", kwlist,
									&py_key, &py_bin, &py_mapKey, &py_mapValue,
									&py_mapPolicy, &py_meta,
									&py_policy) == false) {
		error_occured = true;
		goto CLEANUP;
	}

	POLICY_KEY_META_BIN();
	SETUP_MAP_POLICY();

	if (pyobject_to_val(self, &err, py_mapKey, &put_key, &static_pool,
						SERIALIZER_PYTHON) != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	if (pyobject_to_val(self, &err, py_mapValue, &put_val, &static_pool,
						SERIALIZER_PYTHON) != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	as_operations_add_map_put(&ops, bin, &map_policy, put_key, put_val);

	DO_OPERATION();

CLEANUP:
	CLEANUP_AND_EXCEPTION_ON_ERROR(err);

	if (error_occured) {
		return NULL;
	}
	return PyLong_FromLong(0);
}

PyObject *AerospikeClient_MapPutItems(AerospikeClient *self, PyObject *args,
									  PyObject *kwds)
{
	BASE_VARIABLES

	as_static_pool static_pool;
	memset(&static_pool, 0, sizeof(static_pool));

	PyObject *py_items = NULL;
	PyObject *py_policy = NULL;
	PyObject *py_meta = NULL;
	PyObject *py_mapPolicy = NULL;
	as_map_policy map_policy;
	as_map_policy_init(&map_policy);
	as_record *rec = NULL;
	as_map *put_items = NULL;
	as_policy_operate operate_policy;
	as_policy_operate *operate_policy_p = NULL;
	bool error_occured = false;

	CHECK_CONNECTED();

	static char *kwlist[] = {"key",	 "bin",	   "items", "map_policy",
							 "meta", "policy", NULL};
	if (PyArg_ParseTupleAndKeywords(args, kwds, "OOO|OOO:map_put_items", kwlist,
									&py_key, &py_bin, &py_items, &py_mapPolicy,
									&py_meta, &py_policy) == false) {
		error_occured = true;
		goto CLEANUP;
	}

	POLICY_KEY_META_BIN();
	SETUP_MAP_POLICY();

	if (pyobject_to_map(self, &err, py_items, &put_items, &static_pool,
						SERIALIZER_PYTHON) != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	as_operations_add_map_put_items(&ops, bin, &map_policy, put_items);

	DO_OPERATION();

CLEANUP:
	CLEANUP_AND_EXCEPTION_ON_ERROR(err);
	if (error_occured) {
		return NULL;
	}
	return PyLong_FromLong(0);
}

PyObject *AerospikeClient_MapIncrement(AerospikeClient *self, PyObject *args,
									   PyObject *kwds)
{
	BASE_VARIABLES

	as_static_pool pool;
	memset(&pool, 0, sizeof(pool));

	PyObject *py_mapKey = NULL;
	PyObject *py_incr = NULL;
	PyObject *py_meta = NULL;
	PyObject *py_policy = NULL;
	PyObject *py_mapPolicy = NULL;
	as_record *rec = NULL;
	as_val *key_put;
	as_val *incr_put;
	as_map_policy map_policy;
	as_map_policy_init(&map_policy);
	as_policy_operate operate_policy;
	as_policy_operate *operate_policy_p = NULL;
	bool error_occured = false;

	CHECK_CONNECTED();

	static char *kwlist[] = {"key",		   "bin",  "map_key", "incr",
							 "map_policy", "meta", "policy",  NULL};
	if (PyArg_ParseTupleAndKeywords(args, kwds, "OOOO|OOO:map_increment",
									kwlist, &py_key, &py_bin, &py_mapKey,
									&py_incr, &py_mapPolicy, &py_meta,
									&py_policy) == false) {
		error_occured = true;
		goto CLEANUP;
	}

	POLICY_KEY_META_BIN();
	SETUP_MAP_POLICY();

	if (pyobject_to_val(self, &err, py_mapKey, &key_put, &pool,
						SERIALIZER_PYTHON) != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	if (pyobject_to_val(self, &err, py_incr, &incr_put, &pool,
						SERIALIZER_PYTHON) != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	as_operations_add_map_increment(&ops, bin, &map_policy, key_put, incr_put);
	DO_OPERATION();

CLEANUP:
	CLEANUP_AND_EXCEPTION_ON_ERROR(err);

	if (error_occured) {
		return NULL;
	}
	return PyLong_FromLong(0);
}

PyObject *AerospikeClient_MapDecrement(AerospikeClient *self, PyObject *args,
									   PyObject *kwds)
{
	BASE_VARIABLES

	as_static_pool pool;
	memset(&pool, 0, sizeof(pool));
	PyObject *py_mapKey = NULL;
	PyObject *py_decr = NULL;
	PyObject *py_meta = NULL;
	PyObject *py_policy = NULL;
	PyObject *py_mapPolicy = NULL;
	as_record *rec = NULL;
	as_val *key_put;
	as_val *decr_put;
	as_map_policy map_policy;
	as_map_policy_init(&map_policy);
	as_policy_operate operate_policy;
	as_policy_operate *operate_policy_p = NULL;
	bool error_occured = false;

	CHECK_CONNECTED();

	static char *kwlist[] = {"key",		   "bin",  "map_key", "decr",
							 "map_policy", "meta", "policy",  NULL};
	if (PyArg_ParseTupleAndKeywords(args, kwds, "OOOO|OOO:map_decrement",
									kwlist, &py_key, &py_bin, &py_mapKey,
									&py_decr, &py_meta, &py_policy) == false) {
		error_occured = true;
		goto CLEANUP;
	}

	POLICY_KEY_META_BIN();
	SETUP_MAP_POLICY();

	if (pyobject_to_val(self, &err, py_mapKey, &key_put, &pool,
						SERIALIZER_PYTHON) != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	if (pyobject_to_val(self, &err, py_decr, &decr_put, &pool,
						SERIALIZER_PYTHON) != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	as_operations_add_map_decrement(&ops, bin, &map_policy, key_put, decr_put);
	DO_OPERATION();

CLEANUP:
	CLEANUP_AND_EXCEPTION_ON_ERROR(err);

	if (error_occured) {
		return NULL;
	}
	return PyLong_FromLong(0);
}

PyObject *AerospikeClient_MapSize(AerospikeClient *self, PyObject *args,
								  PyObject *kwds)
{
	BASE_VARIABLES
	PyObject *py_meta = NULL;
	PyObject *py_policy = NULL;
	as_record *rec = NULL;
	int64_t size = 0;
	as_policy_operate operate_policy;
	as_policy_operate *operate_policy_p = NULL;
	bool error_occured = false;

	CHECK_CONNECTED();

	static char *kwlist[] = {"key", "bin", "meta", "policy", NULL};
	if (PyArg_ParseTupleAndKeywords(args, kwds, "OO|OO:map_size", kwlist,
									&py_key, &py_bin, &py_meta,
									&py_policy) == false) {
		error_occured = true;
		goto CLEANUP;
	}

	POLICY_KEY_META_BIN();

	as_operations_add_map_size(&ops, bin);
	DO_OPERATION();

	if (rec && rec->bins.entries && rec->bins.size > 0 &&
		as_val_type(rec->bins.entries[0].valuep) != AS_NIL) {
		size = rec->bins.entries[0].valuep->integer.value;
	}

CLEANUP:
	CLEANUP_AND_EXCEPTION_ON_ERROR(err);

	if (error_occured) {
		return NULL;
	}
	return PyLong_FromLong(size);
}

PyObject *AerospikeClient_MapClear(AerospikeClient *self, PyObject *args,
								   PyObject *kwds)
{
	BASE_VARIABLES
	PyObject *py_meta = NULL;
	PyObject *py_policy = NULL;
	as_policy_operate operate_policy;
	as_policy_operate *operate_policy_p = NULL;
	bool error_occured = false;
	as_record *rec = NULL;

	CHECK_CONNECTED();

	static char *kwlist[] = {"key", "bin", "meta", "policy", NULL};
	if (PyArg_ParseTupleAndKeywords(args, kwds, "OO|OO:map_clear", kwlist,
									&py_key, &py_bin, &py_meta,
									&py_policy) == false) {
		error_occured = true;
		goto CLEANUP;
	}

	POLICY_KEY_META_BIN();

	as_operations_add_map_clear(&ops, bin);
	DO_OPERATION();

CLEANUP:
	CLEANUP_AND_EXCEPTION_ON_ERROR(err);
	if (error_occured) {
		return NULL;
	}
	return PyLong_FromLong(0);
}

PyObject *AerospikeClient_MapRemoveByKey(AerospikeClient *self, PyObject *args,
										 PyObject *kwds)
{
	BASE_VARIABLES

	as_static_pool pool;
	memset(&pool, 0, sizeof(pool));

	PyObject *py_result = NULL;
	PyObject *py_mapKey = NULL;
	PyObject *py_meta = NULL;
	PyObject *py_policy = NULL;
	PyObject *py_inverted = NULL;

	uint64_t returnType;
	as_record *rec = NULL;
	as_val *key_put;
	as_policy_operate operate_policy;
	as_policy_operate *operate_policy_p = NULL;

	CHECK_CONNECTED();

	static char *kwlist[] = {"key",	 "bin",	   "map_key",  "return_type",
							 "meta", "policy", "inverted", NULL};
	if (PyArg_ParseTupleAndKeywords(args, kwds, "OOOl|OOO:map_remove_by_key",
									kwlist, &py_key, &py_bin, &py_mapKey,
									&returnType, &py_meta, &py_policy,
									&py_inverted) == false) {
		goto CLEANUP;
	}

	if (invertIfSpecified(&err, py_inverted, &returnType) != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	POLICY_KEY_META_BIN();

	if (pyobject_to_val(self, &err, py_mapKey, &key_put, &pool,
						SERIALIZER_PYTHON) != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	as_operations_add_map_remove_by_key(&ops, bin, key_put, returnType);

	DO_OPERATION();

	SETUP_RETURN_VAL();

CLEANUP:
	CLEANUP_AND_EXCEPTION_ON_ERROR(err);

	return py_result;
}

PyObject *AerospikeClient_MapRemoveByKeyList(AerospikeClient *self,
											 PyObject *args, PyObject *kwds)
{
	BASE_VARIABLES

	as_static_pool pool;
	memset(&pool, 0, sizeof(pool));

	PyObject *py_result = NULL;
	PyObject *py_list = NULL;
	PyObject *py_meta = NULL;
	PyObject *py_policy = NULL;
	PyObject *py_inverted = NULL;
	uint64_t returnType;
	as_record *rec = NULL;
	as_val *list_put;
	as_policy_operate operate_policy;
	as_policy_operate *operate_policy_p = NULL;

	CHECK_CONNECTED();

	static char *kwlist[] = {"key",	 "bin",	   "list",	   "return_type",
							 "meta", "policy", "inverted", NULL};
	if (PyArg_ParseTupleAndKeywords(
			args, kwds, "OOOl|OOO:map_remove_by_key_list", kwlist, &py_key,
			&py_bin, &py_list, &returnType, &py_meta, &py_policy,
			&py_inverted) == false) {
		goto CLEANUP;
	}

	if (invertIfSpecified(&err, py_inverted, &returnType) != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	POLICY_KEY_META_BIN();

	if (!PyList_Check(py_list)) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM,
						"List parameter should be of type list");
		goto CLEANUP;
	}

	if (pyobject_to_val(self, &err, py_list, &list_put, &pool,
						SERIALIZER_PYTHON) != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	as_operations_add_map_remove_by_key_list(&ops, bin, (as_list *)list_put,
											 returnType);

	DO_OPERATION();

	SETUP_RETURN_VAL();

CLEANUP:
	CLEANUP_AND_EXCEPTION_ON_ERROR(err);

	return py_result;
}

PyObject *AerospikeClient_MapRemoveByKeyRange(AerospikeClient *self,
											  PyObject *args, PyObject *kwds)
{
	BASE_VARIABLES

	as_static_pool pool;
	memset(&pool, 0, sizeof(pool));

	PyObject *py_mapKey = NULL;
	PyObject *py_result = NULL;
	PyObject *py_range = NULL;
	PyObject *py_meta = NULL;
	PyObject *py_policy = NULL;
	PyObject *py_inverted = NULL;

	uint64_t returnType;
	as_record *rec = NULL;
	as_val *key_put;
	as_val *range_put;
	as_policy_operate operate_policy;
	as_policy_operate *operate_policy_p = NULL;

	CHECK_CONNECTED();

	static char *kwlist[] = {"key",	   "bin",		  "map_key",
							 "range",  "return_type", "meta",
							 "policy", "inverted",	  NULL};
	if (PyArg_ParseTupleAndKeywords(
			args, kwds, "OOOOl|OOO:map_remove_by_key_range", kwlist, &py_key,
			&py_bin, &py_mapKey, &py_range, &returnType, &py_meta, &py_policy,
			&py_inverted) == false) {
		goto CLEANUP;
	}

	if (invertIfSpecified(&err, py_inverted, &returnType) != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	POLICY_KEY_META_BIN();

	if (pyobject_to_val(self, &err, py_mapKey, &key_put, &pool,
						SERIALIZER_PYTHON) != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	if (pyobject_to_val(self, &err, py_range, &range_put, &pool,
						SERIALIZER_PYTHON) != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	as_operations_add_map_remove_by_key_range(&ops, bin, key_put, range_put,
											  returnType);

	DO_OPERATION();

	SETUP_RETURN_VAL();

CLEANUP:
	CLEANUP_AND_EXCEPTION_ON_ERROR(err);

	return py_result;
}

PyObject *AerospikeClient_MapRemoveByValue(AerospikeClient *self,
										   PyObject *args, PyObject *kwds)
{
	BASE_VARIABLES

	as_static_pool pool;
	memset(&pool, 0, sizeof(pool));

	PyObject *py_mapValue = NULL;
	PyObject *py_result = NULL;
	PyObject *py_meta = NULL;
	PyObject *py_policy = NULL;
	PyObject *py_inverted = NULL;
	uint64_t returnType;
	as_record *rec = NULL;
	as_val *value_put;
	as_policy_operate operate_policy;
	as_policy_operate *operate_policy_p = NULL;

	CHECK_CONNECTED();

	static char *kwlist[] = {"key",	 "bin",	   "val",	   "return_type",
							 "meta", "policy", "inverted", NULL};
	if (PyArg_ParseTupleAndKeywords(args, kwds, "OOOl|OOO:map_remove_by_value",
									kwlist, &py_key, &py_bin, &py_mapValue,
									&returnType, &py_meta, &py_policy,
									&py_inverted) == false) {
		goto CLEANUP;
	}

	if (invertIfSpecified(&err, py_inverted, &returnType) != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	POLICY_KEY_META_BIN();

	if (pyobject_to_val(self, &err, py_mapValue, &value_put, &pool,
						SERIALIZER_PYTHON) != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	as_operations_add_map_remove_by_value(&ops, bin, value_put, returnType);

	DO_OPERATION();

	SETUP_RETURN_VAL();

CLEANUP:
	CLEANUP_AND_EXCEPTION_ON_ERROR(err);

	return py_result;
}

PyObject *AerospikeClient_MapRemoveByValueList(AerospikeClient *self,
											   PyObject *args, PyObject *kwds)
{
	BASE_VARIABLES

	as_static_pool pool;
	memset(&pool, 0, sizeof(pool));

	PyObject *py_result = NULL;
	PyObject *py_list = NULL;
	PyObject *py_meta = NULL;
	PyObject *py_policy = NULL;
	PyObject *py_inverted = NULL;

	uint64_t returnType;
	as_record *rec = NULL;
	as_val *list_put;
	as_policy_operate operate_policy;
	as_policy_operate *operate_policy_p = NULL;

	CHECK_CONNECTED();

	static char *kwlist[] = {"key",	 "bin",	   "list",	   "return_type",
							 "meta", "policy", "inverted", NULL};
	if (PyArg_ParseTupleAndKeywords(
			args, kwds, "OOOl|OOO:map_remove_by_value_list", kwlist, &py_key,
			&py_bin, &py_list, &returnType, &py_meta, &py_policy,
			&py_inverted) == false) {
		goto CLEANUP;
	}

	if (invertIfSpecified(&err, py_inverted, &returnType) != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	POLICY_KEY_META_BIN();

	if (!PyList_Check(py_list)) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM,
						"List parameter should be of type list");
		goto CLEANUP;
	}

	if (pyobject_to_val(self, &err, py_list, &list_put, &pool,
						SERIALIZER_PYTHON) != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	as_operations_add_map_remove_by_value_list(&ops, bin, (as_list *)list_put,
											   returnType);

	DO_OPERATION();

	SETUP_RETURN_VAL();

CLEANUP:
	CLEANUP_AND_EXCEPTION_ON_ERROR(err);

	return py_result;
}

PyObject *AerospikeClient_MapRemoveByValueRange(AerospikeClient *self,
												PyObject *args, PyObject *kwds)
{
	BASE_VARIABLES

	as_static_pool pool;
	memset(&pool, 0, sizeof(pool));

	PyObject *py_mapValue = NULL;
	PyObject *py_result = NULL;
	PyObject *py_range = NULL;
	PyObject *py_meta = NULL;
	PyObject *py_policy = NULL;
	PyObject *py_inverted = NULL;

	uint64_t returnType;
	as_record *rec = NULL;
	as_val *value_put;
	as_val *range_put;
	as_policy_operate operate_policy;
	as_policy_operate *operate_policy_p = NULL;

	CHECK_CONNECTED();

	static char *kwlist[] = {"key",	   "bin",		  "val",
							 "range",  "return_type", "meta",
							 "policy", "inverted",	  NULL};
	if (PyArg_ParseTupleAndKeywords(
			args, kwds, "OOOOl|OOO:map_remove_by_value_range", kwlist, &py_key,
			&py_bin, &py_mapValue, &py_range, &returnType, &py_meta, &py_policy,
			&py_inverted) == false) {
		goto CLEANUP;
	}

	if (invertIfSpecified(&err, py_inverted, &returnType) != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	POLICY_KEY_META_BIN();

	if (pyobject_to_val(self, &err, py_mapValue, &value_put, &pool,
						SERIALIZER_PYTHON) != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	if (pyobject_to_val(self, &err, py_range, &range_put, &pool,
						SERIALIZER_PYTHON) != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	as_operations_add_map_remove_by_value_range(&ops, bin, value_put, range_put,
												returnType);

	DO_OPERATION();

	SETUP_RETURN_VAL();

CLEANUP:
	CLEANUP_AND_EXCEPTION_ON_ERROR(err);

	return py_result;
}

PyObject *AerospikeClient_MapRemoveByIndex(AerospikeClient *self,
										   PyObject *args, PyObject *kwds)
{
	BASE_VARIABLES

	PyObject *py_result = NULL;
	PyObject *py_meta = NULL;
	PyObject *py_policy = NULL;
	PyObject *py_inverted = NULL;

	uint64_t index;
	uint64_t returnType;
	as_record *rec = NULL;
	as_policy_operate operate_policy;
	as_policy_operate *operate_policy_p = NULL;

	CHECK_CONNECTED();

	static char *kwlist[] = {"key",	 "bin",	   "index",	   "return_type",
							 "meta", "policy", "inverted", NULL};
	if (PyArg_ParseTupleAndKeywords(args, kwds, "OOll|OOO:map_remove_by_index",
									kwlist, &py_key, &py_bin, &index,
									&returnType, &py_meta, &py_policy,
									&py_inverted) == false) {
		goto CLEANUP;
	}

	if (invertIfSpecified(&err, py_inverted, &returnType) != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	POLICY_KEY_META_BIN();

	as_operations_add_map_remove_by_index(&ops, bin, index, returnType);
	DO_OPERATION();

	SETUP_RETURN_VAL();

CLEANUP:
	CLEANUP_AND_EXCEPTION_ON_ERROR(err);

	return py_result;
}

PyObject *AerospikeClient_MapRemoveByIndexRange(AerospikeClient *self,
												PyObject *args, PyObject *kwds)
{
	BASE_VARIABLES

	PyObject *py_result = NULL;
	PyObject *py_meta = NULL;
	PyObject *py_policy = NULL;
	PyObject *py_inverted = NULL;
	uint64_t returnType;
	uint64_t index;
	uint64_t range;
	as_record *rec = NULL;
	as_policy_operate operate_policy;
	as_policy_operate *operate_policy_p = NULL;

	CHECK_CONNECTED();

	static char *kwlist[] = {"key",	   "bin",		  "index",
							 "range",  "return_type", "meta",
							 "policy", "inverted",	  NULL};
	if (PyArg_ParseTupleAndKeywords(
			args, kwds, "OOlll|OOO:map_remove_by_index_range", kwlist, &py_key,
			&py_bin, &index, &range, &returnType, &py_meta, &py_policy,
			&py_inverted) == false) {
		goto CLEANUP;
	}

	if (invertIfSpecified(&err, py_inverted, &returnType) != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	POLICY_KEY_META_BIN();

	as_operations_add_map_remove_by_index_range(&ops, bin, index, range,
												returnType);
	DO_OPERATION();

	SETUP_RETURN_VAL();

CLEANUP:
	CLEANUP_AND_EXCEPTION_ON_ERROR(err);

	return py_result;
}

PyObject *AerospikeClient_MapRemoveByRank(AerospikeClient *self, PyObject *args,
										  PyObject *kwds)
{
	BASE_VARIABLES

	PyObject *py_result = NULL;
	PyObject *py_meta = NULL;
	PyObject *py_policy = NULL;
	PyObject *py_inverted = NULL;
	uint64_t rank;
	uint64_t returnType;
	as_record *rec = NULL;
	as_policy_operate operate_policy;
	as_policy_operate *operate_policy_p = NULL;

	CHECK_CONNECTED();

	static char *kwlist[] = {"key",	 "bin",	   "rank",	   "return_type",
							 "meta", "policy", "inverted", NULL};
	if (PyArg_ParseTupleAndKeywords(
			args, kwds, "OOll|OOO:map_remove_by_rank", kwlist, &py_key, &py_bin,
			&rank, &returnType, &py_meta, &py_policy, &py_inverted) == false) {
		goto CLEANUP;
	}

	if (invertIfSpecified(&err, py_inverted, &returnType) != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	POLICY_KEY_META_BIN();

	as_operations_add_map_remove_by_rank(&ops, bin, rank, returnType);
	DO_OPERATION();

	SETUP_RETURN_VAL();

CLEANUP:
	CLEANUP_AND_EXCEPTION_ON_ERROR(err);

	return py_result;
}

PyObject *AerospikeClient_MapRemoveByRankRange(AerospikeClient *self,
											   PyObject *args, PyObject *kwds)
{
	BASE_VARIABLES

	PyObject *py_result = NULL;
	PyObject *py_meta = NULL;
	PyObject *py_policy = NULL;
	PyObject *py_inverted = NULL;

	uint64_t returnType;
	uint64_t rank;
	uint64_t range;
	as_record *rec = NULL;
	as_policy_operate operate_policy;
	as_policy_operate *operate_policy_p = NULL;

	CHECK_CONNECTED();

	static char *kwlist[] = {"key",	   "bin",		  "rank",
							 "range",  "return_type", "meta",
							 "policy", "inverted",	  NULL};
	if (PyArg_ParseTupleAndKeywords(
			args, kwds, "OOlll|OOO:map_remove_by_rank_range", kwlist, &py_key,
			&py_bin, &rank, &range, &returnType, &py_meta, &py_policy,
			&py_inverted) == false) {
		goto CLEANUP;
	}

	if (invertIfSpecified(&err, py_inverted, &returnType) != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	POLICY_KEY_META_BIN();

	as_operations_add_map_remove_by_rank_range(&ops, bin, rank, range,
											   returnType);
	DO_OPERATION();

	SETUP_RETURN_VAL();

CLEANUP:
	CLEANUP_AND_EXCEPTION_ON_ERROR(err);

	return py_result;
}

PyObject *AerospikeClient_MapGetByKey(AerospikeClient *self, PyObject *args,
									  PyObject *kwds)
{
	BASE_VARIABLES

	as_static_pool pool;
	memset(&pool, 0, sizeof(pool));

	PyObject *py_mapKey = NULL;
	PyObject *py_result = NULL;
	PyObject *py_meta = NULL;
	PyObject *py_policy = NULL;
	PyObject *py_inverted = NULL;
	uint64_t returnType;
	as_record *rec = NULL;
	as_val *key_put;
	as_policy_operate operate_policy;
	as_policy_operate *operate_policy_p = NULL;

	CHECK_CONNECTED();

	static char *kwlist[] = {"key",	 "bin",	   "map_key",  "return_type",
							 "meta", "policy", "inverted", NULL};
	if (PyArg_ParseTupleAndKeywords(args, kwds, "OOOl|OOO:map_get_by_key",
									kwlist, &py_key, &py_bin, &py_mapKey,
									&returnType, &py_meta, &py_policy,
									&py_inverted) == false) {
		goto CLEANUP;
	}

	if (invertIfSpecified(&err, py_inverted, &returnType) != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	POLICY_KEY_META_BIN();

	if (pyobject_to_val(self, &err, py_mapKey, &key_put, &pool,
						SERIALIZER_PYTHON) != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	as_operations_add_map_get_by_key(&ops, bin, key_put, returnType);

	DO_OPERATION();

	SETUP_RETURN_VAL();

CLEANUP:
	CLEANUP_AND_EXCEPTION_ON_ERROR(err);

	return py_result;
}

PyObject *AerospikeClient_MapGetByValue(AerospikeClient *self, PyObject *args,
										PyObject *kwds)
{
	BASE_VARIABLES

	as_static_pool pool;
	memset(&pool, 0, sizeof(pool));

	PyObject *py_mapValue = NULL;
	PyObject *py_result = NULL;
	PyObject *py_meta = NULL;
	PyObject *py_policy = NULL;
	PyObject *py_inverted = NULL;

	uint64_t returnType;
	as_record *rec = NULL;
	as_val *value_put;
	as_policy_operate operate_policy;
	as_policy_operate *operate_policy_p = NULL;

	CHECK_CONNECTED();

	static char *kwlist[] = {"key",	 "bin",	   "val",	   "return_type",
							 "meta", "policy", "inverted", NULL};
	if (PyArg_ParseTupleAndKeywords(args, kwds, "OOOl|OOO:map_get_by_value",
									kwlist, &py_key, &py_bin, &py_mapValue,
									&returnType, &py_meta, &py_policy,
									&py_inverted) == false) {
		goto CLEANUP;
	}

	if (invertIfSpecified(&err, py_inverted, &returnType) != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	POLICY_KEY_META_BIN();

	if (pyobject_to_val(self, &err, py_mapValue, &value_put, &pool,
						SERIALIZER_PYTHON) != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	as_operations_add_map_get_by_value(&ops, bin, value_put, returnType);

	DO_OPERATION();

	SETUP_RETURN_VAL();

CLEANUP:
	CLEANUP_AND_EXCEPTION_ON_ERROR(err);

	return py_result;
}

PyObject *AerospikeClient_MapGetByKeyRange(AerospikeClient *self,
										   PyObject *args, PyObject *kwds)
{
	BASE_VARIABLES

	as_static_pool pool;
	memset(&pool, 0, sizeof(pool));

	PyObject *py_mapKey = NULL;
	PyObject *py_result = NULL;
	PyObject *py_meta = NULL;
	PyObject *py_policy = NULL;
	PyObject *py_range = NULL;
	PyObject *py_inverted = NULL;
	uint64_t returnType;
	as_record *rec = NULL;
	as_val *map_key;
	as_val *range_put;
	as_policy_operate operate_policy;
	as_policy_operate *operate_policy_p = NULL;

	CHECK_CONNECTED();

	static char *kwlist[] = {"key",	   "bin",		  "map_key",
							 "range",  "return_type", "meta",
							 "policy", "inverted",	  NULL};
	if (PyArg_ParseTupleAndKeywords(
			args, kwds, "OOOOl|OOO:map_get_by_key_range", kwlist, &py_key,
			&py_bin, &py_mapKey, &py_range, &returnType, &py_meta, &py_policy,
			&py_inverted) == false) {
		goto CLEANUP;
	}

	if (invertIfSpecified(&err, py_inverted, &returnType) != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	POLICY_KEY_META_BIN();

	if (pyobject_to_val(self, &err, py_mapKey, &map_key, &pool,
						SERIALIZER_PYTHON) != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	if (pyobject_to_val(self, &err, py_range, &range_put, &pool,
						SERIALIZER_PYTHON) != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	as_operations_add_map_get_by_key_range(&ops, bin, map_key, range_put,
										   returnType);

	DO_OPERATION();

	SETUP_RETURN_VAL();

CLEANUP:
	CLEANUP_AND_EXCEPTION_ON_ERROR(err);

	return py_result;
}

PyObject *AerospikeClient_MapGetByValueRange(AerospikeClient *self,
											 PyObject *args, PyObject *kwds)
{
	BASE_VARIABLES

	as_static_pool pool;
	memset(&pool, 0, sizeof(pool));

	PyObject *py_mapValue = NULL;
	PyObject *py_result = NULL;
	PyObject *py_meta = NULL;
	PyObject *py_policy = NULL;
	PyObject *py_range = NULL;
	PyObject *py_inverted = NULL;
	uint64_t returnType;
	as_record *rec = NULL;
	as_val *value_put;
	as_val *range_put;
	as_policy_operate operate_policy;
	as_policy_operate *operate_policy_p = NULL;

	CHECK_CONNECTED();

	static char *kwlist[] = {"key",	   "bin",		  "val",
							 "range",  "return_type", "meta",
							 "policy", "inverted",	  NULL};
	if (PyArg_ParseTupleAndKeywords(
			args, kwds, "OOOOl|OOO:map_get_by_value_range", kwlist, &py_key,
			&py_bin, &py_mapValue, &py_range, &returnType, &py_meta, &py_policy,
			&py_inverted) == false) {
		goto CLEANUP;
	}

	if (invertIfSpecified(&err, py_inverted, &returnType) != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	POLICY_KEY_META_BIN();

	if (pyobject_to_val(self, &err, py_mapValue, &value_put, &pool,
						SERIALIZER_PYTHON) != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	if (pyobject_to_val(self, &err, py_range, &range_put, &pool,
						SERIALIZER_PYTHON) != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	as_operations_add_map_get_by_value_range(&ops, bin, value_put, range_put,
											 returnType);

	DO_OPERATION();

	SETUP_RETURN_VAL();

CLEANUP:
	CLEANUP_AND_EXCEPTION_ON_ERROR(err);

	return py_result;
}

/*
 * key = ('test', 'demo', 1)
 * res = client.map_get_by_value_list(key, 'map_bin', ['val1', 'val2'], aerospike.MAP_RETURN_VALUE)
 */
PyObject *AerospikeClient_MapGetByValueList(AerospikeClient *self,
											PyObject *args, PyObject *kwds)
{
	BASE_VARIABLES

	// Parameter vars
	PyObject *py_meta = NULL;
	PyObject *py_policy = NULL;
	PyObject *py_value_list = NULL;
	PyObject *py_inverted = NULL;
	uint64_t returnType;

	// C client function arg vars
	as_policy_operate operate_policy;
	as_policy_operate *operate_policy_p = NULL;
	as_val *as_value_list = NULL;
	as_record *rec = NULL;

	// Return Vars
	PyObject *py_result = NULL;

	//Util Vars
	as_static_pool pool;
	memset(&pool, 0, sizeof(pool));

	CHECK_CONNECTED();

	static char *kwlist[] = {"key",	 "bin",	   "value_list", "return_type",
							 "meta", "policy", "inverted",	 NULL};
	if (PyArg_ParseTupleAndKeywords(
			args, kwds, "OOOl|OOO:map_get_by_value_list", kwlist, &py_key,
			&py_bin, &py_value_list, &returnType, &py_meta, &py_policy,
			&py_inverted) == false) {
		goto CLEANUP;
	}

	if (invertIfSpecified(&err, py_inverted, &returnType) != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	// Initialize the variables
	POLICY_KEY_META_BIN()

	if (!py_value_list || !PyList_Check(py_value_list)) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM,
						"type of value_list must be list");
		goto CLEANUP;
	}

	if (pyobject_to_val(self, &err, py_value_list, &as_value_list, &pool,
						SERIALIZER_PYTHON) != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	if (!as_list_fromval(as_value_list)) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM,
						"Failed to convert Python list");
		goto CLEANUP;
	}

	if (!as_operations_add_map_get_by_value_list(
			&ops, bin, as_list_fromval(as_value_list),
			(as_map_return_type)returnType)) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM,
						"Failed to add map_get_by_value_list operation");
		goto CLEANUP;
	}

	DO_OPERATION()
	SETUP_RETURN_VAL()

CLEANUP:
	CLEANUP_AND_EXCEPTION_ON_ERROR(err);
	return py_result;
}

/*
 * key = ('test', 'demo', 1)
 * res = client.map_get_by_key_list(key, 'map_bin', ['key1', 'key2'], aerospike.MAP_RETURN_VALUE)
 */
PyObject *AerospikeClient_MapGetByKeyList(AerospikeClient *self, PyObject *args,
										  PyObject *kwds)
{
	BASE_VARIABLES

	// Parameter vars
	PyObject *py_meta = NULL;
	PyObject *py_policy = NULL;
	PyObject *py_key_list = NULL;
	PyObject *py_inverted = NULL;
	uint64_t returnType;

	// C client function arg vars
	as_policy_operate operate_policy;
	as_policy_operate *operate_policy_p = NULL;
	as_val *as_key_list = NULL;
	as_record *rec = NULL;

	// Return Vars
	PyObject *py_result = NULL;

	//Util Vars
	as_static_pool pool;
	memset(&pool, 0, sizeof(pool));

	CHECK_CONNECTED();

	static char *kwlist[] = {"key",	 "bin",	   "key_list", "return_type",
							 "meta", "policy", "inverted", NULL};
	if (PyArg_ParseTupleAndKeywords(args, kwds, "OOOl|OOO:map_get_by_key_list",
									kwlist, &py_key, &py_bin, &py_key_list,
									&returnType, &py_meta, &py_policy,
									&py_inverted) == false) {
		goto CLEANUP;
	}

	if (invertIfSpecified(&err, py_inverted, &returnType) != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	// Initialize the variables
	POLICY_KEY_META_BIN()

	if (!py_key_list || !PyList_Check(py_key_list)) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM,
						"type of key_list must be list");
		goto CLEANUP;
	}

	if (pyobject_to_val(self, &err, py_key_list, &as_key_list, &pool,
						SERIALIZER_PYTHON) != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	if (!as_list_fromval(as_key_list)) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM,
						"Failed to convert Python list");
		goto CLEANUP;
	}

	if (!as_operations_add_map_get_by_key_list(
			&ops, bin, as_list_fromval(as_key_list),
			(as_map_return_type)returnType)) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM,
						"Failed to add map_get_by_key_list operation");
		goto CLEANUP;
	}

	DO_OPERATION()
	SETUP_RETURN_VAL()

CLEANUP:
	CLEANUP_AND_EXCEPTION_ON_ERROR(err);
	return py_result;
}

PyObject *AerospikeClient_MapGetByIndex(AerospikeClient *self, PyObject *args,
										PyObject *kwds)
{
	BASE_VARIABLES

	PyObject *py_result = NULL;
	PyObject *py_meta = NULL;
	PyObject *py_policy = NULL;
	PyObject *py_inverted = NULL;
	uint64_t returnType;
	uint64_t index;
	as_record *rec = NULL;
	as_policy_operate operate_policy;
	as_policy_operate *operate_policy_p = NULL;

	CHECK_CONNECTED();

	static char *kwlist[] = {"key",	 "bin",	   "index",	   "return_type",
							 "meta", "policy", "inverted", NULL};
	if (PyArg_ParseTupleAndKeywords(
			args, kwds, "OOll|OOO:map_get_by_index", kwlist, &py_key, &py_bin,
			&index, &returnType, &py_meta, &py_policy, &py_inverted) == false) {
		goto CLEANUP;
	}

	if (invertIfSpecified(&err, py_inverted, &returnType) != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	POLICY_KEY_META_BIN();

	as_operations_add_map_get_by_index(&ops, bin, index, returnType);
	DO_OPERATION();

	SETUP_RETURN_VAL();

CLEANUP:
	CLEANUP_AND_EXCEPTION_ON_ERROR(err);

	return py_result;
}

PyObject *AerospikeClient_MapGetByIndexRange(AerospikeClient *self,
											 PyObject *args, PyObject *kwds)
{
	BASE_VARIABLES

	PyObject *py_result = NULL;
	PyObject *py_meta = NULL;
	PyObject *py_policy = NULL;
	PyObject *py_inverted = NULL;
	uint64_t returnType;
	uint64_t index;
	uint64_t range;
	as_record *rec = NULL;
	as_policy_operate operate_policy;
	as_policy_operate *operate_policy_p = NULL;

	CHECK_CONNECTED();

	static char *kwlist[] = {"key",	   "bin",		  "index",
							 "range",  "return_type", "meta",
							 "policy", "inverted",	  NULL};
	if (PyArg_ParseTupleAndKeywords(
			args, kwds, "OOlll|OOO:map_get_by_index_range", kwlist, &py_key,
			&py_bin, &index, &range, &returnType, &py_meta, &py_policy,
			&py_inverted) == false) {
		goto CLEANUP;
	}

	if (invertIfSpecified(&err, py_inverted, &returnType) != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	POLICY_KEY_META_BIN();

	as_operations_add_map_get_by_index_range(&ops, bin, index, range,
											 returnType);
	DO_OPERATION();

	SETUP_RETURN_VAL();

CLEANUP:
	CLEANUP_AND_EXCEPTION_ON_ERROR(err);

	return py_result;
}

PyObject *AerospikeClient_MapGetByRank(AerospikeClient *self, PyObject *args,
									   PyObject *kwds)
{
	BASE_VARIABLES

	PyObject *py_result = NULL;
	PyObject *py_meta = NULL;
	PyObject *py_policy = NULL;
	PyObject *py_inverted = NULL;

	uint64_t returnType;
	uint64_t rank;
	as_record *rec = NULL;
	as_policy_operate operate_policy;
	as_policy_operate *operate_policy_p = NULL;

	CHECK_CONNECTED();

	static char *kwlist[] = {"key",	 "bin",	   "rank",	   "return_type",
							 "meta", "policy", "inverted", NULL};
	if (PyArg_ParseTupleAndKeywords(
			args, kwds, "OOll|OOO:map_get_by_rank", kwlist, &py_key, &py_bin,
			&rank, &returnType, &py_meta, &py_policy, &py_inverted) == false) {
		goto CLEANUP;
	}

	if (invertIfSpecified(&err, py_inverted, &returnType) != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	POLICY_KEY_META_BIN();

	as_operations_add_map_get_by_rank(&ops, bin, rank, returnType);
	DO_OPERATION();

	SETUP_RETURN_VAL();

CLEANUP:
	CLEANUP_AND_EXCEPTION_ON_ERROR(err);

	return py_result;
}

PyObject *AerospikeClient_MapGetByRankRange(AerospikeClient *self,
											PyObject *args, PyObject *kwds)
{
	BASE_VARIABLES

	PyObject *py_result = NULL;
	PyObject *py_meta = NULL;
	PyObject *py_policy = NULL;
	PyObject *py_inverted = NULL;
	uint64_t returnType;
	uint64_t rank;
	uint64_t range;
	as_record *rec = NULL;
	as_policy_operate operate_policy;
	as_policy_operate *operate_policy_p = NULL;

	CHECK_CONNECTED();

	static char *kwlist[] = {"key",	   "bin",		  "rank",
							 "range",  "return_type", "meta",
							 "policy", "inverted",	  NULL};
	if (PyArg_ParseTupleAndKeywords(
			args, kwds, "OOlll|OOO:map_get_by_rank_range", kwlist, &py_key,
			&py_bin, &rank, &range, &returnType, &py_meta, &py_policy,
			&py_inverted) == false) {
		goto CLEANUP;
	}

	if (invertIfSpecified(&err, py_inverted, &returnType) != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	POLICY_KEY_META_BIN();

	as_operations_add_map_get_by_rank_range(&ops, bin, rank, range, returnType);
	DO_OPERATION();

	SETUP_RETURN_VAL();

CLEANUP:
	CLEANUP_AND_EXCEPTION_ON_ERROR(err);

	return py_result;
}

static as_status invertIfSpecified(as_error *err, PyObject *py_inverted,
								   uint64_t *returnType)
{
	if (!py_inverted) {
		return AEROSPIKE_OK;
	}

	int truthValue = PyObject_IsTrue(py_inverted);

	/* An error ocurred, update the flag */
	if (truthValue == -1) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM,
							   "Invalid inverted value");
	}

	if (truthValue) {
		*returnType |= AS_MAP_RETURN_INVERTED;
	}

	return AEROSPIKE_OK;
}
