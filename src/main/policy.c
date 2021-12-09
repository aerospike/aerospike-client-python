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

#include <aerospike/as_error.h>
#include <aerospike/as_exp.h>
#include <aerospike/as_predexp.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>
#include <aerospike/as_admin.h>
#include <aerospike/as_operations.h>
#include <aerospike/as_exp_operations.h>
#include <aerospike/aerospike_index.h>
#include "aerospike/as_scan.h"
#include "aerospike/as_job.h"

#include "conversions.h"
#include "policy.h"
#include "macros.h"

#define MAP_WRITE_FLAGS_KEY "map_write_flags"
#define BIT_WRITE_FLAGS_KEY "bit_write_flags"

#define POLICY_INIT(__policy)                                                  \
	as_error_reset(err);                                                       \
	if (!py_policy || py_policy == Py_None) {                                  \
		return err->code;                                                      \
	}                                                                          \
	if (!PyDict_Check(py_policy)) {                                            \
		return as_error_update(err, AEROSPIKE_ERR_PARAM,                       \
							   "policy must be a dict");                       \
	}                                                                          \
	__policy##_init(policy);

#define POLICY_UPDATE() *policy_p = policy;

#define POLICY_SET_FIELD(__field, __type)                                      \
	{                                                                          \
		PyObject *py_field = PyDict_GetItemString(py_policy, #__field);        \
		if (py_field) {                                                        \
			if (PyInt_Check(py_field)) {                                       \
				policy->__field = (__type)PyInt_AsLong(py_field);              \
			}                                                                  \
			else {                                                             \
				return as_error_update(err, AEROSPIKE_ERR_PARAM,               \
									   "%s is invalid", #__field);             \
			}                                                                  \
		}                                                                      \
	}

#define POLICY_SET_BASE_FIELD(__field, __type)                                 \
	{                                                                          \
		PyObject *py_field = PyDict_GetItemString(py_policy, #__field);        \
		if (py_field) {                                                        \
			if (PyInt_Check(py_field)) {                                       \
				policy->base.__field = (__type)PyInt_AsLong(py_field);         \
			}                                                                  \
			else {                                                             \
				return as_error_update(err, AEROSPIKE_ERR_PARAM,               \
									   "%s is invalid", #__field);             \
			}                                                                  \
		}                                                                      \
	}

#define POLICY_SET_EXPRESSIONS_BASE_FIELD()                                    \
	{                                                                          \
		if (exp_list) {                                                        \
			PyObject *py_exp_list =                                            \
				PyDict_GetItemString(py_policy, "expressions");                \
			if (py_exp_list) {                                                 \
				if (convert_exp_list(self, py_exp_list, &exp_list, err) ==     \
					AEROSPIKE_OK) {                                            \
					policy->base.filter_exp = exp_list;                        \
					*exp_list_p = exp_list;                                    \
				}                                                              \
			}                                                                  \
		}                                                                      \
	}

#define POLICY_SET_PREDEXP_BASE_FIELD()                                        \
	{                                                                          \
		if (predexp_list) {                                                    \
			PyObject *py_predexp_list =                                        \
				PyDict_GetItemString(py_policy, "predexp");                    \
			if (py_predexp_list) {                                             \
				if (PyDict_GetItemString(py_policy, "expressions")) {          \
					return as_error_update(                                    \
						err, AEROSPIKE_ERR_PARAM,                              \
						"Got both predexp and expressions, can only handle "   \
						"one or the other.");                                  \
				}                                                              \
				long number_predexp = PyList_Size(py_predexp_list);            \
				as_predexp_list_init(predexp_list, number_predexp);            \
				convert_predexp_list(py_predexp_list, predexp_list, err);      \
				policy->base.predexp = predexp_list;                           \
				*predexp_list_p = predexp_list;                                \
			}                                                                  \
		}                                                                      \
	}

/* This allows the old timeout field to properly set timeout, remove in future release: > 3.1 */
#define POLICY_SET_TOTAL_TIMEOUT_FROM_TIMEOUT()                                \
	{                                                                          \
		PyObject *py_field = PyDict_GetItemString(py_policy, "timeout");       \
		if (py_field) {                                                        \
			if (PyInt_Check(py_field)) {                                       \
				policy->base.total_timeout = (uint32_t)PyInt_AsLong(py_field); \
			}                                                                  \
			else {                                                             \
				return as_error_update(err, AEROSPIKE_ERR_PARAM,               \
									   "timeout is invalid");                  \
			}                                                                  \
		}                                                                      \
	}

#define MAP_POLICY_SET_FIELD(__field)                                          \
	{                                                                          \
		PyObject *py_field = PyDict_GetItemString(py_policy, #__field);        \
		if (py_field) {                                                        \
			if (PyInt_Check(py_field)) {                                       \
				__field = PyInt_AsLong(py_field);                              \
			}                                                                  \
			else {                                                             \
				return as_error_update(err, AEROSPIKE_ERR_PARAM,               \
									   "%s is invalid", #__field);             \
			}                                                                  \
		}                                                                      \
	}

/*
 *******************************************************************************************************
 * Mapping of constant number to constant name string.
 *******************************************************************************************************
 */
static AerospikeConstants aerospike_constants[] = {
	{AS_POLICY_RETRY_NONE, "POLICY_RETRY_NONE"},
	{AS_POLICY_RETRY_ONCE, "POLICY_RETRY_ONCE"},
	{AS_POLICY_EXISTS_IGNORE, "POLICY_EXISTS_IGNORE"},
	{AS_POLICY_EXISTS_CREATE, "POLICY_EXISTS_CREATE"},
	{AS_POLICY_EXISTS_UPDATE, "POLICY_EXISTS_UPDATE"},
	{AS_POLICY_EXISTS_REPLACE, "POLICY_EXISTS_REPLACE"},
	{AS_POLICY_EXISTS_CREATE_OR_REPLACE, "POLICY_EXISTS_CREATE_OR_REPLACE"},
	{AS_UDF_TYPE_LUA, "UDF_TYPE_LUA"},
	{AS_POLICY_KEY_DIGEST, "POLICY_KEY_DIGEST"},
	{AS_POLICY_KEY_SEND, "POLICY_KEY_SEND"},
	{AS_POLICY_GEN_IGNORE, "POLICY_GEN_IGNORE"},
	{AS_POLICY_GEN_EQ, "POLICY_GEN_EQ"},
	{AS_POLICY_GEN_GT, "POLICY_GEN_GT"},
	{AS_SCAN_STATUS_COMPLETED, "SCAN_STATUS_COMPLETED"},
	{AS_SCAN_STATUS_ABORTED, "SCAN_STATUS_ABORTED"},
	{AS_SCAN_STATUS_UNDEF, "SCAN_STATUS_UNDEF"},
	{AS_SCAN_STATUS_INPROGRESS, "SCAN_STATUS_INPROGRESS"},
	{AS_JOB_STATUS_COMPLETED, "JOB_STATUS_COMPLETED"},
	{AS_JOB_STATUS_UNDEF, "JOB_STATUS_UNDEF"},
	{AS_JOB_STATUS_INPROGRESS, "JOB_STATUS_INPROGRESS"},
	{AS_POLICY_REPLICA_MASTER, "POLICY_REPLICA_MASTER"},
	{AS_POLICY_REPLICA_ANY, "POLICY_REPLICA_ANY"},
	{AS_POLICY_REPLICA_SEQUENCE, "POLICY_REPLICA_SEQUENCE"},
	{AS_POLICY_REPLICA_PREFER_RACK, "POLICY_REPLICA_PREFER_RACK"},
	{AS_POLICY_COMMIT_LEVEL_ALL, "POLICY_COMMIT_LEVEL_ALL"},
	{AS_POLICY_COMMIT_LEVEL_MASTER, "POLICY_COMMIT_LEVEL_MASTER"},
	{SERIALIZER_PYTHON, "SERIALIZER_PYTHON"},
	{SERIALIZER_USER, "SERIALIZER_USER"},
	{SERIALIZER_JSON, "SERIALIZER_JSON"},
	{SERIALIZER_NONE, "SERIALIZER_NONE"},
	{SEND_BOOL_AS_PY_BYTES, "PY_BYTES"},
	{SEND_BOOL_AS_INTEGER, "INTEGER"},
	{SEND_BOOL_AS_AS_BOOL, "AS_BOOL"},
	{AS_INDEX_STRING, "INDEX_STRING"},
	{AS_INDEX_NUMERIC, "INDEX_NUMERIC"},
	{AS_INDEX_GEO2DSPHERE, "INDEX_GEO2DSPHERE"},
	{AS_INDEX_TYPE_LIST, "INDEX_TYPE_LIST"},
	{AS_INDEX_TYPE_MAPKEYS, "INDEX_TYPE_MAPKEYS"},
	{AS_INDEX_TYPE_MAPVALUES, "INDEX_TYPE_MAPVALUES"},
	{AS_PRIVILEGE_USER_ADMIN, "PRIV_USER_ADMIN"},
	{AS_PRIVILEGE_SYS_ADMIN, "PRIV_SYS_ADMIN"},
	{AS_PRIVILEGE_DATA_ADMIN, "PRIV_DATA_ADMIN"},
	{AS_PRIVILEGE_READ, "PRIV_READ"},
	{AS_PRIVILEGE_WRITE, "PRIV_WRITE"},
	{AS_PRIVILEGE_READ_WRITE, "PRIV_READ_WRITE"},
	{AS_PRIVILEGE_READ_WRITE_UDF, "PRIV_READ_WRITE_UDF"},

	{OP_LIST_APPEND, "OP_LIST_APPEND"},
	{OP_LIST_APPEND_ITEMS, "OP_LIST_APPEND_ITEMS"},
	{OP_LIST_INSERT, "OP_LIST_INSERT"},
	{OP_LIST_INSERT_ITEMS, "OP_LIST_INSERT_ITEMS"},
	{OP_LIST_POP, "OP_LIST_POP"},
	{OP_LIST_POP_RANGE, "OP_LIST_POP_RANGE"},
	{OP_LIST_REMOVE, "OP_LIST_REMOVE"},
	{OP_LIST_REMOVE_RANGE, "OP_LIST_REMOVE_RANGE"},
	{OP_LIST_CLEAR, "OP_LIST_CLEAR"},
	{OP_LIST_SET, "OP_LIST_SET"},
	{OP_LIST_GET, "OP_LIST_GET"},
	{OP_LIST_GET_RANGE, "OP_LIST_GET_RANGE"},
	{OP_LIST_TRIM, "OP_LIST_TRIM"},
	{OP_LIST_SIZE, "OP_LIST_SIZE"},
	{OP_LIST_INCREMENT, "OP_LIST_INCREMENT"},

	{OP_MAP_SET_POLICY, "OP_MAP_SET_POLICY"},
	{OP_MAP_PUT, "OP_MAP_PUT"},
	{OP_MAP_PUT_ITEMS, "OP_MAP_PUT_ITEMS"},
	{OP_MAP_INCREMENT, "OP_MAP_INCREMENT"},
	{OP_MAP_DECREMENT, "OP_MAP_DECREMENT"},
	{OP_MAP_SIZE, "OP_MAP_SIZE"},
	{OP_MAP_CLEAR, "OP_MAP_CLEAR"},
	{OP_MAP_REMOVE_BY_KEY, "OP_MAP_REMOVE_BY_KEY"},
	{OP_MAP_REMOVE_BY_KEY_LIST, "OP_MAP_REMOVE_BY_KEY_LIST"},
	{OP_MAP_REMOVE_BY_KEY_RANGE, "OP_MAP_REMOVE_BY_KEY_RANGE"},
	{OP_MAP_REMOVE_BY_VALUE, "OP_MAP_REMOVE_BY_VALUE"},
	{OP_MAP_REMOVE_BY_VALUE_LIST, "OP_MAP_REMOVE_BY_VALUE_LIST"},
	{OP_MAP_REMOVE_BY_VALUE_RANGE, "OP_MAP_REMOVE_BY_VALUE_RANGE"},
	{OP_MAP_REMOVE_BY_INDEX, "OP_MAP_REMOVE_BY_INDEX"},
	{OP_MAP_REMOVE_BY_INDEX_RANGE, "OP_MAP_REMOVE_BY_INDEX_RANGE"},
	{OP_MAP_REMOVE_BY_RANK, "OP_MAP_REMOVE_BY_RANK"},
	{OP_MAP_REMOVE_BY_RANK_RANGE, "OP_MAP_REMOVE_BY_RANK_RANGE"},
	{OP_MAP_GET_BY_KEY, "OP_MAP_GET_BY_KEY"},
	{OP_MAP_GET_BY_KEY_RANGE, "OP_MAP_GET_BY_KEY_RANGE"},
	{OP_MAP_GET_BY_VALUE, "OP_MAP_GET_BY_VALUE"},
	{OP_MAP_GET_BY_VALUE_RANGE, "OP_MAP_GET_BY_VALUE_RANGE"},
	{OP_MAP_GET_BY_INDEX, "OP_MAP_GET_BY_INDEX"},
	{OP_MAP_GET_BY_INDEX_RANGE, "OP_MAP_GET_BY_INDEX_RANGE"},
	{OP_MAP_GET_BY_RANK, "OP_MAP_GET_BY_RANK"},
	{OP_MAP_GET_BY_RANK_RANGE, "OP_MAP_GET_BY_RANK_RANGE"},
	{OP_MAP_GET_BY_VALUE_LIST, "OP_MAP_GET_BY_VALUE_LIST"},
	{OP_MAP_GET_BY_KEY_LIST, "OP_MAP_GET_BY_KEY_LIST"},

	{AS_MAP_UNORDERED, "MAP_UNORDERED"},
	{AS_MAP_KEY_ORDERED, "MAP_KEY_ORDERED"},
	{AS_MAP_KEY_VALUE_ORDERED, "MAP_KEY_VALUE_ORDERED"},

	{AS_MAP_UPDATE, "MAP_UPDATE"},
	{AS_MAP_UPDATE_ONLY, "MAP_UPDATE_ONLY"},
	{AS_MAP_CREATE_ONLY, "MAP_CREATE_ONLY"},

	{AS_MAP_RETURN_NONE, "MAP_RETURN_NONE"},
	{AS_MAP_RETURN_INDEX, "MAP_RETURN_INDEX"},
	{AS_MAP_RETURN_REVERSE_INDEX, "MAP_RETURN_REVERSE_INDEX"},
	{AS_MAP_RETURN_RANK, "MAP_RETURN_RANK"},
	{AS_MAP_RETURN_REVERSE_RANK, "MAP_RETURN_REVERSE_RANK"},
	{AS_MAP_RETURN_COUNT, "MAP_RETURN_COUNT"},
	{AS_MAP_RETURN_KEY, "MAP_RETURN_KEY"},
	{AS_MAP_RETURN_VALUE, "MAP_RETURN_VALUE"},
	{AS_MAP_RETURN_KEY_VALUE, "MAP_RETURN_KEY_VALUE"},

	{AS_RECORD_DEFAULT_TTL, "TTL_NAMESPACE_DEFAULT"},
	{AS_RECORD_NO_EXPIRE_TTL, "TTL_NEVER_EXPIRE"},
	{AS_RECORD_NO_CHANGE_TTL, "TTL_DONT_UPDATE"},
	{AS_AUTH_INTERNAL, "AUTH_INTERNAL"},
	{AS_AUTH_EXTERNAL, "AUTH_EXTERNAL"},
	{AS_AUTH_EXTERNAL_INSECURE, "AUTH_EXTERNAL_INSECURE"},
	{AS_AUTH_PKI, "AUTH_PKI"},
	/* New CDT Operations, post 3.16.0.1 */
	{OP_LIST_GET_BY_INDEX, "OP_LIST_GET_BY_INDEX"},
	{OP_LIST_GET_BY_INDEX_RANGE, "OP_LIST_GET_BY_INDEX_RANGE"},
	{OP_LIST_GET_BY_RANK, "OP_LIST_GET_BY_RANK"},
	{OP_LIST_GET_BY_RANK_RANGE, "OP_LIST_GET_BY_RANK_RANGE"},
	{OP_LIST_GET_BY_VALUE, "OP_LIST_GET_BY_VALUE"},
	{OP_LIST_GET_BY_VALUE_LIST, "OP_LIST_GET_BY_VALUE_LIST"},
	{OP_LIST_GET_BY_VALUE_RANGE, "OP_LIST_GET_BY_VALUE_RANGE"},
	{OP_LIST_REMOVE_BY_INDEX, "OP_LIST_REMOVE_BY_INDEX"},
	{OP_LIST_REMOVE_BY_INDEX_RANGE, "OP_LIST_REMOVE_BY_INDEX_RANGE"},
	{OP_LIST_REMOVE_BY_RANK, "OP_LIST_REMOVE_BY_RANK"},
	{OP_LIST_REMOVE_BY_RANK_RANGE, "OP_LIST_REMOVE_BY_RANK_RANGE"},
	{OP_LIST_REMOVE_BY_VALUE, "OP_LIST_REMOVE_BY_VALUE"},
	{OP_LIST_REMOVE_BY_VALUE_LIST, "OP_LIST_REMOVE_BY_VALUE_LIST"},
	{OP_LIST_REMOVE_BY_VALUE_RANGE, "OP_LIST_REMOVE_BY_VALUE_RANGE"},
	{OP_LIST_SET_ORDER, "OP_LIST_SET_ORDER"},
	{OP_LIST_SORT, "OP_LIST_SORT"},
	{AS_LIST_RETURN_NONE, "LIST_RETURN_NONE"},
	{AS_LIST_RETURN_INDEX, "LIST_RETURN_INDEX"},
	{AS_LIST_RETURN_REVERSE_INDEX, "LIST_RETURN_REVERSE_INDEX"},
	{AS_LIST_RETURN_RANK, "LIST_RETURN_RANK"},
	{AS_LIST_RETURN_REVERSE_RANK, "LIST_RETURN_REVERSE_RANK"},
	{AS_LIST_RETURN_COUNT, "LIST_RETURN_COUNT"},
	{AS_LIST_RETURN_VALUE, "LIST_RETURN_VALUE"},
	{AS_LIST_SORT_DROP_DUPLICATES, "LIST_SORT_DROP_DUPLICATES"},
	{AS_LIST_SORT_DEFAULT, "LIST_SORT_DEFAULT"},
	{AS_LIST_WRITE_ADD_UNIQUE, "LIST_WRITE_ADD_UNIQUE"},
	{AS_LIST_WRITE_INSERT_BOUNDED, "LIST_WRITE_INSERT_BOUNDED"},
	{AS_LIST_ORDERED, "LIST_ORDERED"},
	{AS_LIST_UNORDERED, "LIST_UNORDERED"},
	{OP_LIST_REMOVE_BY_VALUE_RANK_RANGE_REL,
	 "OP_LIST_REMOVE_BY_VALUE_RANK_RANGE_REL"},
	{OP_LIST_GET_BY_VALUE_RANK_RANGE_REL,
	 "OP_LIST_GET_BY_VALUE_RANK_RANGE_REL"},

	/* CDT operations for use with expressions, new in 5.0 */
	{OP_MAP_REMOVE_BY_VALUE_RANK_RANGE_REL,
	 "OP_MAP_REMOVE_BY_VALUE_RANK_RANGE_REL"},
	{OP_MAP_REMOVE_BY_KEY_INDEX_RANGE_REL,
	 "OP_MAP_REMOVE_BY_KEY_INDEX_RANGE_REL"},
	{OP_MAP_GET_BY_VALUE_RANK_RANGE_REL, "OP_MAP_GET_BY_VALUE_RANK_RANGE_REL"},
	{OP_MAP_GET_BY_KEY_INDEX_RANGE_REL, "OP_MAP_GET_BY_KEY_INDEX_RANGE_REL"},

	{OP_LIST_GET_BY_VALUE_RANK_RANGE_REL_TO_END,
	 "OP_LIST_GET_BY_VALUE_RANK_RANGE_REL_TO_END"},
	{OP_LIST_GET_BY_INDEX_RANGE_TO_END, "OP_LIST_GET_BY_INDEX_RANGE_TO_END"},
	{OP_LIST_GET_BY_RANK_RANGE_TO_END, "OP_LIST_GET_BY_RANK_RANGE_TO_END"},
	{OP_LIST_REMOVE_BY_REL_RANK_RANGE_TO_END,
	 "OP_LIST_REMOVE_BY_REL_RANK_RANGE_TO_END"},
	{OP_LIST_REMOVE_BY_REL_RANK_RANGE, "OP_LIST_REMOVE_BY_REL_RANK_RANGE"},
	{OP_LIST_REMOVE_BY_INDEX_RANGE_TO_END,
	 "OP_LIST_REMOVE_BY_INDEX_RANGE_TO_END"},
	{OP_LIST_REMOVE_BY_RANK_RANGE_TO_END,
	 "OP_LIST_REMOVE_BY_RANK_RANGE_TO_END"},

	{AS_MAP_WRITE_NO_FAIL, "MAP_WRITE_NO_FAIL"},
	{AS_MAP_WRITE_PARTIAL, "MAP_WRITE_PARTIAL"},

	{AS_LIST_WRITE_NO_FAIL, "LIST_WRITE_NO_FAIL"},
	{AS_LIST_WRITE_PARTIAL, "LIST_WRITE_PARTIAL"},

	/* Map write flags post 3.5.0 */
	{AS_MAP_WRITE_DEFAULT, "MAP_WRITE_FLAGS_DEFAULT"},
	{AS_MAP_WRITE_CREATE_ONLY, "MAP_WRITE_FLAGS_CREATE_ONLY"},
	{AS_MAP_WRITE_UPDATE_ONLY, "MAP_WRITE_FLAGS_UPDATE_ONLY"},
	{AS_MAP_WRITE_NO_FAIL, "MAP_WRITE_FLAGS_NO_FAIL"},
	{AS_MAP_WRITE_PARTIAL, "MAP_WRITE_FLAGS_PARTIAL"},

	/* READ Mode constants 4.0.0 */

	// AP Read Mode
	{AS_POLICY_READ_MODE_AP_ONE, "POLICY_READ_MODE_AP_ONE"},
	{AS_POLICY_READ_MODE_AP_ALL, "POLICY_READ_MODE_AP_ALL"},
	// SC Read Mode
	{AS_POLICY_READ_MODE_SC_SESSION, "POLICY_READ_MODE_SC_SESSION"},
	{AS_POLICY_READ_MODE_SC_LINEARIZE, "POLICY_READ_MODE_SC_LINEARIZE"},
	{AS_POLICY_READ_MODE_SC_ALLOW_REPLICA, "POLICY_READ_MODE_SC_ALLOW_REPLICA"},
	{AS_POLICY_READ_MODE_SC_ALLOW_UNAVAILABLE,
	 "POLICY_READ_MODE_SC_ALLOW_UNAVAILABLE"},

	/* Bitwise constants: 3.9.0 */
	{AS_BIT_WRITE_DEFAULT, "BIT_WRITE_DEFAULT"},
	{AS_BIT_WRITE_CREATE_ONLY, "BIT_WRITE_CREATE_ONLY"},
	{AS_BIT_WRITE_UPDATE_ONLY, "BIT_WRITE_UPDATE_ONLY"},
	{AS_BIT_WRITE_NO_FAIL, "BIT_WRITE_NO_FAIL"},
	{AS_BIT_WRITE_PARTIAL, "BIT_WRITE_PARTIAL"},

	{AS_BIT_RESIZE_DEFAULT, "BIT_RESIZE_DEFAULT"},
	{AS_BIT_RESIZE_FROM_FRONT, "BIT_RESIZE_FROM_FRONT"},
	{AS_BIT_RESIZE_GROW_ONLY, "BIT_RESIZE_GROW_ONLY"},
	{AS_BIT_RESIZE_SHRINK_ONLY, "BIT_RESIZE_SHRINK_ONLY"},

	{AS_BIT_OVERFLOW_FAIL, "BIT_OVERFLOW_FAIL"},
	{AS_BIT_OVERFLOW_SATURATE, "BIT_OVERFLOW_SATURATE"},
	{AS_BIT_OVERFLOW_WRAP, "BIT_OVERFLOW_WRAP"},

	/* BITWISE OPS: 3.9.0 */
	{OP_BIT_INSERT, "OP_BIT_INSERT"},
	{OP_BIT_RESIZE, "OP_BIT_RESIZE"},
	{OP_BIT_REMOVE, "OP_BIT_REMOVE"},
	{OP_BIT_SET, "OP_BIT_SET"},
	{OP_BIT_OR, "OP_BIT_OR"},
	{OP_BIT_XOR, "OP_BIT_XOR"},
	{OP_BIT_AND, "OP_BIT_AND"},
	{OP_BIT_NOT, "OP_BIT_NOT"},
	{OP_BIT_LSHIFT, "OP_BIT_LSHIFT"},
	{OP_BIT_RSHIFT, "OP_BIT_RSHIFT"},
	{OP_BIT_ADD, "OP_BIT_ADD"},
	{OP_BIT_SUBTRACT, "OP_BIT_SUBTRACT"},
	{OP_BIT_GET_INT, "OP_BIT_GET_INT"},
	{OP_BIT_SET_INT, "OP_BIT_SET_INT"},
	{OP_BIT_GET, "OP_BIT_GET"},
	{OP_BIT_COUNT, "OP_BIT_COUNT"},
	{OP_BIT_LSCAN, "OP_BIT_LSCAN"},
	{OP_BIT_RSCAN, "OP_BIT_RSCAN"},

	/* Nested CDT constants: 3.9.0 */
	{AS_CDT_CTX_LIST_INDEX, "CDT_CTX_LIST_INDEX"},
	{AS_CDT_CTX_LIST_RANK, "CDT_CTX_LIST_RANK"},
	{AS_CDT_CTX_LIST_VALUE, "CDT_CTX_LIST_VALUE"},
	{CDT_CTX_LIST_INDEX_CREATE, "CDT_CTX_LIST_INDEX_CREATE"},
	{AS_CDT_CTX_MAP_INDEX, "CDT_CTX_MAP_INDEX"},
	{AS_CDT_CTX_MAP_RANK, "CDT_CTX_MAP_RANK"},
	{AS_CDT_CTX_MAP_KEY, "CDT_CTX_MAP_KEY"},
	{AS_CDT_CTX_MAP_VALUE, "CDT_CTX_MAP_VALUE"},
	{CDT_CTX_MAP_KEY_CREATE, "CDT_CTX_MAP_KEY_CREATE"},

	/* HLL constants 3.11.0 */
	{OP_HLL_ADD, "OP_HLL_ADD"},
	{OP_HLL_DESCRIBE, "OP_HLL_DESCRIBE"},
	{OP_HLL_FOLD, "OP_HLL_FOLD"},
	{OP_HLL_GET_COUNT, "OP_HLL_GET_COUNT"},
	{OP_HLL_GET_INTERSECT_COUNT, "OP_HLL_GET_INTERSECT_COUNT"},
	{OP_HLL_GET_SIMILARITY, "OP_HLL_GET_SIMILARITY"},
	{OP_HLL_GET_UNION, "OP_HLL_GET_UNION"},
	{OP_HLL_GET_UNION_COUNT, "OP_HLL_GET_UNION_COUNT"},
	{OP_HLL_GET_SIMILARITY, "OP_HLL_GET_SIMILARITY"},
	{OP_HLL_INIT, "OP_HLL_INIT"},
	{OP_HLL_REFRESH_COUNT, "OP_HLL_REFRESH_COUNT"},
	{OP_HLL_SET_UNION, "OP_HLL_SET_UNION"},
	{OP_HLL_MAY_CONTAIN, "OP_HLL_MAY_CONTAIN"}, // for expression filters

	{AS_HLL_WRITE_DEFAULT, "HLL_WRITE_DEFAULT"},
	{AS_HLL_WRITE_CREATE_ONLY, "HLL_WRITE_CREATE_ONLY"},
	{AS_HLL_WRITE_UPDATE_ONLY, "HLL_WRITE_UPDATE_ONLY"},
	{AS_HLL_WRITE_NO_FAIL, "HLL_WRITE_NO_FAIL"},
	{AS_HLL_WRITE_ALLOW_FOLD, "HLL_WRITE_ALLOW_FOLD"},

	{OP_MAP_REMOVE_BY_KEY_REL_INDEX_RANGE_TO_END,
	 "OP_MAP_REMOVE_BY_KEY_REL_INDEX_RANGE_TO_END"},
	{OP_MAP_REMOVE_BY_VALUE_REL_RANK_RANGE_TO_END,
	 "OP_MAP_REMOVE_BY_VALUE_REL_RANK_RANGE_TO_END"},
	{OP_MAP_REMOVE_BY_INDEX_RANGE_TO_END,
	 "OP_MAP_REMOVE_BY_INDEX_RANGE_TO_END"},
	{OP_MAP_REMOVE_BY_RANK_RANGE_TO_END, "OP_MAP_REMOVE_BY_RANK_RANGE_TO_END"},
	{OP_MAP_GET_BY_KEY_REL_INDEX_RANGE_TO_END,
	 "OP_MAP_GET_BY_KEY_REL_INDEX_RANGE_TO_END"},
	{OP_MAP_REMOVE_BY_KEY_REL_INDEX_RANGE,
	 "OP_MAP_REMOVE_BY_KEY_REL_INDEX_RANGE"},
	{OP_MAP_REMOVE_BY_VALUE_REL_INDEX_RANGE,
	 "OP_MAP_REMOVE_BY_VALUE_REL_INDEX_RANGE"},
	{OP_MAP_REMOVE_BY_VALUE_REL_RANK_RANGE,
	 "OP_MAP_REMOVE_BY_VALUE_REL_RANK_RANGE"},
	{OP_MAP_GET_BY_KEY_REL_INDEX_RANGE, "OP_MAP_GET_BY_KEY_REL_INDEX_RANGE"},
	{OP_MAP_GET_BY_VALUE_RANK_RANGE_REL_TO_END,
	 "OP_MAP_GET_BY_VALUE_RANK_RANGE_REL_TO_END"},
	{OP_MAP_GET_BY_INDEX_RANGE_TO_END, "OP_MAP_GET_BY_INDEX_RANGE_TO_END"},
	{OP_MAP_GET_BY_RANK_RANGE_TO_END, "OP_MAP_GET_BY_RANK_RANGE_TO_END"},

	/* Expression operation constants 5.1.0 */
	{OP_EXPR_READ, "OP_EXPR_READ"},
	{OP_EXPR_WRITE, "OP_EXPR_WRITE"},
	{AS_EXP_WRITE_DEFAULT, "EXP_WRITE_DEFAULT"},
	{AS_EXP_WRITE_CREATE_ONLY, "EXP_WRITE_CREATE_ONLY"},
	{AS_EXP_WRITE_UPDATE_ONLY, "EXP_WRITE_UPDATE_ONLY"},
	{AS_EXP_WRITE_ALLOW_DELETE, "EXP_WRITE_ALLOW_DELETE"},
	{AS_EXP_WRITE_POLICY_NO_FAIL, "EXP_WRITE_POLICY_NO_FAIL"},
	{AS_EXP_WRITE_EVAL_NO_FAIL, "EXP_WRITE_EVAL_NO_FAIL"},
	{AS_EXP_READ_DEFAULT, "EXP_READ_DEFAULT"},
	{AS_EXP_READ_EVAL_NO_FAIL, "EXP_READ_EVAL_NO_FAIL"},

	/* For BinType expression, as_bytes_type */
	{AS_BYTES_UNDEF, "AS_BYTES_UNDEF"},
	{AS_BYTES_INTEGER, "AS_BYTES_INTEGER"},
	{AS_BYTES_DOUBLE, "AS_BYTES_DOUBLE"},
	{AS_BYTES_STRING, "AS_BYTES_STRING"},
	{AS_BYTES_BLOB, "AS_BYTES_BLOB"},
	{AS_BYTES_JAVA, "AS_BYTES_JAVA"},
	{AS_BYTES_CSHARP, "AS_BYTES_CSHARP"},
	{AS_BYTES_PYTHON, "AS_BYTES_PYTHON"},
	{AS_BYTES_RUBY, "AS_BYTES_RUBY"},
	{AS_BYTES_PHP, "AS_BYTES_PHP"},
	{AS_BYTES_ERLANG, "AS_BYTES_ERLANG"},
	{AS_BYTES_HLL, "AS_BYTES_HLL"},
	{AS_BYTES_MAP, "AS_BYTES_MAP"},
	{AS_BYTES_LIST, "AS_BYTES_LIST"},
	{AS_BYTES_GEOJSON, "AS_BYTES_GEOJSON"},
	{AS_BYTES_TYPE_MAX, "AS_BYTES_TYPE_MAX"}};

static AerospikeJobConstants aerospike_job_constants[] = {
	{"scan", "JOB_SCAN"}, {"query", "JOB_QUERY"}};
/**
 * Function for setting scan parameters in scan.
 * Like Scan Priority, Percentage, Concurrent, Nobins
 *
 * @param err                   The as_error to be populated by the function
 *                              with the encountered error if any.
 * @scan_p                      Scan parameter.
 * @py_options                  The user's optional scan options.
 */
void set_scan_options(as_error *err, as_scan *scan_p, PyObject *py_options)
{
	if (!scan_p) {
		as_error_update(err, AEROSPIKE_ERR_CLIENT, "Scan is not initialized");
		return;
	}

	if (PyDict_Check(py_options)) {
		PyObject *key = NULL, *value = NULL;
		Py_ssize_t pos = 0;
		int64_t val = 0;
		while (PyDict_Next(py_options, &pos, &key, &value)) {
			char *key_name = PyString_AsString(key);
			if (!PyString_Check(key)) {
				as_error_update(err, AEROSPIKE_ERR_PARAM,
								"Policy key must be string");
				break;
			}

			if (strcmp("concurrent", key_name) == 0) {
				if (!PyBool_Check(value)) {
					as_error_update(err, AEROSPIKE_ERR_PARAM,
									"Invalid value(type) for concurrent");
					break;
				}
				val = (int64_t)PyObject_IsTrue(value);
				if (val == -1 || (!as_scan_set_concurrent(scan_p, val))) {
					as_error_update(err, AEROSPIKE_ERR_PARAM,
									"Unable to set scan concurrent");
					break;
				}
			}
			else if (strcmp("nobins", key_name) == 0) {
				if (!PyBool_Check(value)) {
					as_error_update(err, AEROSPIKE_ERR_PARAM,
									"Invalid value(type) for nobins");
					break;
				}
				val = (int64_t)PyObject_IsTrue(value);
				if (val == -1 || (!as_scan_set_nobins(scan_p, val))) {
					as_error_update(err, AEROSPIKE_ERR_PARAM,
									"Unable to set scan nobins");
					break;
				}
			}
			else {
				as_error_update(err, AEROSPIKE_ERR_PARAM,
								"Invalid value for scan options");
				break;
			}
		}
	}
	else {
		as_error_update(err, AEROSPIKE_ERR_PARAM, "Invalid option(type)");
	}
}

as_status set_query_options(as_error *err, PyObject *query_options,
							as_query *query)
{
	PyObject *no_bins_val = NULL;
	if (!query_options || query_options == Py_None) {
		return AEROSPIKE_OK;
	}

	if (!PyDict_Check(query_options)) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM,
							   "query options must be a dictionary");
	}

	no_bins_val = PyDict_GetItemString(query_options, "nobins");
	if (no_bins_val) {
		if (!PyBool_Check(no_bins_val)) {
			return as_error_update(err, AEROSPIKE_ERR_PARAM,
								   "nobins value must be a bool");
		}
		query->no_bins = PyObject_IsTrue(no_bins_val);
	}
	return AEROSPIKE_OK;
}
/**
 * Declares policy constants.
 */
as_status declare_policy_constants(PyObject *aerospike)
{
	as_status status = AEROSPIKE_OK;
	int i;

	if (!aerospike) {
		status = AEROSPIKE_ERR;
		goto exit;
	}
	for (i = 0; i < (int)AEROSPIKE_CONSTANTS_ARR_SIZE; i++) {
		PyModule_AddIntConstant(aerospike, aerospike_constants[i].constant_str,
								aerospike_constants[i].constantno);
	}

	for (i = 0; i < (int)AEROSPIKE_JOB_CONSTANTS_ARR_SIZE; i++) {
		PyModule_AddStringConstant(aerospike,
								   aerospike_job_constants[i].exposed_job_str,
								   aerospike_job_constants[i].job_str);
	}
exit:
	return status;
}

/**
 * Converts a PyObject into an as_policy_admin object.
 * Returns AEROSPIKE_OK on success. On error, the err argument is populated.
 * We assume that the error object and the policy object are already allocated
 * and initialized (although, we do reset the error object here).
 */
as_status pyobject_to_policy_admin(AerospikeClient *self, as_error *err,
								   PyObject *py_policy, // remove self
								   as_policy_admin *policy,
								   as_policy_admin **policy_p,
								   as_policy_admin *config_admin_policy)
{

	// Initialize Policy
	POLICY_INIT(as_policy_admin);

	//Initialize policy with global defaults
	as_policy_admin_copy(config_admin_policy, policy);

	// Set policy fields
	POLICY_SET_FIELD(timeout, uint32_t);

	// Update the policy
	POLICY_UPDATE();

	return err->code;
}

/**
 * Converts a PyObject into an as_policy_apply object.
 * Returns AEROSPIKE_OK on success. On error, the err argument is populated.
 * We assume that the error object and the policy object are already allocated
 * and initialized (although, we do reset the error object here).
 */
as_status pyobject_to_policy_apply(AerospikeClient *self, as_error *err,
								   PyObject *py_policy, as_policy_apply *policy,
								   as_policy_apply **policy_p,
								   as_policy_apply *config_apply_policy,
								   as_predexp_list *predexp_list,
								   as_predexp_list **predexp_list_p,
								   as_exp *exp_list, as_exp **exp_list_p)
{
	// Initialize Policy
	POLICY_INIT(as_policy_apply);

	//Initialize policy with global defaults
	as_policy_apply_copy(config_apply_policy, policy);

	// Set policy fields
	POLICY_SET_TOTAL_TIMEOUT_FROM_TIMEOUT();

	POLICY_SET_BASE_FIELD(total_timeout, uint32_t);
	POLICY_SET_BASE_FIELD(socket_timeout, uint32_t);
	POLICY_SET_BASE_FIELD(max_retries, uint32_t);
	POLICY_SET_BASE_FIELD(sleep_between_retries, uint32_t);
	POLICY_SET_BASE_FIELD(compress, bool);

	POLICY_SET_FIELD(key, as_policy_key);
	POLICY_SET_FIELD(replica, as_policy_replica);
	//POLICY_SET_FIELD(gen, as_policy_gen); removed
	POLICY_SET_FIELD(commit_level, as_policy_commit_level);
	POLICY_SET_FIELD(durable_delete, bool);

	// C client 4.6.7 new policy
	POLICY_SET_PREDEXP_BASE_FIELD();

	// C client 5.0 new expressions
	POLICY_SET_EXPRESSIONS_BASE_FIELD();

	// Update the policy
	POLICY_UPDATE();

	return err->code;
}

/**
 * Converts a PyObject into an as_policy_info object.
 * Returns AEROSPIKE_OK on success. On error, the err argument is populated.
 * We assume that the error object and the policy object are already allocated
 * and initialized (although, we do reset the error object here).
 */
as_status pyobject_to_policy_info(as_error *err, PyObject *py_policy,
								  as_policy_info *policy,
								  as_policy_info **policy_p,
								  as_policy_info *config_info_policy)
{
	// Initialize Policy
	POLICY_INIT(as_policy_info);

	//Initialize policy with global defaults
	as_policy_info_copy(config_info_policy, policy);

	// Set policy fields
	POLICY_SET_FIELD(timeout, uint32_t);
	POLICY_SET_FIELD(send_as_is, bool);
	POLICY_SET_FIELD(check_bounds, bool);

	// Update the policy
	POLICY_UPDATE();

	return err->code;
}

/**
 * Converts a PyObject into an as_policy_query object.
 * Returns AEROSPIKE_OK on success. On error, the err argument is populated.
 * We assume that the error object and the policy object are already allocated
 * and initialized (although, we do reset the error object here).
 * exp_list, predexp_list are initialized by this function, caller must free.
 */
as_status pyobject_to_policy_query(AerospikeClient *self, as_error *err,
								   PyObject *py_policy, as_policy_query *policy,
								   as_policy_query **policy_p,
								   as_policy_query *config_query_policy,
								   as_predexp_list *predexp_list,
								   as_predexp_list **predexp_list_p,
								   as_exp *exp_list, as_exp **exp_list_p)
{
	// Initialize Policy
	POLICY_INIT(as_policy_query);

	//Initialize policy with global defaults
	as_policy_query_copy(config_query_policy, policy);

	// Set policy fields
	POLICY_SET_TOTAL_TIMEOUT_FROM_TIMEOUT();
	POLICY_SET_BASE_FIELD(total_timeout, uint32_t);
	POLICY_SET_BASE_FIELD(socket_timeout, uint32_t);
	POLICY_SET_BASE_FIELD(max_retries, uint32_t);
	POLICY_SET_BASE_FIELD(sleep_between_retries, uint32_t);
	POLICY_SET_BASE_FIELD(compress, bool);

	POLICY_SET_FIELD(deserialize, bool);

	// C client 4.6.7 new policy
	POLICY_SET_PREDEXP_BASE_FIELD();

	// C client 5.0 new expressions
	POLICY_SET_EXPRESSIONS_BASE_FIELD();

	// Update the policy
	POLICY_UPDATE();

	return err->code;
}

/**
 * Converts a PyObject into an as_policy_read object.
 * Returns AEROSPIKE_OK on success. On error, the err argument is populated.
 * We assume that the error object and the policy object are already allocated
 * and initialized (although, we do reset the error object here).
 */
as_status pyobject_to_policy_read(AerospikeClient *self, as_error *err,
								  PyObject *py_policy, as_policy_read *policy,
								  as_policy_read **policy_p,
								  as_policy_read *config_read_policy,
								  as_predexp_list *predexp_list,
								  as_predexp_list **predexp_list_p,
								  as_exp *exp_list, as_exp **exp_list_p)
{
	// Initialize Policy
	POLICY_INIT(as_policy_read);

	//Initialize policy with global defaults
	as_policy_read_copy(config_read_policy, policy);

	// Set policy fields
	POLICY_SET_TOTAL_TIMEOUT_FROM_TIMEOUT();

	POLICY_SET_BASE_FIELD(total_timeout, uint32_t);
	POLICY_SET_BASE_FIELD(socket_timeout, uint32_t);
	POLICY_SET_BASE_FIELD(max_retries, uint32_t);
	POLICY_SET_BASE_FIELD(sleep_between_retries, uint32_t);
	POLICY_SET_BASE_FIELD(compress, bool);

	POLICY_SET_FIELD(key, as_policy_key);
	POLICY_SET_FIELD(replica, as_policy_replica);
	POLICY_SET_FIELD(deserialize, bool);

	// 4.0.0 new policies
	POLICY_SET_FIELD(read_mode_ap, as_policy_read_mode_ap);
	POLICY_SET_FIELD(read_mode_sc, as_policy_read_mode_sc);

	// C client 4.6.7 new policy
	POLICY_SET_PREDEXP_BASE_FIELD();

	// C client 5.0 new expressions
	POLICY_SET_EXPRESSIONS_BASE_FIELD();

	// Update the policy
	POLICY_UPDATE();

	return err->code;
}

/**
 * Converts a PyObject into an as_policy_remove object.
 * Returns AEROSPIKE_OK on success. On error, the err argument is populated.
 * We assume that the error object and the policy object are already allocated
 * and initialized (although, we do reset the error object here).
 */
as_status pyobject_to_policy_remove(
	AerospikeClient *self, as_error *err, PyObject *py_policy,
	as_policy_remove *policy, as_policy_remove **policy_p,
	as_policy_remove *config_remove_policy, as_predexp_list *predexp_list,
	as_predexp_list **predexp_list_p, as_exp *exp_list, as_exp **exp_list_p)
{
	// Initialize Policy
	POLICY_INIT(as_policy_remove);

	//Initialize policy with global defaults
	as_policy_remove_copy(config_remove_policy, policy);

	// Set policy fields
	POLICY_SET_TOTAL_TIMEOUT_FROM_TIMEOUT();

	POLICY_SET_BASE_FIELD(total_timeout, uint32_t);
	POLICY_SET_BASE_FIELD(socket_timeout, uint32_t);
	POLICY_SET_BASE_FIELD(max_retries, uint32_t);
	POLICY_SET_BASE_FIELD(sleep_between_retries, uint32_t);
	POLICY_SET_BASE_FIELD(compress, bool);

	POLICY_SET_FIELD(generation, uint16_t);

	POLICY_SET_FIELD(key, as_policy_key);
	POLICY_SET_FIELD(gen, as_policy_gen);
	POLICY_SET_FIELD(commit_level, as_policy_commit_level);
	POLICY_SET_FIELD(replica, as_policy_replica);
	POLICY_SET_FIELD(durable_delete, bool);

	// C client 4.6.7 new policy
	POLICY_SET_PREDEXP_BASE_FIELD();

	// C client 5.0 new expressions
	POLICY_SET_EXPRESSIONS_BASE_FIELD();

	// Update the policy
	POLICY_UPDATE();

	return err->code;
}

/**
 * Converts a PyObject into an as_policy_scan object.
 * Returns AEROSPIKE_OK on success. On error, the err argument is populated.
 * We assume that the error object and the policy object are already allocated
 * and initialized (although, we do reset the error object here).
 */
as_status pyobject_to_policy_scan(AerospikeClient *self, as_error *err,
								  PyObject *py_policy, as_policy_scan *policy,
								  as_policy_scan **policy_p,
								  as_policy_scan *config_scan_policy,
								  as_predexp_list *predexp_list,
								  as_predexp_list **predexp_list_p,
								  as_exp *exp_list, as_exp **exp_list_p)
{
	// Initialize Policy
	POLICY_INIT(as_policy_scan);

	//Initialize policy with global defaults
	as_policy_scan_copy(config_scan_policy, policy);

	// Set policy fields
	// server side socket_timeout
	POLICY_SET_TOTAL_TIMEOUT_FROM_TIMEOUT();

	POLICY_SET_BASE_FIELD(total_timeout, uint32_t);
	POLICY_SET_BASE_FIELD(socket_timeout, uint32_t);
	POLICY_SET_BASE_FIELD(max_retries, uint32_t);
	POLICY_SET_BASE_FIELD(sleep_between_retries, uint32_t);
	POLICY_SET_BASE_FIELD(compress, bool);

	POLICY_SET_FIELD(durable_delete, bool);
	POLICY_SET_FIELD(records_per_second, uint32_t);
	POLICY_SET_FIELD(max_records, uint64_t);

	// C client 4.6.7 new policy
	POLICY_SET_PREDEXP_BASE_FIELD();

	// C client 5.0 new expressions
	POLICY_SET_EXPRESSIONS_BASE_FIELD();

	// Update the policy
	POLICY_UPDATE();

	return err->code;
}

/**
 * Converts a PyObject into an as_policy_write object.
 * Returns AEROSPIKE_OK on success. On error, the err argument is populated.
 * We assume that the error object and the policy object are already allocated
 * and initialized (although, we do reset the error object here).
 */
as_status pyobject_to_policy_write(AerospikeClient *self, as_error *err,
								   PyObject *py_policy, as_policy_write *policy,
								   as_policy_write **policy_p,
								   as_policy_write *config_write_policy,
								   as_predexp_list *predexp_list,
								   as_predexp_list **predexp_list_p,
								   as_exp *exp_list, as_exp **exp_list_p)
{
	// Initialize Policy
	POLICY_INIT(as_policy_write);

	//Initialize policy with global defaults
	as_policy_write_copy(config_write_policy, policy);

	// Set policy fields
	POLICY_SET_TOTAL_TIMEOUT_FROM_TIMEOUT();

	// Base policy_fields
	POLICY_SET_BASE_FIELD(total_timeout, uint32_t);
	POLICY_SET_BASE_FIELD(socket_timeout, uint32_t);
	POLICY_SET_BASE_FIELD(max_retries, uint32_t);
	POLICY_SET_BASE_FIELD(sleep_between_retries, uint32_t);
	POLICY_SET_BASE_FIELD(compress, bool);

	POLICY_SET_FIELD(key, as_policy_key);
	POLICY_SET_FIELD(gen, as_policy_gen);
	POLICY_SET_FIELD(exists, as_policy_exists);
	POLICY_SET_FIELD(commit_level, as_policy_commit_level);
	POLICY_SET_FIELD(durable_delete, bool);
	POLICY_SET_FIELD(replica, as_policy_replica);
	POLICY_SET_FIELD(compression_threshold, uint32_t);

	// C client 4.6.7 new policy
	POLICY_SET_PREDEXP_BASE_FIELD();

	// C client 5.0 new expressions
	POLICY_SET_EXPRESSIONS_BASE_FIELD();

	// Update the policy
	POLICY_UPDATE();

	return err->code;
}

/**
 * Converts a PyObject into an as_policy_operate object.
 * Returns AEROSPIKE_OK on success. On error, the err argument is populated.
 * We assume that the error object and the policy object are already allocated
 * and initialized (although, we do reset the error object here).
 */
as_status pyobject_to_policy_operate(
	AerospikeClient *self, as_error *err, PyObject *py_policy,
	as_policy_operate *policy, as_policy_operate **policy_p,
	as_policy_operate *config_operate_policy, as_predexp_list *predexp_list,
	as_predexp_list **predexp_list_p, as_exp *exp_list, as_exp **exp_list_p)
{
	// Initialize Policy
	POLICY_INIT(as_policy_operate);

	//Initialize policy with global defaults
	as_policy_operate_copy(config_operate_policy, policy);

	// Set policy fields
	POLICY_SET_TOTAL_TIMEOUT_FROM_TIMEOUT();

	POLICY_SET_BASE_FIELD(total_timeout, uint32_t);
	POLICY_SET_BASE_FIELD(socket_timeout, uint32_t);
	POLICY_SET_BASE_FIELD(max_retries, uint32_t);
	POLICY_SET_BASE_FIELD(sleep_between_retries, uint32_t);
	POLICY_SET_BASE_FIELD(compress, bool);

	POLICY_SET_FIELD(key, as_policy_key);
	POLICY_SET_FIELD(gen, as_policy_gen);
	POLICY_SET_FIELD(commit_level, as_policy_commit_level);
	POLICY_SET_FIELD(replica, as_policy_replica);
	POLICY_SET_FIELD(durable_delete, bool);
	POLICY_SET_FIELD(deserialize, bool);
	POLICY_SET_FIELD(exists, as_policy_exists);

	// 4.0.0 new policies
	POLICY_SET_FIELD(read_mode_ap, as_policy_read_mode_ap);
	POLICY_SET_FIELD(read_mode_sc, as_policy_read_mode_sc);

	// C client 4.6.7 new policy
	POLICY_SET_PREDEXP_BASE_FIELD();

	// C client 5.0 new expressions
	POLICY_SET_EXPRESSIONS_BASE_FIELD();

	// Update the policy
	POLICY_UPDATE();

	return err->code;
}

/**
 * Converts a PyObject into an as_policy_batch object.
 * Returns AEROSPIKE_OK on success. On error, the err argument is populated.
 * We assume that the error object and the policy object are already allocated
 * and initialized (although, we do reset the error object here).
 */
as_status pyobject_to_policy_batch(AerospikeClient *self, as_error *err,
								   PyObject *py_policy, as_policy_batch *policy,
								   as_policy_batch **policy_p,
								   as_policy_batch *config_batch_policy,
								   as_predexp_list *predexp_list,
								   as_predexp_list **predexp_list_p,
								   as_exp *exp_list, as_exp **exp_list_p)
{
	// Initialize Policy
	POLICY_INIT(as_policy_batch);

	//Initialize policy with global defaults
	as_policy_batch_copy(config_batch_policy, policy);

	// Set policy fields
	POLICY_SET_TOTAL_TIMEOUT_FROM_TIMEOUT();

	POLICY_SET_BASE_FIELD(total_timeout, uint32_t);
	POLICY_SET_BASE_FIELD(socket_timeout, uint32_t);
	POLICY_SET_BASE_FIELD(max_retries, uint32_t);
	POLICY_SET_BASE_FIELD(sleep_between_retries, uint32_t);
	POLICY_SET_BASE_FIELD(compress, bool);

	POLICY_SET_FIELD(concurrent, bool);
	POLICY_SET_FIELD(allow_inline, bool);
	POLICY_SET_FIELD(send_set_name, bool);
	POLICY_SET_FIELD(deserialize, bool);
	POLICY_SET_FIELD(replica, as_policy_replica);

	// 4.0.0 new policies
	POLICY_SET_FIELD(read_mode_ap, as_policy_read_mode_ap);
	POLICY_SET_FIELD(read_mode_sc, as_policy_read_mode_sc);

	// C client 4.6.7 new policy
	POLICY_SET_PREDEXP_BASE_FIELD();

	// C client 5.0 new expressions
	POLICY_SET_EXPRESSIONS_BASE_FIELD();

	// Update the policy
	POLICY_UPDATE();

	return err->code;
}
as_status pyobject_to_bit_policy(as_error *err, PyObject *py_policy,
								 as_bit_policy *policy)
{
	as_bit_policy_init(policy);
	POLICY_INIT(as_bit_policy);

	PyObject *py_bit_flags =
		PyDict_GetItemString(py_policy, BIT_WRITE_FLAGS_KEY);
	if (py_bit_flags) {
		if (PyInt_Check(py_bit_flags)) {
			as_bit_write_flags bit_write_flags =
				(as_bit_write_flags)PyInt_AsLong(py_bit_flags);
			as_bit_policy_set_write_flags(policy, bit_write_flags);
		}
	}
	else if (PyErr_Occurred()) {
		/* Fetching a map key failed internally for some reason, raise an error and exit.*/
		PyErr_Clear();
		return as_error_update(err, AEROSPIKE_ERR_CLIENT,
							   "Unable to get bit_write_flags");
	}

	return err->code;
}
as_status pyobject_to_map_policy(as_error *err, PyObject *py_policy,
								 as_map_policy *policy)
{
	// Initialize Policy
	POLICY_INIT(as_map_policy);

	long map_order = AS_MAP_UNORDERED;
	long map_write_mode = AS_MAP_UPDATE;
	uint32_t map_write_flags = AS_MAP_WRITE_DEFAULT;

	MAP_POLICY_SET_FIELD(map_order);
	PyObject *mode_or_flags =
		PyDict_GetItemString(py_policy, MAP_WRITE_FLAGS_KEY);

	/*
	This only works for client >= 3.5.0 and server >= 4.3.0
	If py_policy["map_write_flags"] is set, we use it
	otherwise we use py_policy["map_write_mode"]
	*/
	if (mode_or_flags) {
		if (PyInt_Check(mode_or_flags)) {
			map_write_flags = (uint32_t)PyInt_AsLong(mode_or_flags);
			as_map_policy_set_flags(policy, map_order, map_write_flags);
		}
		else {
			as_error_update(err, AEROSPIKE_ERR_PARAM,
							"map write flags must be an integer");
		}
		return err->code;
	}

	MAP_POLICY_SET_FIELD(map_write_mode);
	as_map_policy_set(policy, map_order, map_write_mode);

	return err->code;
}

as_status pyobject_to_list_policy(as_error *err, PyObject *py_policy,
								  as_list_policy *list_policy)
{
	as_list_policy_init(list_policy);
	PyObject *py_val = NULL;
	long list_order = AS_LIST_UNORDERED;
	long flags = AS_LIST_WRITE_DEFAULT;

	if (!py_policy || py_policy == Py_None) {
		return AEROSPIKE_OK;
	}

	if (!PyDict_Check(py_policy)) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM,
							   "List policy must be a dictionary.");
	}

	py_val = PyDict_GetItemString(py_policy, "list_order");
	if (py_val && py_val != Py_None) {
		if (PyInt_Check(py_val)) {
			list_order = (int64_t)PyInt_AsLong(py_val);
			if (PyErr_Occurred()) {
				return as_error_update(err, AEROSPIKE_ERR_PARAM,
									   "Failed to convert list_order");
			}
		}
		else if (PyLong_Check(py_val)) {
			list_order = (int64_t)PyLong_AsLong(py_val);
			if (PyErr_Occurred()) {
				return as_error_update(err, AEROSPIKE_ERR_PARAM,
									   "Failed to convert list_order");
			}
		}
		else {
			return as_error_update(err, AEROSPIKE_ERR_PARAM,
								   "Invalid List order");
		}
	}

	py_val = PyDict_GetItemString(py_policy, "write_flags");
	if (py_val && py_val != Py_None) {
		if (PyInt_Check(py_val)) {
			flags = (int64_t)PyInt_AsLong(py_val);
			if (PyErr_Occurred()) {
				return as_error_update(err, AEROSPIKE_ERR_PARAM,
									   "Failed to convert write_flags");
			}
		}
		else if (PyLong_Check(py_val)) {
			flags = (int64_t)PyLong_AsLong(py_val);
			if (PyErr_Occurred()) {
				return as_error_update(err, AEROSPIKE_ERR_PARAM,
									   "Failed to convert write_flags");
			}
		}
		else {
			return as_error_update(err, AEROSPIKE_ERR_PARAM,
								   "Invalid write_flags");
		}
	}

	as_list_policy_set(list_policy, (as_list_order)list_order,
					   (as_list_write_flags)flags);

	return AEROSPIKE_OK;
}

as_status pyobject_to_hll_policy(as_error *err, PyObject *py_policy,
								 as_hll_policy *hll_policy)
{
	int64_t flags = 0;
	as_hll_policy_init(hll_policy);
	PyObject *py_val = NULL;

	if (!py_policy || py_policy == Py_None) {
		return AEROSPIKE_OK;
	}

	if (!PyDict_Check(py_policy)) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM,
							   "Hll policy must be a dictionary.");
	}

	py_val = PyDict_GetItemString(py_policy, "flags");
	if (py_val && py_val != Py_None) {
		if (PyInt_Check(py_val)) {
			flags = (int64_t)PyInt_AsLong(py_val);
			if (PyErr_Occurred()) {
				return as_error_update(err, AEROSPIKE_ERR_PARAM,
									   "Failed to convert flags.");
			}
		}
		else if (PyLong_Check(py_val)) {
			flags = (int64_t)PyLong_AsLong(py_val);
			if (PyErr_Occurred()) {
				return as_error_update(err, AEROSPIKE_ERR_PARAM,
									   "Failed to convert flags.");
			}
		}
		else {
			return as_error_update(err, AEROSPIKE_ERR_PARAM,
								   "Invalid hll policy flags.");
		}
	}

	as_hll_policy_set_write_flags(hll_policy, flags);

	return AEROSPIKE_OK;
}
