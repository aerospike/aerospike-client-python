/*******************************************************************************
 * Copyright 2013-2016 Aerospike, Inc.
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
#include <aerospike/as_policy.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>
#include <aerospike/as_admin.h>
#include <aerospike/as_operations.h>
#include <aerospike/as_map_operations.h>
#include <aerospike/aerospike_index.h>
#include "aerospike/as_scan.h"
#include "aerospike/as_job.h"

#include "policy.h"
#include "macros.h"

#define POLICY_INIT(__policy) \
	as_error_reset(err);\
if (!py_policy || py_policy == Py_None) {\
	return err->code;\
}\
if (!PyDict_Check(py_policy)) {\
	return as_error_update(err, AEROSPIKE_ERR_PARAM, "policy must be a dict");\
}\
__policy##_init(policy);\

#define POLICY_UPDATE() \
	*policy_p = policy;

#define POLICY_SET_FIELD(__field, __type) { \
	PyObject * py_field = PyDict_GetItemString(py_policy, #__field);\
	if (py_field) {\
		if (PyInt_Check(py_field)) {\
			policy->__field = (__type) PyInt_AsLong(py_field);\
		}\
		else {\
			return as_error_update(err, AEROSPIKE_ERR_PARAM, "%s is invalid", #__field);\
		}\
	}\
}

#define MAP_POLICY_SET_FIELD(__field) { \
	PyObject * py_field = PyDict_GetItemString(py_policy, #__field);\
	if (py_field) {\
		if (PyInt_Check(py_field)) {\
			__field = PyInt_AsLong(py_field);\
		}\
		else {\
			return as_error_update(err, AEROSPIKE_ERR_PARAM, "%s is invalid", #__field);\
		}\
	}\
}

/*
 *******************************************************************************************************
 * Mapping of constant number to constant name string.
 *******************************************************************************************************
 */
static
AerospikeConstants aerospike_constants[] = {
	{ AS_POLICY_RETRY_NONE                  ,   "POLICY_RETRY_NONE" },
	{ AS_POLICY_RETRY_ONCE                  ,   "POLICY_RETRY_ONCE" },
	{ AS_POLICY_EXISTS_IGNORE               ,   "POLICY_EXISTS_IGNORE" },
	{ AS_POLICY_EXISTS_CREATE               ,   "POLICY_EXISTS_CREATE" },
	{ AS_POLICY_EXISTS_UPDATE               ,   "POLICY_EXISTS_UPDATE" },
	{ AS_POLICY_EXISTS_REPLACE              ,   "POLICY_EXISTS_REPLACE" },
	{ AS_POLICY_EXISTS_CREATE_OR_REPLACE    ,   "POLICY_EXISTS_CREATE_OR_REPLACE" },
	{ AS_UDF_TYPE_LUA                       ,   "UDF_TYPE_LUA" },
	{ AS_POLICY_KEY_DIGEST                  ,   "POLICY_KEY_DIGEST" },
	{ AS_POLICY_KEY_SEND                    ,   "POLICY_KEY_SEND" },
	{ AS_POLICY_GEN_IGNORE                  ,   "POLICY_GEN_IGNORE" },
	{ AS_POLICY_GEN_EQ                      ,   "POLICY_GEN_EQ" },
	{ AS_POLICY_GEN_GT                      ,   "POLICY_GEN_GT" },
	{ AS_SCAN_PRIORITY_AUTO                 ,   "SCAN_PRIORITY_AUTO" },
	{ AS_SCAN_PRIORITY_LOW                  ,   "SCAN_PRIORITY_LOW" },
	{ AS_SCAN_PRIORITY_MEDIUM               ,   "SCAN_PRIORITY_MEDIUM" },
	{ AS_SCAN_PRIORITY_HIGH                 ,   "SCAN_PRIORITY_HIGH" },
	{ AS_SCAN_STATUS_COMPLETED              ,   "SCAN_STATUS_COMPLETED" },
	{ AS_SCAN_STATUS_ABORTED                ,   "SCAN_STATUS_ABORTED" },
	{ AS_SCAN_STATUS_UNDEF                  ,   "SCAN_STATUS_UNDEF" },
	{ AS_SCAN_STATUS_INPROGRESS             ,   "SCAN_STATUS_INPROGRESS" },
	{ AS_JOB_STATUS_COMPLETED               ,   "JOB_STATUS_COMPLETED" },
	{ AS_JOB_STATUS_UNDEF                   ,   "JOB_STATUS_UNDEF" },
	{ AS_JOB_STATUS_INPROGRESS              ,   "JOB_STATUS_INPROGRESS" },
	{ AS_POLICY_REPLICA_MASTER              ,   "POLICY_REPLICA_MASTER" },
	{ AS_POLICY_REPLICA_ANY                 ,   "POLICY_REPLICA_ANY" },
	{ AS_POLICY_REPLICA_SEQUENCE            ,   "POLICY_REPLICA_SEQUENCE" },
	{ AS_POLICY_CONSISTENCY_LEVEL_ONE       ,   "POLICY_CONSISTENCY_ONE" },
	{ AS_POLICY_CONSISTENCY_LEVEL_ALL       ,   "POLICY_CONSISTENCY_ALL" },
	{ AS_POLICY_COMMIT_LEVEL_ALL            ,   "POLICY_COMMIT_LEVEL_ALL" },
	{ AS_POLICY_COMMIT_LEVEL_MASTER         ,   "POLICY_COMMIT_LEVEL_MASTER" },
	{ SERIALIZER_PYTHON                     ,   "SERIALIZER_PYTHON" },
	{ SERIALIZER_USER                       ,   "SERIALIZER_USER" },
	{ SERIALIZER_JSON                       ,   "SERIALIZER_JSON" },
	{ SERIALIZER_NONE                       ,   "SERIALIZER_NONE" },
	{ AS_INDEX_STRING                       ,   "INDEX_STRING" },
	{ AS_INDEX_NUMERIC                      ,   "INDEX_NUMERIC" },
	{ AS_INDEX_GEO2DSPHERE                  ,   "INDEX_GEO2DSPHERE" },
	{ AS_INDEX_TYPE_LIST                    ,   "INDEX_TYPE_LIST" },
	{ AS_INDEX_TYPE_MAPKEYS                 ,   "INDEX_TYPE_MAPKEYS" },
	{ AS_INDEX_TYPE_MAPVALUES               ,   "INDEX_TYPE_MAPVALUES" },
	{ AS_PRIVILEGE_USER_ADMIN               ,   "PRIV_USER_ADMIN" },
	{ AS_PRIVILEGE_SYS_ADMIN                ,   "PRIV_SYS_ADMIN"	},
	{ AS_PRIVILEGE_DATA_ADMIN               ,   "PRIV_DATA_ADMIN"	},
	{ AS_PRIVILEGE_READ                     ,   "PRIV_READ"},
	{ AS_PRIVILEGE_READ_WRITE               ,   "PRIV_READ_WRITE"},
	{ AS_PRIVILEGE_READ_WRITE_UDF           ,   "PRIV_READ_WRITE_UDF"},

	{ OP_LIST_APPEND                        ,   "OP_LIST_APPEND"},
	{ OP_LIST_APPEND_ITEMS                  ,   "OP_LIST_APPEND_ITEMS"},
	{ OP_LIST_INSERT                        ,   "OP_LIST_INSERT"},
	{ OP_LIST_INSERT_ITEMS                  ,   "OP_LIST_INSERT_ITEMS"},
	{ OP_LIST_POP                           ,   "OP_LIST_POP"},
	{ OP_LIST_POP_RANGE                     ,   "OP_LIST_POP_RANGE"},
	{ OP_LIST_REMOVE                        ,   "OP_LIST_REMOVE"},
	{ OP_LIST_REMOVE_RANGE                  ,   "OP_LIST_REMOVE_RANGE"},
	{ OP_LIST_CLEAR                         ,   "OP_LIST_CLEAR"},
	{ OP_LIST_SET                           ,   "OP_LIST_SET"},
	{ OP_LIST_GET                           ,   "OP_LIST_GET"},
	{ OP_LIST_GET_RANGE                     ,   "OP_LIST_GET_RANGE"},
	{ OP_LIST_TRIM                          ,   "OP_LIST_TRIM"},
	{ OP_LIST_SIZE                          ,   "OP_LIST_SIZE"},

	{ OP_MAP_SET_POLICY                     ,   "OP_MAP_SET_POLICY"},
	{ OP_MAP_PUT                            ,   "OP_MAP_PUT"},
	{ OP_MAP_PUT_ITEMS                      ,   "OP_MAP_PUT_ITEMS"},
	{ OP_MAP_INCREMENT                      ,   "OP_MAP_INCREMENT"},
	{ OP_MAP_DECREMENT                      ,   "OP_MAP_DECREMENT"},
	{ OP_MAP_SIZE                           ,   "OP_MAP_SIZE"},
	{ OP_MAP_CLEAR                          ,   "OP_MAP_CLEAR"},
	{ OP_MAP_REMOVE_BY_KEY                  ,   "OP_MAP_REMOVE_BY_KEY"},
	{ OP_MAP_REMOVE_BY_KEY_LIST             ,   "OP_MAP_REMOVE_BY_KEY_LIST"},
	{ OP_MAP_REMOVE_BY_KEY_RANGE            ,   "OP_MAP_REMOVE_BY_KEY_RANGE"},
	{ OP_MAP_REMOVE_BY_VALUE                ,   "OP_MAP_REMOVE_BY_VALUE"},
	{ OP_MAP_REMOVE_BY_VALUE_LIST           ,   "OP_MAP_REMOVE_BY_VALUE_LIST"},
	{ OP_MAP_REMOVE_BY_VALUE_RANGE          ,   "OP_MAP_REMOVE_BY_VALUE_RANGE"},
	{ OP_MAP_REMOVE_BY_INDEX                ,   "OP_MAP_REMOVE_BY_INDEX"},
	{ OP_MAP_REMOVE_BY_INDEX_RANGE          ,   "OP_MAP_REMOVE_BY_INDEX_RANGE"},
	{ OP_MAP_REMOVE_BY_RANK                 ,   "OP_MAP_REMOVE_BY_RANK"},
	{ OP_MAP_REMOVE_BY_RANK_RANGE           ,   "OP_MAP_REMOVE_BY_RANK_RANGE"},
	{ OP_MAP_GET_BY_KEY                     ,   "OP_MAP_GET_BY_KEY"},
	{ OP_MAP_GET_BY_KEY_RANGE               ,   "OP_MAP_GET_BY_KEY_RANGE"},
	{ OP_MAP_GET_BY_VALUE                   ,   "OP_MAP_GET_BY_VALUE"},
	{ OP_MAP_GET_BY_VALUE_RANGE             ,   "OP_MAP_GET_BY_VALUE_RANGE"},
	{ OP_MAP_GET_BY_INDEX                   ,   "OP_MAP_GET_BY_INDEX"},
	{ OP_MAP_GET_BY_INDEX_RANGE             ,   "OP_MAP_GET_BY_INDEX_RANGE"},
	{ OP_MAP_GET_BY_RANK                    ,   "OP_MAP_GET_BY_RANK"},
	{ OP_MAP_GET_BY_RANK_RANGE              ,   "OP_MAP_GET_BY_RANK_RANGE"},

	{ AS_MAP_UNORDERED                      ,   "MAP_UNORDERED"},
	{ AS_MAP_KEY_ORDERED                    ,   "MAP_KEY_ORDERED"},
	{ AS_MAP_KEY_VALUE_ORDERED              ,   "MAP_KEY_VALUE_ORDERED"},

	{ AS_MAP_UPDATE                         ,   "MAP_UPDATE"},
	{ AS_MAP_UPDATE_ONLY                    ,   "MAP_UPDATE_ONLY"},
	{ AS_MAP_CREATE_ONLY                    ,   "MAP_CREATE_ONLY"},

	{ AS_MAP_RETURN_NONE                    ,   "MAP_RETURN_NONE"},
	{ AS_MAP_RETURN_INDEX                   ,   "MAP_RETURN_INDEX"},
	{ AS_MAP_RETURN_REVERSE_INDEX           ,   "MAP_RETURN_REVERSE_INDEX"},
	{ AS_MAP_RETURN_RANK                    ,   "MAP_RETURN_RANK"},
	{ AS_MAP_RETURN_REVERSE_RANK            ,   "MAP_RETURN_REVERSE_RANK"},
	{ AS_MAP_RETURN_COUNT                   ,   "MAP_RETURN_COUNT"},
	{ AS_MAP_RETURN_KEY                     ,   "MAP_RETURN_KEY"},
	{ AS_MAP_RETURN_VALUE                   ,   "MAP_RETURN_VALUE"},
	{ AS_MAP_RETURN_KEY_VALUE               ,   "MAP_RETURN_KEY_VALUE"},

	{ AS_RECORD_DEFAULT_TTL                 ,   "TTL_NAMESPACE_DEFAULT"},
	{ AS_RECORD_NO_EXPIRE_TTL               ,   "TTL_NEVER_EXPIRE"},
	{ -2                                    ,   "TTL_DONT_UPDATE"}
};

static
AerospikeJobConstants aerospike_job_constants[] = {
	{ "scan"        ,     "JOB_SCAN"},
	{ "query"       ,     "JOB_QUERY"}
};
/**
 * Function for setting scan parameters in scan.
 * Like Scan Priority, Percentage, Concurrent, Nobins
 *
 * @param err                   The as_error to be populated by the function
 *                              with the encountered error if any.
 * @scan_p                      Scan parameter.
 * @py_options                  The user's optional scan options.
 */
void set_scan_options(as_error *err, as_scan* scan_p, PyObject * py_options)
{
	if (!scan_p) {
		as_error_update(err, AEROSPIKE_ERR_CLIENT, "Scan is not initialized");
		return ;
	}

	if (PyDict_Check(py_options)) {
		PyObject *key = NULL, *value = NULL;
		Py_ssize_t pos = 0;
		int64_t val = 0;
		while (PyDict_Next(py_options, &pos, &key, &value)) {
			char *key_name = PyString_AsString(key);
			if (!PyString_Check(key)) {
				as_error_update(err, AEROSPIKE_ERR_PARAM, "Policy key must be string");
				break;
			}

			if (strcmp("priority", key_name) == 0) {
				if (!PyInt_Check(value)) {
					as_error_update(err, AEROSPIKE_ERR_PARAM, "Invalid value(type) for priority");
					break;
				}
				val = (int64_t) PyInt_AsLong(value);
				if (!as_scan_set_priority(scan_p, val)) {
					as_error_update(err, AEROSPIKE_ERR_PARAM, "Unable to set scan priority");
					break;
				}
			} else if (strcmp("percent", key_name) == 0) {
				if (!PyInt_Check(value)) {
					as_error_update(err, AEROSPIKE_ERR_PARAM, "Invalid value(type) for percent");
					break;
				}
				val = (int64_t) PyInt_AsLong(value);
				if (val<0 || val>100) {
					as_error_update(err, AEROSPIKE_ERR_PARAM, "Invalid value for scan percentage");
					break;
				}
				else if (!as_scan_set_percent(scan_p, val)) {
					as_error_update(err, AEROSPIKE_ERR_PARAM, "Unable to set scan percentage");
					break;
				}
			} else if (strcmp("concurrent", key_name) == 0) {
				if (!PyBool_Check(value)) {
					as_error_update(err, AEROSPIKE_ERR_PARAM, "Invalid value(type) for concurrent");
					break;
				}
				val = (int64_t)PyObject_IsTrue(value);
				if (val == -1 || (!as_scan_set_concurrent(scan_p, val))) {
					as_error_update(err, AEROSPIKE_ERR_PARAM, "Unable to set scan concurrent");
					break;
				}
			} else if (strcmp("nobins", key_name) == 0) {
				if (!PyBool_Check(value)) {
					as_error_update(err, AEROSPIKE_ERR_PARAM, "Invalid value(type) for nobins");
					break;
				}
				val = (int64_t)PyObject_IsTrue(value);
				if (val == -1 || (!as_scan_set_nobins(scan_p, val))) {
					as_error_update(err, AEROSPIKE_ERR_PARAM, "Unable to set scan nobins");
					break;
				}
			} else if (strcmp("include_ldt", key_name) == 0) {
				if (!PyBool_Check(value)) {
					as_error_update(err, AEROSPIKE_ERR_PARAM, "Invalid value(type) for nobins");
					break;
				}
				val = (int8_t)PyObject_IsTrue(value);
				if (val == -1) {
					as_error_update(err, AEROSPIKE_ERR_PARAM, "Unable to set scan nobins");
					break;
				}
				scan_p->include_ldt = val;
			} else {
				as_error_update(err, AEROSPIKE_ERR_PARAM, "Invalid value for scan options");
				break;
			}
		}
	} else {
		as_error_update(err, AEROSPIKE_ERR_PARAM, "Invalid option(type)");
	}
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
		PyModule_AddIntConstant(aerospike,
				aerospike_constants[i].constant_str,
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
as_status pyobject_to_policy_admin(as_error * err, PyObject * py_policy,
		as_policy_admin * policy,
		as_policy_admin ** policy_p,
		as_policy_admin * config_admin_policy)
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
as_status pyobject_to_policy_apply(as_error * err, PyObject * py_policy,
		as_policy_apply * policy,
		as_policy_apply ** policy_p,
		as_policy_apply * config_apply_policy)
{
	// Initialize Policy
	POLICY_INIT(as_policy_apply);
	
	//Initialize policy with global defaults
	as_policy_apply_copy(config_apply_policy, policy);

	// Set policy fields
	POLICY_SET_FIELD(timeout, uint32_t);
	POLICY_SET_FIELD(key, as_policy_key);
	POLICY_SET_FIELD(retry_on_timeout, bool);
	POLICY_SET_FIELD(durable_delete, bool);

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
as_status pyobject_to_policy_info(as_error * err, PyObject * py_policy,
		as_policy_info * policy,
		as_policy_info ** policy_p,
		as_policy_info * config_info_policy)
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
 */
as_status pyobject_to_policy_query(as_error * err, PyObject * py_policy,
		as_policy_query * policy,
		as_policy_query ** policy_p,
		as_policy_query * config_query_policy)
{
	// Initialize Policy
	POLICY_INIT(as_policy_query);

	//Initialize policy with global defaults
	as_policy_query_copy(config_query_policy, policy);

	// Set policy fields
	POLICY_SET_FIELD(timeout, uint32_t);

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
as_status pyobject_to_policy_read(as_error * err, PyObject * py_policy,
		as_policy_read * policy,
		as_policy_read ** policy_p,
		as_policy_read * config_read_policy)
{
	// Initialize Policy
	POLICY_INIT(as_policy_read);
	
	//Initialize policy with global defaults
	as_policy_read_copy(config_read_policy, policy);

	// Set policy fields
	POLICY_SET_FIELD(timeout, uint32_t);
	POLICY_SET_FIELD(key, as_policy_key);
	POLICY_SET_FIELD(consistency_level, as_policy_consistency_level);
	POLICY_SET_FIELD(replica, as_policy_replica);
	POLICY_SET_FIELD(retry_on_timeout, bool);

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
as_status pyobject_to_policy_remove(as_error * err, PyObject * py_policy,
		as_policy_remove * policy,
		as_policy_remove ** policy_p,
		as_policy_remove * config_remove_policy)
{
	// Initialize Policy
	POLICY_INIT(as_policy_remove);
	
	//Initialize policy with global defaults
	as_policy_remove_copy(config_remove_policy, policy);

	// Set policy fields
	POLICY_SET_FIELD(timeout, uint32_t);
	POLICY_SET_FIELD(generation, uint16_t);
	POLICY_SET_FIELD(retry, as_policy_retry);
	POLICY_SET_FIELD(key, as_policy_key);
	POLICY_SET_FIELD(gen, as_policy_gen);
	POLICY_SET_FIELD(commit_level, as_policy_commit_level);
	POLICY_SET_FIELD(retry_on_timeout, bool);
	POLICY_SET_FIELD(durable_delete, bool);

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
as_status pyobject_to_policy_scan(as_error * err, PyObject * py_policy,
		as_policy_scan * policy,
		as_policy_scan ** policy_p,
		as_policy_scan * config_scan_policy)
{
	// Initialize Policy
	POLICY_INIT(as_policy_scan);

	//Initialize policy with global defaults
	as_policy_scan_copy(config_scan_policy, policy);

	// Set policy fields
	// server side socket_timeout
	POLICY_SET_FIELD(socket_timeout, uint32_t);
	POLICY_SET_FIELD(timeout, uint32_t);
	POLICY_SET_FIELD(fail_on_cluster_change, bool);
	POLICY_SET_FIELD(durable_delete, bool);

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
as_status pyobject_to_policy_write(as_error * err, PyObject * py_policy,
		as_policy_write * policy,
		as_policy_write ** policy_p,
		as_policy_write * config_write_policy)
{
	// Initialize Policy
	POLICY_INIT(as_policy_write);

	//Initialize policy with global defaults
	as_policy_write_copy(config_write_policy, policy);

	// Set policy fields
	POLICY_SET_FIELD(timeout, uint32_t);
	POLICY_SET_FIELD(retry, as_policy_retry);
	POLICY_SET_FIELD(key, as_policy_key);
	POLICY_SET_FIELD(gen, as_policy_gen);
	POLICY_SET_FIELD(exists, as_policy_exists);
	POLICY_SET_FIELD(commit_level, as_policy_commit_level);
	POLICY_SET_FIELD(retry_on_timeout, bool);
	POLICY_SET_FIELD(durable_delete, bool);

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
as_status pyobject_to_policy_operate(as_error * err, PyObject * py_policy,
		as_policy_operate * policy,
		as_policy_operate ** policy_p,
		as_policy_operate * config_operate_policy)
{
	// Initialize Policy
	POLICY_INIT(as_policy_operate);
	
	//Initialize policy with global defaults
	as_policy_operate_copy(config_operate_policy, policy);

	// Set policy fields
	POLICY_SET_FIELD(timeout, uint32_t);
	POLICY_SET_FIELD(retry, as_policy_retry);
	POLICY_SET_FIELD(key, as_policy_key);
	POLICY_SET_FIELD(gen, as_policy_gen);
	POLICY_SET_FIELD(commit_level, as_policy_commit_level);
	POLICY_SET_FIELD(consistency_level, as_policy_consistency_level);
	POLICY_SET_FIELD(replica, as_policy_replica);
	POLICY_SET_FIELD(retry_on_timeout, bool);
	POLICY_SET_FIELD(durable_delete, bool);

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
as_status pyobject_to_policy_batch(as_error * err, PyObject * py_policy,
		as_policy_batch * policy,
		as_policy_batch ** policy_p,
		as_policy_batch * config_batch_policy)
{
	// Initialize Policy
	POLICY_INIT(as_policy_batch);

	//Initialize policy with global defaults
	as_policy_batch_copy(config_batch_policy, policy);

	// Set policy fields
	POLICY_SET_FIELD(timeout, uint32_t);
	POLICY_SET_FIELD(retry_on_timeout, bool);

	// Update the policy
	POLICY_UPDATE();

	return err->code;
}

as_status pyobject_to_map_policy(as_error * err, PyObject * py_policy,
		as_map_policy * policy)
{
	// Initialize Policy
	POLICY_INIT(as_map_policy);

	long map_order = AS_MAP_UNORDERED;
	long map_write_mode = AS_MAP_UPDATE;

	MAP_POLICY_SET_FIELD(map_write_mode);
	MAP_POLICY_SET_FIELD(map_order);

	as_map_policy_set(policy, map_order, map_write_mode);

	return err->code;
}
