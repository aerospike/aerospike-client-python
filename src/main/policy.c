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

#include <aerospike/as_error.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_status.h>

#include "policy.h"

// Policy names
#define PY_POLICY_TIMEOUT "timeout"		// Number of milliseconds to wait
#define PY_POLICY_RETRY   "retry"		// Behavior of failed operations
#define PY_POLICY_KEY     "key"			// Behavior of the key
#define PY_POLICY_GEN     "gen"			// Behavior of the Generation value
#define PY_POLICY_EXISTS  "exists"		// Behavior for record existence

#define POLICY_INIT(__policy) \
	as_error_reset(err);\
if ( ! py_policy || py_policy == Py_None ) {\
	return err->code;\
}\
if ( ! PyDict_Check(py_policy) ) {\
	return as_error_update(err, AEROSPIKE_ERR_PARAM, "policy must be a dict");\
}\
__policy##_init(policy);\

#define POLICY_UPDATE() \
	*policy_p = policy;

#define POLICY_SET_FIELD(__field, __type) { \
	PyObject * py_field = PyDict_GetItemString(py_policy, #__field);\
	if ( py_field ) {\
		if ( PyInt_Check(py_field) ) {\
			policy->__field = (__type) PyInt_AsLong(py_field);\
		}\
		else {\
			return as_error_update(err, AEROSPIKE_ERR_PARAM, "%s is invalid", #__field);\
		}\
	}\
}

/**
 * Converts a PyObject into an as_policy_admin object.
 * Returns AEROSPIKE_OK on success. On error, the err argument is populated.
 * We assume that the error object and the policy object are already allocated
 * and initialized (although, we do reset the error object here).
 */
as_status pyobject_to_policy_admin(as_error * err, PyObject * py_policy,
		as_policy_admin * policy,
		as_policy_admin ** policy_p)
{

	// Initialize Policy
	POLICY_INIT(as_policy_admin);

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
		as_policy_apply ** policy_p)
{
	// Initialize Policy
	POLICY_INIT(as_policy_apply);

	// Set policy fields
	POLICY_SET_FIELD(timeout, uint32_t);
	POLICY_SET_FIELD(key, as_policy_key);

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
		as_policy_info ** policy_p)
{
	// Initialize Policy
	POLICY_INIT(as_policy_info);

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
		as_policy_query ** policy_p)
{
	// Initialize Policy
	POLICY_INIT(as_policy_query);

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
		as_policy_read ** policy_p)
{
	// Initialize Policy
	POLICY_INIT(as_policy_read);

	// Set policy fields
	POLICY_SET_FIELD(timeout, uint32_t);
	POLICY_SET_FIELD(key, as_policy_key);

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
		as_policy_remove ** policy_p)
{
	// Initialize Policy
	POLICY_INIT(as_policy_remove);

	// Set policy fields
	POLICY_SET_FIELD(timeout, uint32_t);
	POLICY_SET_FIELD(generation, uint16_t);
	POLICY_SET_FIELD(retry, as_policy_retry);
	POLICY_SET_FIELD(key, as_policy_key);
	POLICY_SET_FIELD(gen, as_policy_gen);

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
		as_policy_scan ** policy_p)
{
	// Initialize Policy
	POLICY_INIT(as_policy_scan);

	// Set policy fields
	POLICY_SET_FIELD(timeout, uint32_t);
	POLICY_SET_FIELD(fail_on_cluster_change, bool);

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
		as_policy_write ** policy_p)
{
	// Initialize Policy
	POLICY_INIT(as_policy_write);

	// Set policy fields
	POLICY_SET_FIELD(timeout, uint32_t);
	POLICY_SET_FIELD(retry, as_policy_retry);
	POLICY_SET_FIELD(key, as_policy_key);
	POLICY_SET_FIELD(gen, as_policy_gen);
	POLICY_SET_FIELD(exists, as_policy_exists);

	// Update the policy
	POLICY_UPDATE();

	return err->code;
}

as_status pyobject_to_policy_batch(as_error * err, PyObject * py_policy,
		as_policy_batch * policy,
		as_policy_batch ** policy_p){

	// Initialize Policy
	POLICY_INIT(as_policy_batch);

	// Set policy fields
	POLICY_SET_FIELD(timeout, uint32_t);

	// Update the policy
	POLICY_UPDATE();

	return err->code;
}