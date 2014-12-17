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
#include "aerospike/as_scan.h"

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

/*
 *******************************************************************************************************
 * Mapping of constant number to constant name string.
 *******************************************************************************************************
 */
static
AerospikeConstants aerospike_constants[] = {
    { POLICY_RETRY_NONE                 ,   "POLICY_RETRY_NONE" },
    { POLICY_RETRY_ONCE                 ,   "POLICY_RETRY_ONCE" },
    { POLICY_EXISTS_IGNORE              ,   "POLICY_EXISTS_IGNORE" },
    { POLICY_EXISTS_CREATE              ,   "POLICY_EXISTS_CREATE" },
    { POLICY_EXISTS_UPDATE              ,   "POLICY_EXISTS_UPDATE" },
    { POLICY_EXISTS_REPLACE             ,   "POLICY_EXISTS_REPLACE" },
    { POLICY_EXISTS_CREATE_OR_REPLACE   ,   "POLICY_EXISTS_CREATE_OR_REPLACE" },
    { UDF_TYPE_LUA                      ,   "UDF_TYPE_LUA" },
    { POLICY_KEY_DIGEST                 ,   "POLICY_KEY_DIGEST" },
    { POLICY_KEY_SEND                   ,   "POLICY_KEY_SEND" },
    { POLICY_GEN_IGNORE                 ,   "POLICY_GEN_IGNORE" },
    { POLICY_GEN_EQ                     ,   "POLICY_GEN_EQ" },
    { POLICY_GEN_GT                     ,   "POLICY_GEN_GT" },
    { SCAN_PRIORITY_AUTO                ,   "SCAN_PRIORITY_AUTO" },
    { SCAN_PRIORITY_LOW                 ,   "SCAN_PRIORITY_AUTO" },
    { SCAN_PRIORITY_MEDIUM              ,   "SCAN_PRIORITY_MEDIUM" },
    { SCAN_PRIORITY_HIGH                ,   "SCAN_PRIORITY_HIGH" },
    { SCAN_STATUS_COMPLETED             ,   "SCAN_STATUS_COMPLETED" },
    { SCAN_STATUS_ABORTED               ,   "SCAN_STATUS_ABORTED" },
    { SCAN_STATUS_UNDEF                 ,   "SCAN_STATUS_UNDEF" },
    { SCAN_STATUS_INPROGRESS            ,   "SCAN_STATUS_INPROGRESS" }
};

/**
 * Function for validating aerospike policies.
 *
 * @param err                   The as_error to be populated by the function
 *                              with the encountered error if any.
 * @param read_policy_p         The as_policy_read to be passed in case of
 *                              connect/get.
 * @param write_policy_p        The as_policy_write to be passed in case of
 *                              connect/put.
 * @param operate_policy_p      The as_policy_operate to be passed in case of
 *                              operations:append, prepend, increment, touch
 *                              and operate.
 * @param remove_policy_p       The as_policy_remove to be passed in case of
 *                              remove.
 */
static
void validate_policy(as_error *err, PyObject * py_policy, as_policy_read* read_policy_p,
        as_policy_write* write_policy_p, as_policy_operate* operate_policy_p,
        as_policy_remove* remove_policy_p, as_policy_apply* apply_policy_p)
{
    if (PyDict_Check(py_policy)) {
        PyObject *key = NULL, *value = NULL;
        Py_ssize_t pos = 0;
        int64_t val = 0;

        while (PyDict_Next(py_policy, &pos, &key, &value)) {
            if (!PyString_Check(key)) {
                as_error_update(err, AEROSPIKE_ERR_PARAM, "Policy key must be string");
                break;
            }
            if (!PyInt_Check(value)) {
                as_error_update(err, AEROSPIKE_ERR_PARAM, "Invalid value(type) for policy key");
                break;
            }

            char *key_name = PyString_AsString(key);
            if (strcmp("timeout", key_name) == 0) {
            } else if (strcmp("exists", key_name) == 0) {
                val = (int64_t) PyInt_AsLong(value);
                if ((val & AS_POLICY_EXISTS) != AS_POLICY_EXISTS) {
                    as_error_update(err, AEROSPIKE_ERR_PARAM, "Invalid value for OPT_POLICY_EXISTS");
                    break;
                }
                if (write_policy_p) {
                    PyObject *py_key = PyDict_GetItemString(py_policy, "exists");
                    long keyval = PyInt_AsLong(py_key) - AS_POLICY_EXISTS;
                    PyObject * py_keyval = PyInt_FromLong(keyval);
                    PyDict_SetItemString(py_policy, "exists", py_keyval);
                } else {
                    as_error_update(err, AEROSPIKE_ERR_PARAM, "Invalid value for OPT_POLICY_KEY_EXISTS");
                    break;
                }
            } else if (strcmp("retry", key_name) == 0) {
                val = (int64_t) PyInt_AsLong(value);
                if ((val & AS_POLICY_RETRY) != AS_POLICY_RETRY) {
                    as_error_update(err, AEROSPIKE_ERR_PARAM, "Invalid value for OPT_POLICY_KEY");
                    break;
                }
                if (write_policy_p || operate_policy_p || remove_policy_p) {
                    PyObject *py_key = PyDict_GetItemString(py_policy, "retry");
                    long keyval = PyInt_AsLong(py_key) - AS_POLICY_RETRY;
                    PyObject * py_keyval = PyInt_FromLong(keyval);
                    PyDict_SetItemString(py_policy, "retry", py_keyval);
                } else {
                    as_error_update(err, AEROSPIKE_ERR_PARAM, "Invalid value for OPT_POLICY_KEY_GEN");
                    break;
                }
            } else if (strcmp("key", key_name) == 0) {
                val = (int64_t) PyInt_AsLong(value);
                if ((val & AS_POLICY_KEY_DIGEST) != AS_POLICY_KEY_DIGEST) {
                    as_error_update(err, AEROSPIKE_ERR_PARAM, "Invalid value for OPT_POLICY_KEY");
                    break;
                }
                if (read_policy_p || write_policy_p || operate_policy_p || remove_policy_p || apply_policy_p) {
                    PyObject *py_key = PyDict_GetItemString(py_policy, "key");
                    long keyval = PyInt_AsLong(py_key) - AS_POLICY_KEY_DIGEST;
                    PyObject * py_keyval = PyInt_FromLong(keyval);
                    PyDict_SetItemString(py_policy, "key", py_keyval);
                } else {
                    as_error_update(err, AEROSPIKE_ERR_PARAM, "Invalid value for OPT_POLICY_KEY_GEN");
                    break;
                }
            } else if (strcmp("gen", key_name) == 0) {
                val = (int64_t) PyInt_AsLong(value);
                if ((val & AS_POLICY_KEY_GEN) != AS_POLICY_KEY_GEN) {
                    as_error_update(err, AEROSPIKE_ERR_PARAM, "Invalid value for OPT_POLICY_KEY_GEN");
                    break;
                }
                if (write_policy_p || operate_policy_p || remove_policy_p) {
                    PyObject *py_key = PyDict_GetItemString(py_policy, "gen");
                    long keyval = PyInt_AsLong(py_key) - AS_POLICY_KEY_GEN;
                    PyObject * py_keyval = PyInt_FromLong(keyval);
                    PyDict_SetItemString(py_policy, "gen", py_keyval);
                } else {
                    as_error_update(err, AEROSPIKE_ERR_PARAM, "Invalid value for OPT_POLICY_KEY_GEN");
                    break;
                }
            } else {
                as_error_update(err, AEROSPIKE_ERR_PARAM, "Invalid value for policy key");
                break;
            }
        }
    } else {
        as_error_update(err, AEROSPIKE_ERR_PARAM, "Invalid policy(type)");
    }
}

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
                if ((val & AS_SCAN_PRIORITY) != AS_SCAN_PRIORITY) {
                    as_error_update(err, AEROSPIKE_ERR_PARAM, "Invalid value for priority");
                    break;
                } else if (!as_scan_set_priority(scan_p, (val - AS_SCAN_PRIORITY))) {
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
                    as_error_update(err, AEROSPIKE_ERR_PARAM, "Unable to set scan percentage");
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
    for (i = 0; i <= AEROSPIKE_CONSTANTS_ARR_SIZE; i++) {
        PyModule_AddIntConstant(aerospike,
                aerospike_constants[i].constant_str,
                aerospike_constants[i].constantno);
    }
exit:
    return status;
}

/**
 * Wrapper function for checking scan policy parameters.
 */
void validate_policy_scan(as_error *err, PyObject * py_policy, as_policy_scan* scan_policy_p)
{
    validate_policy(err, py_policy, NULL, NULL, NULL, NULL, NULL);
}
void validate_policy_query(as_error *err, PyObject * py_policy, as_policy_query* query_policy_p)
{
    validate_policy(err, py_policy, NULL, NULL, NULL, NULL, NULL);
}
/**
 * Wrapper function for checking operate policy parameters.
 */
void validate_policy_operate(as_error *err, PyObject * py_policy, as_policy_operate* operate_policy_p)
{
    validate_policy(err, py_policy, NULL, NULL, operate_policy_p, NULL, NULL);
}

/**
 * Wrapper function for checking info policy parameters.
 */
void validate_policy_info(as_error *err, PyObject * py_policy, as_policy_info* info_policy_p)
{
    validate_policy(err, py_policy, NULL, NULL, NULL, NULL, NULL);
}

/**
 * Wrapper function for checking info policy parameters.
 */
void validate_policy_admin(as_error *err, PyObject * py_policy, as_policy_admin* admin_policy_p)
{
    validate_policy(err, py_policy, NULL, NULL, NULL, NULL, NULL);
}

void validate_policy_batch(as_error *err, PyObject * py_policy, as_policy_batch* batch_policy_p)
{
    validate_policy(err, py_policy, NULL, NULL, NULL, NULL, NULL);
}

/*
     * Wrapper function for checking write policy parameters.
     */
void validate_policy_write(as_error *err, PyObject * py_policy, as_policy_write * write_policy_p)
{
    validate_policy(err, py_policy, NULL, write_policy_p, NULL, NULL, NULL);
}

/**
 * Wrapper function for checking read policy parameters.
 */
void validate_policy_read(as_error *err, PyObject * py_policy, as_policy_read * read_policy_p)
{
    validate_policy(err, py_policy, read_policy_p, NULL, NULL, NULL, NULL);
}

/**
 * Wrapper function for checking apply policy parameters.
 */
void validate_policy_apply(as_error *err, PyObject * py_policy, as_policy_apply * apply_policy_p)
{
    validate_policy(err, py_policy, NULL, NULL, NULL, NULL, apply_policy_p);
}

/**
 * Wrapper function for checking remove policy parameters.
 */
void validate_policy_remove(as_error *err, PyObject * py_policy, as_policy_remove * remove_policy_p)
{
    validate_policy(err, py_policy, NULL, NULL, NULL, remove_policy_p, NULL);
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

/**
 * Converts a PyObject into an as_policy_operate object.
 * Returns AEROSPIKE_OK on success. On error, the err argument is populated.
 * We assume that the error object and the policy object are already allocated
 * and initialized (although, we do reset the error object here).
 */
as_status pyobject_to_policy_operate(as_error * err, PyObject * py_policy,
		as_policy_operate * policy,
		as_policy_operate ** policy_p)
{
	// Initialize Policy
	POLICY_INIT(as_policy_operate);

	// Set policy fields
	POLICY_SET_FIELD(timeout, uint32_t);
	POLICY_SET_FIELD(retry, as_policy_retry);
	POLICY_SET_FIELD(key, as_policy_key);
	POLICY_SET_FIELD(gen, as_policy_gen);

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
                                   as_policy_batch ** policy_p)
{
	// Initialize Policy
	POLICY_INIT(as_policy_batch);

	// Set policy fields
	POLICY_SET_FIELD(timeout, uint32_t);

	// Update the policy
	POLICY_UPDATE();

	return err->code;

}
