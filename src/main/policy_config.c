/*******************************************************************************
 * Copyright 2017-2021 Aerospike, Inc.
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
#include <stdint.h>

#include "policy_config.h"
#include "types.h"

#include "policy.h"

as_status get_uint32_value(PyObject *py_policy_val, uint32_t *return_uint32);

/*
 * py_policies must exist, and be a dictionary
 */
as_status set_subpolicies(AerospikeClient *self, as_error *err,
                          as_config *config, PyObject *py_policies_dict)
{
    as_status set_policy_status = AEROSPIKE_OK;
    PyObject *py_read_policy = PyDict_GetItemString(py_policies_dict, "read");
    set_policy_status = as_policy_read_set_from_pyobject(
        self, err, py_read_policy, &config->policies.read, false);
    if (set_policy_status != AEROSPIKE_OK) {
        return set_policy_status;
    }

    PyObject *py_write_policy = PyDict_GetItemString(py_policies_dict, "write");
    set_policy_status = as_policy_write_set_from_pyobject(
        self, err, py_write_policy, &config->policies.write, false);
    if (set_policy_status != AEROSPIKE_OK) {
        return set_policy_status;
    }

    PyObject *py_apply_policy = PyDict_GetItemString(py_policies_dict, "apply");
    set_policy_status = as_policy_apply_set_from_pyobject(
        self, err, py_apply_policy, &config->policies.apply, false);
    if (set_policy_status != AEROSPIKE_OK) {
        return set_policy_status;
    }

    PyObject *remove_policy = PyDict_GetItemString(py_policies_dict, "remove");
    // set_policy_status = set_remove_policy(err, &config->policies.remove,
    //                                       remove_policy, self->validate_keys);
    // if (set_policy_status != AEROSPIKE_OK) {
    //     return set_policy_status;
    // }

    PyObject *query_policy = PyDict_GetItemString(py_policies_dict, "query");
    // set_policy_status = set_query_policy(err, &config->policies.query,
    //                                      query_policy, self->validate_keys);
    // if (set_policy_status != AEROSPIKE_OK) {
    //     return set_policy_status;
    // }

    PyObject *scan_policy = PyDict_GetItemString(py_policies_dict, "scan");
    // set_policy_status = set_scan_policy(err, &config->policies.scan,
    //                                     scan_policy, self->validate_keys);
    // if (set_policy_status != AEROSPIKE_OK) {
    //     return set_policy_status;
    // }

    PyObject *operate_policy =
        PyDict_GetItemString(py_policies_dict, "operate");
    // set_policy_status = set_operate_policy(err, &config->policies.operate,
    //                                        operate_policy, self->validate_keys);
    // if (set_policy_status != AEROSPIKE_OK) {
    //     return set_policy_status;
    // }

    PyObject *info_policy = PyDict_GetItemString(py_policies_dict, "info");
    // set_policy_status = set_info_policy(err, &config->policies.info,
    //                                     info_policy, self->validate_keys);
    // if (set_policy_status != AEROSPIKE_OK) {
    //     return set_policy_status;
    // }

    PyObject *admin_policy = PyDict_GetItemString(py_policies_dict, "admin");
    // set_policy_status = set_admin_policy(err, &config->policies.admin,
    //                                      admin_policy, self->validate_keys);
    // if (set_policy_status != AEROSPIKE_OK) {
    //     return set_policy_status;
    // }

    PyObject *batch_apply_policy =
        PyDict_GetItemString(py_policies_dict, "batch_apply");
    set_policy_status = as_policy_batch_apply_copy_and_set_from_pyobject(
        err, &config->policies.batch_apply, batch_apply_policy,
        self->validate_keys);
    if (set_policy_status != AEROSPIKE_OK) {
        return set_policy_status;
    }

    PyObject *batch_remove_policy =
        PyDict_GetItemString(py_policies_dict, "batch_remove");
    // set_policy_status =
    //     set_batch_remove_policy(err, &config->policies.batch_remove,
    //                             batch_remove_policy, self->validate_keys);
    // if (set_policy_status != AEROSPIKE_OK) {
    //     return set_policy_status;
    // }

    PyObject *batch_write_policy =
        PyDict_GetItemString(py_policies_dict, "batch_write");
    // set_policy_status =
    //     set_batch_write_policy(err, &config->policies.batch_write,
    //                            batch_write_policy, self->validate_keys);
    // if (set_policy_status != AEROSPIKE_OK) {
    //     return set_policy_status;
    // }

    const char *batch_policy_names[] = {"batch", "batch_parent_write",
                                        "txn_verify", "txn_roll"};
    as_policy_batch *batch_policies[] = {
        &config->policies.batch,
        &config->policies.batch_parent_write,
        &config->policies.txn_verify,
        &config->policies.txn_roll,
    };
    for (unsigned long i = 0;
         i < sizeof(batch_policy_names) / sizeof(batch_policy_names[0]); i++) {
        PyObject *py_batch_policy =
            PyDict_GetItemString(py_policies_dict, batch_policy_names[i]);
        set_policy_status = as_policy_batch_copy_and_set_from_pyobject(
            err, batch_policies[i], py_batch_policy, self->validate_keys);
        if (set_policy_status != AEROSPIKE_OK) {
            return set_policy_status;
        }
    }
    // Default metrics policy is processed right after this call in the client constructor code
    // If this function fails, the calling function always sets as_error with our own error code and message
    // But when reading the config-level metrics policy, we want to propagate native Python exceptions up to the user
    return AEROSPIKE_OK;
}

// For batch write, batch apply, and batch remove policies:
// Don't set expressions field since it depends on the client's
// serialization policy

as_status get_uint32_value(PyObject *py_policy_val, uint32_t *return_uint32)
{
    long long int uint32_max = 0xFFFFFFFF;

    if (!py_policy_val) {
        return AEROSPIKE_ERR_PARAM;
    }
    if (PyLong_Check(py_policy_val)) {
        long int_value = PyLong_AsLong(py_policy_val);

        if (int_value == -1 && PyErr_Occurred()) {
            PyErr_Clear();
            return AEROSPIKE_ERR_PARAM;
        }

        if (int_value < 0 || int_value > uint32_max) {
            return AEROSPIKE_ERR_PARAM;
        }

        *return_uint32 = (uint32_t)int_value;
        return AEROSPIKE_OK;
    }
    return AEROSPIKE_ERR_PARAM;
}

as_status set_optional_uint32_property(uint32_t *target_ptr,
                                       PyObject *py_policy, const char *name)
{
    PyObject *py_policy_val = NULL;
    long long int uint32_max = 0xFFFFFFFF;
    if (!py_policy || !PyDict_Check(py_policy)) {
        return AEROSPIKE_OK;
    }

    py_policy_val = PyDict_GetItemString(py_policy, name);
    if (!py_policy_val) {
        return AEROSPIKE_OK;
    }
    if (PyLong_Check(py_policy_val)) {
        long int_value = PyLong_AsLong(py_policy_val);

        if (int_value == -1 && PyErr_Occurred()) {
            // This wasn't a valid int, or was too large
            // We are handling the error ourselves, so clear the overflow error
            PyErr_Clear();
            return AEROSPIKE_ERR_PARAM;

            /* If the number was less than zero, or would not fit in a uint32, error */
        }
        if (int_value < 0 || int_value > uint32_max) {
            return AEROSPIKE_ERR_PARAM;
        }

        *target_ptr = (uint32_t)int_value;
        return AEROSPIKE_OK;
    }
    return AEROSPIKE_ERR_PARAM;
}

as_status set_optional_uint16_property(uint16_t *target_ptr,
                                       PyObject *py_policy, const char *name)
{
    // Assume py_policy is a Python dictionary
    PyObject *py_policy_val = PyDict_GetItemString(py_policy, name);
    if (!py_policy_val) {
        // Key doesn't exist in policy
        return AEROSPIKE_OK;
    }
    Py_INCREF(py_policy_val);

    if (!PyLong_Check(py_policy_val)) {
        return AEROSPIKE_ERR_PARAM;
    }

    long int_value = PyLong_AsLong(py_policy_val);
    if (int_value == -1 && PyErr_Occurred()) {
        // This wasn't a valid int, or was too large
        // We are handling the error ourselves, so clear the overflow error
        PyErr_Clear();
        return AEROSPIKE_ERR_PARAM;

        /* If the number was less than zero, or would not fit in a uint16, error */
    }
    if (int_value < 0 || int_value > UINT16_MAX) {
        return AEROSPIKE_ERR_PARAM;
    }

    *target_ptr = (uint16_t)int_value;
    return AEROSPIKE_OK;
}

as_status set_optional_bool_property(bool *target_ptr, PyObject *py_policy,
                                     const char *name)
{
    PyObject *py_policy_val = NULL;
    if (!py_policy || !PyDict_Check(py_policy)) {
        return AEROSPIKE_OK;
    }

    py_policy_val = PyDict_GetItemString(py_policy, name);
    if (!py_policy_val) {
        return AEROSPIKE_OK;
    }
    if (PyBool_Check(py_policy_val)) {
        *target_ptr = PyObject_IsTrue(py_policy_val);
        return AEROSPIKE_OK;
    }
    return AEROSPIKE_ERR_PARAM;
}
