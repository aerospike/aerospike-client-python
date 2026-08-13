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

#include <Python.h>

#include <aerospike/as_config.h>

#include "macros.h"

as_status set_optional_uint32_property(uint32_t *target_ptr,
                                       PyObject *policy_dict, const char *name);
as_status set_optional_uint16_property(uint16_t *target_ptr,
                                       PyObject *py_policy, const char *name);
as_status set_optional_bool_property(bool *target_ptr, PyObject *py_policy,
                                     const char *name);
as_status set_base_policy(as_policy_base *base, PyObject *py_policy);

as_status set_optional_key(as_policy_key *target_ptr, PyObject *py_policy,
                           const char *name);
as_status set_optional_replica(as_policy_replica *target_ptr,
                               PyObject *py_policy, const char *name);
as_status set_optional_commit_level(as_policy_commit_level *target_ptr,
                                    PyObject *py_policy, const char *name);
as_status set_optional_gen(as_policy_gen *target_ptr, PyObject *py_policy,
                           const char *name);
as_status set_optional_exists(as_policy_exists *target_ptr, PyObject *py_policy,
                              const char *name);

// This only sets the err object if an invalid dictionary key is passed
// On error, return an error code
as_status set_subpolicies(as_error *err, as_config *config,
                          PyObject *py_policies, int validate_keys);
as_status set_read_policy(as_error *err, as_policy_read *read_policy,
                          PyObject *py_policy, int validate_keys);
as_status set_write_policy(as_error *err, as_policy_write *write_policy,
                           PyObject *py_policy, int validate_keys);
as_status set_apply_policy(as_error *err, as_policy_apply *apply_policy,
                           PyObject *py_policy, int validate_keys);
as_status set_remove_policy(as_error *err, as_policy_remove *remove_policy,
                            PyObject *py_policy, int validate_keys);
as_status set_query_policy(as_error *err, as_policy_query *query_policy,
                           PyObject *py_policy, int validate_keys);
as_status set_scan_policy(as_error *err, as_policy_scan *scan_policy,
                          PyObject *py_policy, int validate_keys);
as_status set_operate_policy(as_error *err, as_policy_operate *operate_policy,
                             PyObject *py_policy, int validate_keys);
as_status set_batch_policy(as_error *err, as_policy_batch *batch_policy,
                           PyObject *py_policy, int validate_keys);
as_status set_info_policy(as_error *err, as_policy_info *info_policy,
                          PyObject *py_policy, int validate_keys);
as_status set_admin_policy(as_error *err, as_policy_admin *admin_policy,
                           PyObject *py_policy, int validate_keys);
as_status set_batch_apply_policy(as_error *err,
                                 as_policy_batch_apply *batch_apply_policy,
                                 PyObject *py_policy, int validate_keys);
as_status set_batch_write_policy(as_error *err,
                                 as_policy_batch_write *batch_write_policy,
                                 PyObject *py_policy, int validate_keys);
as_status set_batch_remove_policy(as_error *err,
                                  as_policy_batch_remove *batch_remove_policy,
                                  PyObject *py_policy, int validate_keys);

// Builds a dict with one key per policy type, mirroring set_subpolicies.
// These convert trusted internal C policy structs (not user input), so
// unlike the set_*_policy functions above they return the dict directly.
PyObject *as_policies_to_pyobject(const as_policies *policies);
PyObject *as_policy_read_to_pyobject(const as_policy_read *read_policy);
PyObject *as_policy_write_to_pyobject(const as_policy_write *write_policy);
PyObject *as_policy_apply_to_pyobject(const as_policy_apply *apply_policy);
PyObject *as_policy_remove_to_pyobject(const as_policy_remove *remove_policy);
PyObject *as_policy_query_to_pyobject(const as_policy_query *query_policy);
PyObject *as_policy_scan_to_pyobject(const as_policy_scan *scan_policy);
PyObject *
as_policy_operate_to_pyobject(const as_policy_operate *operate_policy);
PyObject *as_policy_batch_to_pyobject(const as_policy_batch *batch_policy);
PyObject *as_policy_info_to_pyobject(const as_policy_info *info_policy);
PyObject *as_policy_admin_to_pyobject(const as_policy_admin *admin_policy);
PyObject *as_policy_batch_apply_to_pyobject(
    const as_policy_batch_apply *batch_apply_policy);
PyObject *as_policy_batch_write_to_pyobject(
    const as_policy_batch_write *batch_write_policy);
PyObject *as_policy_batch_remove_to_pyobject(
    const as_policy_batch_remove *batch_remove_policy);
