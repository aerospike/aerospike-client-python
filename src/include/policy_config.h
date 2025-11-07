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
#include "types.h"

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

// Although set_subpolicies is called by AerospikeClient's init method, and it takes in AerospikeClient as a param,
// it should be safe to use because set_subpolicies only reads from self->validate_keys in this case
// We know that self->validate_keys is initialized by the time we call this.
// TODO: refactor set_subpolicies to not depend on AerospikeClient
//
// This only sets the err object if an invalid dictionary key is passed
// On error, return an error code
as_status set_subpolicies(AerospikeClient *self, as_error *err,
                          as_config *config, PyObject *py_policies);
