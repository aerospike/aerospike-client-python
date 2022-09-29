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

as_status set_subpolicies(as_config *config, PyObject *py_policies);
as_status set_read_policy(as_policy_read *read_policy, PyObject *py_policy);
as_status set_write_policy(as_policy_write *write_policy, PyObject *py_policy);
as_status set_apply_policy(as_policy_apply *apply_policy, PyObject *py_policy);
as_status set_remove_policy(as_policy_remove *remove_policy,
							PyObject *py_policy);
as_status set_query_policy(as_policy_query *query_policy, PyObject *py_policy);
as_status set_scan_policy(as_policy_scan *scan_policy, PyObject *py_policy);
as_status set_operate_policy(as_policy_operate *operate_policy,
							 PyObject *py_policy);
as_status set_batch_policy(as_policy_batch *batch_policy, PyObject *py_policy);
