/*******************************************************************************
 *
 *   Copyright 2013-2014 Aerospike, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 ******************************************************************************/

#include <Python.h>
#include <aerospike/as_error.h>
#include <aerospike/as_policy.h>

as_status pyobject_to_policy_apply(as_error * err, PyObject * py_policy,
									as_policy_apply * policy,
									as_policy_apply ** policy_p);

as_status pyobject_to_policy_info(as_error * err, PyObject * py_policy,
									as_policy_info * policy,
									as_policy_info ** policy_p);

as_status pyobject_to_policy_query(as_error * err, PyObject * py_policy,
									as_policy_query * policy,
									as_policy_query ** policy_p);

as_status pyobject_to_policy_read(as_error * err, PyObject * py_policy,
									as_policy_read * policy,
									as_policy_read ** policy_p);

as_status pyobject_to_policy_remove(as_error * err, PyObject * py_policy,
									as_policy_remove * policy,
									as_policy_remove ** policy_p);

as_status pyobject_to_policy_scan(as_error * err, PyObject * py_policy,
									as_policy_scan * policy,
									as_policy_scan ** policy_p);

as_status pyobject_to_policy_write(as_error * err, PyObject * py_policy,
									as_policy_write * policy,
									as_policy_write ** policy_p);
