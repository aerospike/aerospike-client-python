/*******************************************************************************
 * Copyright 2013-2026 Aerospike, Inc.
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

#include <aerospike/aerospike.h>

#include "client.h"

// This file only converts trusted internal C policy structs into Python
// dicts (the reverse of the set_*_policy functions in policy_config.c) --
// there's no untrusted input to validate here, so unlike the set_*_policy
// functions, these don't check individual Python/C-API call results
// (matching e.g. AerospikeClient_shm_key and get_key_partition_id.c's
// PyDict_New() usage elsewhere in this codebase).
static void py_dict_set_long_long_value(PyObject *py_dict, const char *key,
                                        long long value)
{
    // Use long long (not long) so uint32_t policy fields > LONG_MAX are
    // still represented correctly on platforms where long is 32-bit.
    PyObject *py_val = PyLong_FromLongLong(value);
    PyDict_SetItemString(py_dict, key, py_val);
    Py_DECREF(py_val);
}

static void py_dict_set_bool_value(PyObject *py_dict, const char *key,
                                   bool value)
{
    PyObject *py_val = PyBool_FromLong((long)value);
    PyDict_SetItemString(py_dict, key, py_val);
    Py_DECREF(py_val);
}

// Takes ownership of py_sub_dict.
static void py_dict_set_dict_value(PyObject *py_dict, const char *key,
                                   PyObject *py_sub_dict)
{
    PyDict_SetItemString(py_dict, key, py_sub_dict);
    Py_DECREF(py_sub_dict);
}

// Adds the base policy fields directly into py_dict (flattened, not nested).
static void add_base_policy_fields(PyObject *py_dict,
                                   const as_policy_base *base)
{
    py_dict_set_long_long_value(py_dict, "total_timeout", base->total_timeout);
    py_dict_set_long_long_value(py_dict, "connect_timeout",
                                base->connect_timeout);
    py_dict_set_long_long_value(py_dict, "socket_timeout",
                                base->socket_timeout);
    py_dict_set_long_long_value(py_dict, "timeout_delay", base->timeout_delay);
    py_dict_set_long_long_value(py_dict, "max_retries", base->max_retries);
    py_dict_set_long_long_value(py_dict, "sleep_between_retries",
                                base->sleep_between_retries);
    py_dict_set_bool_value(py_dict, "compress", base->compress);
    py_dict_set_long_long_value(py_dict, "error_detail_verbosity",
                                base->error_detail_verbosity);
}

static inline PyObject *
as_policy_read_to_pyobject(const as_policy_read *read_policy)
{
    PyObject *py_dict = PyDict_New();
    add_base_policy_fields(py_dict, &read_policy->base);
    py_dict_set_long_long_value(py_dict, "key", read_policy->key);
    py_dict_set_long_long_value(py_dict, "replica", read_policy->replica);
    py_dict_set_bool_value(py_dict, "deserialize", read_policy->deserialize);
    py_dict_set_long_long_value(py_dict, "read_mode_ap",
                                read_policy->read_mode_ap);
    py_dict_set_long_long_value(py_dict, "read_mode_sc",
                                read_policy->read_mode_sc);
    py_dict_set_long_long_value(py_dict, "read_touch_ttl_percent",
                                read_policy->read_touch_ttl_percent);
    return py_dict;
}

static inline PyObject *
as_policy_write_to_pyobject(const as_policy_write *write_policy)
{
    PyObject *py_dict = PyDict_New();
    add_base_policy_fields(py_dict, &write_policy->base);
    py_dict_set_long_long_value(py_dict, "key", write_policy->key);
    py_dict_set_long_long_value(py_dict, "replica", write_policy->replica);
    py_dict_set_long_long_value(py_dict, "commit_level",
                                write_policy->commit_level);
    py_dict_set_long_long_value(py_dict, "gen", write_policy->gen);
    py_dict_set_long_long_value(py_dict, "exists", write_policy->exists);
    py_dict_set_long_long_value(py_dict, "ttl", write_policy->ttl);
    py_dict_set_long_long_value(py_dict, "compression_threshold",
                                write_policy->compression_threshold);
    py_dict_set_bool_value(py_dict, "durable_delete",
                           write_policy->durable_delete);
    return py_dict;
}

static inline PyObject *
as_policy_apply_to_pyobject(const as_policy_apply *apply_policy)
{
    PyObject *py_dict = PyDict_New();
    add_base_policy_fields(py_dict, &apply_policy->base);
    py_dict_set_long_long_value(py_dict, "key", apply_policy->key);
    py_dict_set_long_long_value(py_dict, "replica", apply_policy->replica);
    py_dict_set_long_long_value(py_dict, "ttl", apply_policy->ttl);
    py_dict_set_long_long_value(py_dict, "commit_level",
                                apply_policy->commit_level);
    py_dict_set_bool_value(py_dict, "durable_delete",
                           apply_policy->durable_delete);
    return py_dict;
}

static inline PyObject *
as_policy_remove_to_pyobject(const as_policy_remove *remove_policy)
{
    PyObject *py_dict = PyDict_New();
    add_base_policy_fields(py_dict, &remove_policy->base);
    py_dict_set_long_long_value(py_dict, "key", remove_policy->key);
    py_dict_set_long_long_value(py_dict, "replica", remove_policy->replica);
    py_dict_set_long_long_value(py_dict, "commit_level",
                                remove_policy->commit_level);
    py_dict_set_long_long_value(py_dict, "gen", remove_policy->gen);
    py_dict_set_bool_value(py_dict, "durable_delete",
                           remove_policy->durable_delete);
    return py_dict;
}

static inline PyObject *
as_policy_query_to_pyobject(const as_policy_query *query_policy)
{
    PyObject *py_dict = PyDict_New();
    add_base_policy_fields(py_dict, &query_policy->base);
    py_dict_set_bool_value(py_dict, "deserialize", query_policy->deserialize);
    py_dict_set_long_long_value(py_dict, "replica", query_policy->replica);
    py_dict_set_long_long_value(py_dict, "expected_duration",
                                query_policy->expected_duration);
    return py_dict;
}

static inline PyObject *
as_policy_scan_to_pyobject(const as_policy_scan *scan_policy)
{
    PyObject *py_dict = PyDict_New();
    add_base_policy_fields(py_dict, &scan_policy->base);
    py_dict_set_bool_value(py_dict, "durable_delete",
                           scan_policy->durable_delete);
    py_dict_set_long_long_value(py_dict, "ttl", scan_policy->ttl);
    py_dict_set_long_long_value(py_dict, "replica", scan_policy->replica);
    return py_dict;
}

static inline PyObject *
as_policy_operate_to_pyobject(const as_policy_operate *operate_policy)
{
    PyObject *py_dict = PyDict_New();
    add_base_policy_fields(py_dict, &operate_policy->base);
    py_dict_set_long_long_value(py_dict, "key", operate_policy->key);
    py_dict_set_long_long_value(py_dict, "replica", operate_policy->replica);
    py_dict_set_long_long_value(py_dict, "commit_level",
                                operate_policy->commit_level);
    py_dict_set_long_long_value(py_dict, "ttl", operate_policy->ttl);
    py_dict_set_long_long_value(py_dict, "gen", operate_policy->gen);
    py_dict_set_bool_value(py_dict, "durable_delete",
                           operate_policy->durable_delete);
    py_dict_set_bool_value(py_dict, "deserialize", operate_policy->deserialize);
    py_dict_set_long_long_value(py_dict, "read_mode_ap",
                                operate_policy->read_mode_ap);
    py_dict_set_long_long_value(py_dict, "read_mode_sc",
                                operate_policy->read_mode_sc);
    py_dict_set_long_long_value(py_dict, "read_touch_ttl_percent",
                                operate_policy->read_touch_ttl_percent);
    return py_dict;
}

static inline PyObject *
as_policy_batch_to_pyobject(const as_policy_batch *batch_policy)
{
    PyObject *py_dict = PyDict_New();
    add_base_policy_fields(py_dict, &batch_policy->base);
    py_dict_set_bool_value(py_dict, "concurrent", batch_policy->concurrent);
    py_dict_set_bool_value(py_dict, "allow_inline", batch_policy->allow_inline);
    py_dict_set_bool_value(py_dict, "deserialize", batch_policy->deserialize);
    py_dict_set_long_long_value(py_dict, "read_mode_ap",
                                batch_policy->read_mode_ap);
    py_dict_set_long_long_value(py_dict, "read_mode_sc",
                                batch_policy->read_mode_sc);
    py_dict_set_long_long_value(py_dict, "replica", batch_policy->replica);
    py_dict_set_long_long_value(py_dict, "read_touch_ttl_percent",
                                batch_policy->read_touch_ttl_percent);
    return py_dict;
}

static inline PyObject *
as_policy_info_to_pyobject(const as_policy_info *info_policy)
{
    PyObject *py_dict = PyDict_New();
    py_dict_set_long_long_value(py_dict, "timeout", info_policy->timeout);
    return py_dict;
}

static inline PyObject *
as_policy_admin_to_pyobject(const as_policy_admin *admin_policy)
{
    PyObject *py_dict = PyDict_New();
    py_dict_set_long_long_value(py_dict, "timeout", admin_policy->timeout);
    return py_dict;
}

static inline PyObject *as_policy_batch_apply_to_pyobject(
    const as_policy_batch_apply *batch_apply_policy)
{
    PyObject *py_dict = PyDict_New();
    py_dict_set_long_long_value(py_dict, "commit_level",
                                batch_apply_policy->commit_level);
    py_dict_set_bool_value(py_dict, "durable_delete",
                           batch_apply_policy->durable_delete);
    py_dict_set_long_long_value(py_dict, "key", batch_apply_policy->key);
    py_dict_set_long_long_value(py_dict, "ttl", batch_apply_policy->ttl);
    return py_dict;
}

static inline PyObject *as_policy_batch_write_to_pyobject(
    const as_policy_batch_write *batch_write_policy)
{
    PyObject *py_dict = PyDict_New();
    py_dict_set_long_long_value(py_dict, "commit_level",
                                batch_write_policy->commit_level);
    py_dict_set_bool_value(py_dict, "durable_delete",
                           batch_write_policy->durable_delete);
    py_dict_set_long_long_value(py_dict, "exists", batch_write_policy->exists);
    py_dict_set_long_long_value(py_dict, "ttl", batch_write_policy->ttl);
    py_dict_set_long_long_value(py_dict, "gen", batch_write_policy->gen);
    py_dict_set_long_long_value(py_dict, "key", batch_write_policy->key);
    return py_dict;
}

static inline PyObject *as_policy_batch_remove_to_pyobject(
    const as_policy_batch_remove *batch_remove_policy)
{
    PyObject *py_dict = PyDict_New();
    py_dict_set_long_long_value(py_dict, "commit_level",
                                batch_remove_policy->commit_level);
    py_dict_set_bool_value(py_dict, "durable_delete",
                           batch_remove_policy->durable_delete);
    py_dict_set_long_long_value(py_dict, "gen", batch_remove_policy->gen);
    py_dict_set_long_long_value(py_dict, "generation",
                                batch_remove_policy->generation);
    py_dict_set_long_long_value(py_dict, "key", batch_remove_policy->key);
    return py_dict;
}

// Builds the top-level policies dict, mirroring set_subpolicies's key set.
static inline PyObject *as_policies_to_pyobject(const as_policies *policies)
{
    PyObject *py_dict = PyDict_New();
    py_dict_set_dict_value(py_dict, "read",
                           as_policy_read_to_pyobject(&policies->read));
    py_dict_set_dict_value(py_dict, "write",
                           as_policy_write_to_pyobject(&policies->write));
    py_dict_set_dict_value(py_dict, "apply",
                           as_policy_apply_to_pyobject(&policies->apply));
    py_dict_set_dict_value(py_dict, "remove",
                           as_policy_remove_to_pyobject(&policies->remove));
    py_dict_set_dict_value(py_dict, "query",
                           as_policy_query_to_pyobject(&policies->query));
    py_dict_set_dict_value(py_dict, "scan",
                           as_policy_scan_to_pyobject(&policies->scan));
    py_dict_set_dict_value(py_dict, "operate",
                           as_policy_operate_to_pyobject(&policies->operate));
    py_dict_set_dict_value(py_dict, "info",
                           as_policy_info_to_pyobject(&policies->info));
    py_dict_set_dict_value(py_dict, "admin",
                           as_policy_admin_to_pyobject(&policies->admin));
    py_dict_set_dict_value(
        py_dict, "batch_apply",
        as_policy_batch_apply_to_pyobject(&policies->batch_apply));
    py_dict_set_dict_value(
        py_dict, "batch_remove",
        as_policy_batch_remove_to_pyobject(&policies->batch_remove));
    py_dict_set_dict_value(
        py_dict, "batch_write",
        as_policy_batch_write_to_pyobject(&policies->batch_write));
    py_dict_set_dict_value(py_dict, "batch",
                           as_policy_batch_to_pyobject(&policies->batch));
    py_dict_set_dict_value(
        py_dict, "batch_parent_write",
        as_policy_batch_to_pyobject(&policies->batch_parent_write));
    py_dict_set_dict_value(py_dict, "txn_verify",
                           as_policy_batch_to_pyobject(&policies->txn_verify));
    py_dict_set_dict_value(py_dict, "txn_roll",
                           as_policy_batch_to_pyobject(&policies->txn_roll));
    return py_dict;
}

PyObject *AerospikeClient_Get_Policies(AerospikeClient *self, PyObject *args,
                                       PyObject *kwds)
{
    // Read from the live config, not a cached copy, so this reflects any
    // dynamic config updates applied after client construction.
    as_config *config = aerospike_load_config(self->as);
    return as_policies_to_pyobject(&config->policies);
}
