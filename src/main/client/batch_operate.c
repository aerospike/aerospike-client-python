/*******************************************************************************
 * Copyright 2013-2022 Aerospike, Inc.
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

#include <stdint.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

#include <aerospike/aerospike_key.h>
#include <aerospike/as_key.h>
#include <aerospike/as_error.h>
#include <aerospike/as_record.h>
#include <aerospike/as_operations.h>
#include <aerospike/as_log_macros.h>

#include "client.h"
#include "conversions.h"
#include "exceptions.h"
#include "policy.h"

// Struct for Python User-Data for the Callback
typedef struct {
    PyObject *py_results;
    PyObject *batch_records_module;
    PyObject *func_name;
    AerospikeClient *client;
} LocalData;

static bool batch_operate_cb(const as_batch_result *results, uint32_t n,
                             void *udata)
{
    // Extract callback user-data
    LocalData *data = (LocalData *)udata;
    as_error err;
    as_error_init(&err);
    PyObject *py_key = NULL;
    PyObject *py_batch_record = NULL;
    bool success = true;

    // Lock Python State
    PyGILState_STATE gstate;
    gstate = PyGILState_Ensure();

    for (uint32_t i = 0; i < n; i++) {

        as_batch_read *res = NULL;
        res = (as_batch_read *)&results[i];

        // NOTE these conversions shouldn't go wrong but if they do, return
        if (key_to_pyobject(&err, res->key, &py_key) != AEROSPIKE_OK) {
            as_log_error("unable to convert res->key at results index: %d", i);
            success = false;
            break;
        }

        py_batch_record = PyObject_CallMethodObjArgs(
            data->batch_records_module, data->func_name, py_key, NULL);
        if (py_batch_record == NULL) {
            as_log_error("unable to instance BatchRecord at results index: %d",
                         i);
            success = false;
            Py_DECREF(py_key);
            break;
        }
        Py_DECREF(py_key);

        as_batch_result_to_BatchRecord(data->client, &err, res, py_batch_record,
                                       false);
        if (err.code != AEROSPIKE_OK) {
            as_log_error(
                "as_batch_result_to_BatchRecord failed at results index: %d",
                i);
            success = false;
            break;
        }

        PyList_Append(data->py_results, py_batch_record);
        Py_DECREF(py_batch_record);
    }

    PyGILState_Release(gstate);
    return success;
}

/**
 *******************************************************************************************************
 * This function invokes csdk's API's.
 *
 * @param self                      AerospikeClient object
 * @param err                       The as_error to be populated by the function
 *                                  with the encountered error if any.
 * @param py_keys                   The list containing keys.
 * @param py_ops                    The list containing op dictionaries.
 * @param py_policy_batch      		Python dict used to populate policy_batch.
 * @param py_policy_batch_write     Python dict used to populate policy_batch_write.
 *******************************************************************************************************
 */
static PyObject *AerospikeClient_Batch_Operate_Invoke(
    AerospikeClient *self, as_error *err, PyObject *py_keys, PyObject *py_ops,
    PyObject *py_policy_batch, PyObject *py_policy_batch_write)
{
    long operation;
    long return_type = -1;

    as_policy_batch policy_batch;
    as_policy_batch *policy_batch_p = NULL;

    as_policy_batch_write policy_batch_write;
    as_policy_batch_write *policy_batch_write_p = NULL;

    as_batch batch;
    as_batch_init(&batch, 0);

    // For expressions conversion.
    as_exp batch_exp_list;
    as_exp *batch_exp_list_p = NULL;

    as_exp batch_write_exp_list;
    as_exp *batch_write_exp_list_p = NULL;

    as_vector *unicodeStrVector = as_vector_create(sizeof(char *), 128);

    as_static_pool static_pool;
    memset(&static_pool, 0, sizeof(static_pool));

    as_vector *tmp_keys_p = NULL;

    as_operations ops;

    Py_ssize_t ops_size = PyList_Size(py_ops);
    as_operations_inita(&ops, ops_size);

    PyObject *br_instance = NULL;

    if (!self || !self->as) {
        as_error_update(err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
        goto CLEANUP;
    }

    if (!self->is_conn_16) {
        as_error_update(err, AEROSPIKE_ERR_CLUSTER,
                        "No connection to aerospike cluster");
        goto CLEANUP;
    }

    for (int i = 0; i < ops_size; i++) {
        PyObject *py_val = PyList_GetItem(py_ops, i);

        if (!PyDict_Check(py_val)) {
            as_error_update(err, AEROSPIKE_ERR_PARAM,
                            "op should be an aerospike operation dictionary");
            goto CLEANUP;
        }

        if (add_op(self, err, py_val, unicodeStrVector, &static_pool, &ops,
                   &operation, &return_type) != AEROSPIKE_OK) {
            goto CLEANUP;
        }
    }

    Py_ssize_t keys_size = PyList_Size(py_keys);

    as_vector tmp_keys;
    as_vector_init(&tmp_keys, sizeof(as_key), keys_size);
    tmp_keys_p = &tmp_keys;
    uint64_t processed_key_count = 0;

    for (int i = 0; i < keys_size; i++) {
        PyObject *py_key = PyList_GetItem(py_keys, i);
        as_key *tmp_key = (as_key *)as_vector_get(&tmp_keys, i);

        if (!PyTuple_Check(py_key)) {
            as_error_update(err, AEROSPIKE_ERR_PARAM,
                            "key should be an aerospike key tuple");
            goto CLEANUP;
        }

        pyobject_to_key(err, py_key, tmp_key);
        if (err->code != AEROSPIKE_OK) {
            as_error_update(err, AEROSPIKE_ERR_PARAM,
                            "failed to convert key at index: %d", i);
            goto CLEANUP;
        }

        processed_key_count++;
    }

    as_batch_init(&batch, processed_key_count);
    memcpy(batch.keys.entries, tmp_keys.list,
           sizeof(as_key) * processed_key_count);

    if (py_policy_batch) {
        if (pyobject_to_policy_batch(
                self, err, py_policy_batch, &policy_batch, &policy_batch_p,
                &self->as->config.policies.batch, &batch_exp_list,
                &batch_exp_list_p) != AEROSPIKE_OK) {
            goto CLEANUP;
        }
    }

    if (py_policy_batch_write) {
        if (pyobject_to_batch_write_policy(
                self, err, py_policy_batch_write, &policy_batch_write,
                &policy_batch_write_p, &batch_write_exp_list,
                &batch_write_exp_list_p) != AEROSPIKE_OK) {
            goto CLEANUP;
        }

        // The C client's batch write policy doesn't have a ttl option
        // The correct way is to set the ttl inside the as_operations object
        PyObject *py_ttl = PyDict_GetItemString(py_policy_batch_write, "ttl");
        Py_XINCREF(py_ttl);
        // Default ttl
        if (py_ttl != NULL) {
            if (PyLong_Check(py_ttl)) {
                long ttl = PyLong_AsLong(py_ttl);
                if (ttl > UINT32_MAX || ttl < 0) {
                    as_error_update(err, AEROSPIKE_ERR_PARAM,
                                    "ttl is out of range. It must be a 32 bit "
                                    "unsigned integer.");
                    Py_DECREF(py_ttl);
                    goto CLEANUP;
                }
                ops.ttl = ttl;
            }
        }
        Py_XDECREF(py_ttl);
    }

    // import batch_records helper
    PyObject *br_module = NULL;
    PyObject *sys_modules = PyImport_GetModuleDict();

    if (PyMapping_HasKeyString(sys_modules,
                               "aerospike_helpers.batch.records")) {
        br_module = PyMapping_GetItemString(sys_modules,
                                            "aerospike_helpers.batch.records");
    }
    else {
        br_module = PyImport_ImportModule("aerospike_helpers.batch.records");
    }

    if (!br_module) {
        as_error_update(err, AEROSPIKE_ERR_CLIENT,
                        "Unable to load batch_records module");
        goto CLEANUP;
    }

    PyObject *obj_name = PyUnicode_FromString("BatchRecords");
    PyObject *res_list = PyList_New(0);
    br_instance =
        PyObject_CallMethodObjArgs(br_module, obj_name, res_list, NULL);
    if (!br_instance) {
        as_error_update(err, AEROSPIKE_ERR_CLIENT,
                        "Unable to instance BatchRecords");
        Py_DECREF(br_module);
        Py_DECREF(obj_name);
        Py_DECREF(res_list);
        goto CLEANUP;
    }
    Py_DECREF(br_module);
    Py_DECREF(obj_name);
    Py_DECREF(res_list);

    // Create and initialize callback user-data
    LocalData data;
    data.client = self;
    data.func_name = PyUnicode_FromString("BatchRecord");
    data.py_results = PyObject_GetAttrString(br_instance, "batch_records");
    data.batch_records_module = br_module;

    as_error batch_apply_err;
    as_error_init(&batch_apply_err);

    Py_BEGIN_ALLOW_THREADS

    aerospike_batch_operate(self->as, &batch_apply_err, policy_batch_p,
                            policy_batch_write_p, &batch, &ops,
                            batch_operate_cb, &data);

    Py_END_ALLOW_THREADS

    Py_DECREF(data.py_results);
    Py_DECREF(data.func_name);

    PyObject *py_bw_res = PyLong_FromLong((long)batch_apply_err.code);
    PyObject_SetAttrString(br_instance, FIELD_NAME_BATCH_RESULT, py_bw_res);
    Py_DECREF(py_bw_res);

    as_error_reset(err);

CLEANUP:
    for (unsigned int i = 0; i < unicodeStrVector->size; i++) {
        free(as_vector_get_ptr(unicodeStrVector, i));
    }

    if (batch_exp_list_p) {
        as_exp_destroy(batch_exp_list_p);
    }

    if (batch_write_exp_list_p) {
        as_exp_destroy(batch_write_exp_list_p);
    }

    as_vector_destroy(unicodeStrVector);
    as_operations_destroy(&ops);
    as_batch_destroy(&batch);

    if (tmp_keys_p) {
        as_vector_destroy(tmp_keys_p);
    }

    if (err->code != AEROSPIKE_OK) {
        raise_exception(err);
        return NULL;
    }

    return br_instance;
}

/**
 *******************************************************************************************************
 * Same operations on a multiple records
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns aerospike_helpers.batch.records.BatchRecords object on success.
 * Otherwise returns 0 on success for other operations.
 *******************************************************************************************************
 */
PyObject *AerospikeClient_Batch_Operate(AerospikeClient *self, PyObject *args,
                                        PyObject *kwds)
{
    as_error err;
    PyObject *py_policy_batch = NULL;
    PyObject *py_policy_batch_write = NULL;
    PyObject *py_keys = NULL;
    PyObject *py_ops = NULL;
    PyObject *py_results = NULL;

    as_error_init(&err);

    // Python Function Keyword Arguments
    static char *kwlist[] = {"keys", "ops", "policy_batch",
                             "policy_batch_write", NULL};
    if (PyArg_ParseTupleAndKeywords(args, kwds, "OO|OO:batch_Operate", kwlist,
                                    &py_keys, &py_ops, &py_policy_batch,
                                    &py_policy_batch_write) == false) {
        return NULL;
    }

    // required arg so don't need to check for NULL
    if (!PyList_Check(py_ops) || !PyList_Size(py_ops)) {
        as_error_update(&err, AEROSPIKE_ERR_PARAM,
                        "ops should be a list of op dictionaries");
        goto ERROR;
    }

    // required arg so don't need to check for NULL
    if (!PyList_Check(py_keys)) {
        as_error_update(&err, AEROSPIKE_ERR_PARAM,
                        "keys should be a list of aerospike key tuples");
        goto ERROR;
    }

    py_results = AerospikeClient_Batch_Operate_Invoke(
        self, &err, py_keys, py_ops, py_policy_batch, py_policy_batch_write);

    return py_results;

ERROR:

    if (err.code != AEROSPIKE_OK) {
        raise_exception(&err);
    }

    return NULL;
}
