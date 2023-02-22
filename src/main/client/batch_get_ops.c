/*******************************************************************************
 * Copyright 2013-2021 Aerospike, Inc.
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
#include <stdlib.h>
#include <string.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/as_key.h>
#include <aerospike/as_error.h>
#include <aerospike/as_record.h>
#include <aerospike/as_operations.h>
#include <aerospike/as_map_operations.h>
#include <aerospike/aerospike_info.h>
#include "client.h"
#include "conversions.h"
#include "exceptions.h"
#include "policy.h"

#include <aerospike/as_double.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_geojson.h>
#include <aerospike/as_nil.h>

extern as_status add_op(AerospikeClient *self, as_error *err, PyObject *py_val,
                        as_vector *unicodeStrVector,
                        as_static_pool *static_pool, as_operations *ops,
                        long *op, long *ret_type);

// Struct for Python User-Data for the Callback
typedef struct {
    as_error error;
    PyObject *py_results;
    AerospikeClient *client;
    PyObject *py_keys;
} LocalData;

static bool batch_read_operate_cb(const as_batch_read *results, uint32_t n,
                                  void *udata)
{
    // Extract callback user-data
    LocalData *data = (LocalData *)udata;
    as_batch_read *r = NULL;

    // Lock Python State
    PyGILState_STATE gstate;
    gstate = PyGILState_Ensure();

    for (uint32_t i = 0; i < n; i++) {
        PyObject *py_key = NULL;
        PyObject *py_rec = NULL;
        PyObject *py_rec_meta = NULL;
        PyObject *py_rec_bins = NULL;
        as_record *rec = NULL;
        as_error err;

        r = (as_batch_read *)&results[i];
        py_key = PyList_GetItem(data->py_keys, i);
        // key_to_pyobject(&err, &r->key, &py_key);
        rec = &r->record;

        as_error_init(&err);
        err.code = r->result;

        if (err.code == AEROSPIKE_OK) {
            metadata_to_pyobject(&err, rec, &py_rec_meta);
            bins_to_pyobject(data->client, &err, rec, &py_rec_bins, false);
        }
        else {
            py_rec_meta = raise_exception_old(&err);
            Py_INCREF(Py_None);
            py_rec_bins = Py_None;
        }

        py_rec = PyTuple_New(3);
        PyTuple_SetItem(py_rec, 0, py_key);
        PyTuple_SetItem(py_rec, 1, py_rec_meta);
        PyTuple_SetItem(py_rec, 2, py_rec_bins);

        PyList_Append(data->py_results, py_rec);
    }

    PyGILState_Release(gstate);

    return true;
}

/**
 *******************************************************************************************************
 * This function invokes csdk's API's.
 *
 * @param self                  AerospikeClient object
 * @param err                   The as_error to be populated by the function
 *                              with the encountered error if any.
 * @param py_keys                   The list containing keys.
 * @param py_ops               The list containing op, bin and value.
 * @param py_policy      		Python dict used to populate the operate_policy or map_policy.
 *******************************************************************************************************
 */
static PyObject *AerospikeClient_Batch_GetOps_Invoke(AerospikeClient *self,
                                                     as_error *err,
                                                     PyObject *py_keys,
                                                     PyObject *py_ops,
                                                     PyObject *py_policy)
{
    long operation;
    long return_type = -1;
    as_policy_batch policy;
    as_policy_batch *batch_policy_p = NULL;
    PyObject *py_results = NULL;
    as_batch batch;

    as_batch_init(&batch, 0);

    // For expressions conversion.
    as_exp exp_list;
    as_exp *exp_list_p = NULL;

    as_vector *unicodeStrVector = as_vector_create(sizeof(char *), 128);

    as_operations ops;
    Py_ssize_t ops_size = PyList_Size(py_ops);
    as_operations_inita(&ops, ops_size);

    if (py_policy) {
        if (pyobject_to_policy_batch(self, err, py_policy, &policy,
                                     &batch_policy_p,
                                     &self->as->config.policies.batch,
                                     &exp_list, &exp_list_p) != AEROSPIKE_OK) {
            goto CLEANUP;
        }
    }

    as_static_pool static_pool;
    memset(&static_pool, 0, sizeof(static_pool));

    for (int i = 0; i < ops_size; i++) {
        PyObject *py_val = PyList_GetItem(py_ops, i);

        if (PyDict_Check(py_val)) {
            if (add_op(self, err, py_val, unicodeStrVector, &static_pool, &ops,
                       &operation, &return_type) != AEROSPIKE_OK) {
                goto CLEANUP;
            }
        }
    }
    if (err->code != AEROSPIKE_OK) {
        as_error_update(err, err->code, NULL);
        goto CLEANUP;
    }

    Py_ssize_t keys_size = PyList_Size(py_keys);
    as_batch_init(&batch, keys_size);

    for (int i = 0; i < keys_size; i++) {
        PyObject *py_key = PyList_GetItem(py_keys, i);
        if (!PyTuple_Check(py_key)) {
            as_error_update(err, AEROSPIKE_ERR_PARAM, "Key should be a tuple.");
            goto CLEANUP;
        }
        pyobject_to_key(err, py_key, as_batch_keyat(&batch, i));
        if (err->code != AEROSPIKE_OK) {
            as_error_update(err, AEROSPIKE_ERR_PARAM, "Key should be valid.");
            goto CLEANUP;
        }
    }

    // Create and initialize callback user-data
    LocalData data;
    data.client = self;
    py_results = PyList_New(0);
    data.py_results = py_results;
    data.py_keys = py_keys;

    as_error_init(&data.error);

    Py_BEGIN_ALLOW_THREADS
    aerospike_batch_get_ops(self->as, &data.error, batch_policy_p, &batch, &ops,
                            batch_read_operate_cb, &data);
    Py_END_ALLOW_THREADS

    as_error_copy(err, &data.error);

    if (err->code != AEROSPIKE_OK) {
        goto CLEANUP;
    }

CLEANUP:
    for (unsigned int i = 0; i < unicodeStrVector->size; i++) {
        free(as_vector_get_ptr(unicodeStrVector, i));
    }

    if (exp_list_p) {
        as_exp_destroy(exp_list_p);
    }

    as_vector_destroy(unicodeStrVector);

    as_operations_destroy(&ops);

    as_batch_destroy(&batch);

    return py_results;
}

/**
 *******************************************************************************************************
 * Multiple operations on a single record
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns tuple of bins on success if read operation is given.
 * Otherwise returns 0 on success for other operations.
 *******************************************************************************************************
 */
PyObject *AerospikeClient_Batch_GetOps(AerospikeClient *self, PyObject *args,
                                       PyObject *kwds)
{
    as_error err;
    PyObject *py_policy = NULL;
    PyObject *py_keys = NULL;
    PyObject *py_ops = NULL;
    PyObject *py_results = NULL;

    as_error_init(&err);

    // Python Function Keyword Arguments
    static char *kwlist[] = {"keys", "list", "policy", NULL};
    if (PyArg_ParseTupleAndKeywords(args, kwds, "OO|O:batch_getops", kwlist,
                                    &py_keys, &py_ops, &py_policy) == false) {
        return NULL;
    }

    if (!py_keys || !PyList_Check(py_keys) || !py_ops ||
        !PyList_Check(py_ops)) {
        as_error_update(&err, AEROSPIKE_ERR_PARAM,
                        "batch_getops keys/ops should be of type list");
    }

    py_results = AerospikeClient_Batch_GetOps_Invoke(self, &err, py_keys,
                                                     py_ops, py_policy);

    if (py_results == NULL) {
        raise_exception(&err);
    }

    return py_results;
}
