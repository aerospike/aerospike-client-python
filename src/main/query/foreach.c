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
#include <pthread.h>

#include <aerospike/aerospike_scan.h>
#include <aerospike/as_error.h>
#include <aerospike/as_query.h>
#include <aerospike/as_partition.h>
#include <aerospike/as_arraylist.h>
#include <aerospike/as_vector.h>
#include <aerospike/aerospike_query.h>

#include "client.h"
#include "conversions.h"
#include "exceptions.h"
#include "query.h"
#include "policy.h"

// Struct for Python User-Data for the Callback
typedef struct {
    PyObject *callback;
    AerospikeClient *client;
    int partition_query;
    as_vector thread_errors;
    pthread_mutex_t thread_errors_mutex;
} LocalData;

static bool each_result(const as_val *val, void *udata)
{
    bool retval = true;

    if (!val) {
        return false;
    }

    // Extract callback user-data
    LocalData *data = (LocalData *)udata;
    PyObject *py_callback = data->callback;

    // Python Function Arguments and Result Value
    PyObject *py_arglist = NULL;
    PyObject *py_result = NULL;
    PyObject *py_return = NULL;

    // Lock Python State
    PyGILState_STATE gstate;
    gstate = PyGILState_Ensure();

    // Convert as_val to a Python Object
    // Use local thread error so we don't need to pass the main error to the callback
    // We want to avoid resetting the main error in case it was already set by another thread.
    as_error thread_err_local;
    as_error_init(&thread_err_local);
    val_to_pyobject(data->client, &thread_err_local, val, &py_result);

    if (thread_err_local.code != AEROSPIKE_OK) {
        goto EXIT_CALLBACK;
    }

    // Build Python Function Arguments
    if (data->partition_query) {

        uint32_t part_id = 0;

        as_record *rec = as_record_fromval(val);

        if (rec->key.digest.init) {
            part_id =
                as_partition_getid(rec->key.digest.value, CLUSTER_NPARTITIONS);
        }

        py_arglist = PyTuple_New(2);

        PyTuple_SetItem(py_arglist, 0, PyLong_FromUnsignedLong(part_id));
        PyTuple_SetItem(py_arglist, 1, py_result);
    }
    else {
        py_arglist = PyTuple_New(1);
        PyTuple_SetItem(py_arglist, 0, py_result);
    }

    // Invoke Python Callback
    py_return = PyObject_Call(py_callback, py_arglist, NULL);

    // Release Python Function Arguments
    Py_DECREF(py_arglist);
    // handle return value
    if (!py_return) {
        // an exception was raised, handle it (someday)
        // for now, we bail from the loop
        as_error_update(&thread_err_local, AEROSPIKE_ERR_CLIENT,
                        "Callback function contains an error");
        retval = false;
    }
    else if (py_return == Py_False) {
        retval = false;
    }
    Py_XDECREF(py_return);

EXIT_CALLBACK:
    if (thread_err_local.code != AEROSPIKE_OK) {
        pthread_mutex_lock(&data->thread_errors_mutex);
        as_error *stored_err_ref = (as_error *)cf_malloc(sizeof(as_error));
        as_error_copy(stored_err_ref, &thread_err_local);
        as_vector_append(&data->thread_errors, &stored_err_ref);
        pthread_mutex_unlock(&data->thread_errors_mutex);

        retval = false;
    }

    // Release Python State
    PyGILState_Release(gstate);

    return retval;
}

PyObject *AerospikeQuery_Foreach(AerospikeQuery *self, PyObject *args,
                                 PyObject *kwds)
{
    // Python Function Arguments
    PyObject *py_callback = NULL;
    PyObject *py_policy = NULL;
    PyObject *py_options = NULL;
    // Python Function Keyword Arguments
    static char *kwlist[] = {"callback", "policy", "options", NULL};

    // Python Function Argument Parsing
    if (PyArg_ParseTupleAndKeywords(args, kwds, "O|OO:foreach", kwlist,
                                    &py_callback, &py_policy,
                                    &py_options) == false) {
        as_query_destroy(&self->query);
        return NULL;
    }

    // Initialize callback user data
    LocalData data;
    data.callback = py_callback;
    data.client = self->client;
    data.partition_query = 0;

    // Main error
    as_error err;
    as_error_init(&err);
    // Stores errors reported by individual threads when they call the each_result callback
    as_vector_init(&data.thread_errors, sizeof(as_error *), 16);
    pthread_mutex_init(&data.thread_errors_mutex, NULL);

    // Aerospike Client Arguments
    as_policy_query query_policy;
    as_policy_query *query_policy_p = NULL;

    // For converting expressions.
    as_exp exp_list;
    as_exp *exp_list_p = NULL;

    as_partition_filter partition_filter = {0};
    as_partition_filter *partition_filter_p = NULL;
    as_partitions_status *ps = NULL;

    // Initialize error

    if (!self || !self->client->as) {
        as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
        goto CLEANUP;
    }

    if (!self->client->is_conn_16) {
        as_error_update(&err, AEROSPIKE_ERR_CLUSTER,
                        "No connection to aerospike cluster");
        goto CLEANUP;
    }

    // Convert python policy object to as_policy_exists
    pyobject_to_policy_query(
        self->client, &err, py_policy, &query_policy, &query_policy_p,
        &self->client->as->config.policies.query, &exp_list, &exp_list_p);
    if (err.code != AEROSPIKE_OK) {
        goto CLEANUP;
    }

    if (py_policy) {
        PyObject *py_partition_filter =
            PyDict_GetItemString(py_policy, "partition_filter");
        if (py_partition_filter) {
            if (convert_partition_filter(self->client, py_partition_filter,
                                         &partition_filter, &ps,
                                         &err) == AEROSPIKE_OK) {
                partition_filter_p = &partition_filter;
                data.partition_query = 1;
            }
            else {
                goto CLEANUP;
            }
        }
    }

    if (set_query_options(&err, py_options, &self->query) != AEROSPIKE_OK) {
        goto CLEANUP;
    }

    Py_BEGIN_ALLOW_THREADS

    // Invoke operation
    if (partition_filter_p) {
        if (ps) {
            as_partition_filter_set_partitions(partition_filter_p, ps);
        }

        aerospike_query_partitions(self->client->as, &err, query_policy_p,
                                   &self->query, partition_filter_p,
                                   each_result, &data);

        if (ps) {
            as_partitions_status_release(ps);
        }
    }
    else {
        aerospike_query_foreach(self->client->as, &err, query_policy_p,
                                &self->query, each_result, &data);
    }

    Py_END_ALLOW_THREADS

    // Promote any thread-level error if the main error was not set
    if (err.code == AEROSPIKE_OK && data.thread_errors.size > 0) {
        as_error *vector_item =
            (as_error *)as_vector_get_ptr(&data.thread_errors, 0);
        as_error_copy(&err, vector_item);
    }

CLEANUP:
    if (exp_list_p) {
        as_exp_destroy(exp_list_p);
    }

    if (self->query.apply.arglist) {
        as_arraylist_destroy((as_arraylist *)self->query.apply.arglist);
    }
    self->query.apply.arglist = NULL;

    for (uint32_t i = 0; i < data.thread_errors.size; ++i) {
        void *err_ptr = as_vector_get_ptr(&data.thread_errors, i);
        cf_free(err_ptr);
    }
    as_vector_destroy(&data.thread_errors);
    pthread_mutex_destroy(&data.thread_errors_mutex);

    if (err.code != AEROSPIKE_OK) {
        raise_exception_base(&err, Py_None, Py_None, Py_None, Py_None, Py_None);
        return NULL;
    }

    Py_INCREF(Py_None);
    return Py_None;
}
