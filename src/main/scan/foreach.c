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

#include <aerospike/aerospike_scan.h>
#include <aerospike/as_error.h>
#include <aerospike/as_scan.h>
#include <aerospike/as_partition.h>

#include "client.h"
#include "conversions.h"
#include "exceptions.h"
#include "scan.h"
#include "policy.h"
#include "foreach.h"

extern bool each_result(const as_val *val, void *udata);

PyObject *AerospikeScan_Foreach(AerospikeScan *self, PyObject *args,
                                PyObject *kwds)
{
    // Python Function Keyword Arguments
    static char *kwlist[] = {"callback", "policy", "options", "nodename", NULL};

    // Python Function Arguments
    PyObject *py_callback = NULL;
    PyObject *py_policy = NULL;
    PyObject *py_options = NULL;
    PyObject *py_nodename = NULL;

    // Python Function Argument Parsing
    if (PyArg_ParseTupleAndKeywords(args, kwds, "O|OOO:foreach", kwlist,
                                    &py_callback, &py_policy, &py_options,
                                    &py_nodename) == false) {
        return NULL;
    }

    return AerospikeScan_Foreach_Invoke(self, py_callback, py_policy,
                                        py_options, py_nodename);
}

PyObject *AerospikeScan_Foreach_Invoke(AerospikeScan *self,
                                       PyObject *py_callback,
                                       PyObject *py_policy,
                                       PyObject *py_options,
                                       PyObject *py_nodename)
{
    char *nodename = NULL;

    as_policy_scan scan_policy;
    as_policy_scan *scan_policy_p = NULL;

    // For converting expressions.
    as_exp *exp_list_p = NULL;

    as_partition_filter partition_filter = {0};
    as_partition_filter *partition_filter_p = NULL;
    as_partitions_status *ps = NULL;

    // Create and initialize callback user-data
    LocalData data;
    bool is_scan_results = py_callback == NULL;
    if (is_scan_results) {
        data.py_obj = PyList_New(0);
        if (data.py_obj == NULL) {
            goto CLEANUP;
        }
    }
    else {
        data.py_obj = py_callback;
    }
    data.client = self->client;
    data.partition_query = 0;

    as_error err;
    as_error_init(&err);

    // Stores errors reported by individual threads when they call the each_result callback
    as_vector_init(&data.thread_errors, sizeof(as_error *), 16);
    pthread_mutex_init(&data.thread_errors_mutex, NULL);

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
    pyobject_to_policy_scan(
        self->client, &err, py_policy, &scan_policy, &scan_policy_p,
        &self->client->as->config.policies.scan, &exp_list_p, false);

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
            }
            data.partition_query = 1;
        }
    }
    if (err.code != AEROSPIKE_OK) {
        goto CLEANUP;
    }

    if (py_options && PyDict_Check(py_options)) {
        set_scan_options(&err, &self->scan, py_options);
        if (err.code != AEROSPIKE_OK) {
            goto CLEANUP;
        }
    }

    if (py_nodename) {
        if (PyUnicode_Check(py_nodename)) {
            nodename = (char *)PyUnicode_AsUTF8(py_nodename);
        }
        else {
            as_error_update(&err, AEROSPIKE_ERR_PARAM,
                            "nodename must be a string");
            goto CLEANUP;
        }
    }

    // We are spawning multiple threads
    Py_BEGIN_ALLOW_THREADS
    // Invoke operation
    if (partition_filter_p) {
        if (ps) {
            as_partition_filter_set_partitions(partition_filter_p, ps);
        }
        aerospike_scan_partitions(self->client->as, &err, scan_policy_p,
                                  &self->scan, partition_filter_p, each_result,
                                  &data);
        if (ps) {
            as_partitions_status_release(ps);
        }
    }
    else if (nodename) {
        aerospike_scan_node(self->client->as, &err, scan_policy_p, &self->scan,
                            nodename, each_result, &data);
    }
    else {
        aerospike_scan_foreach(self->client->as, &err, scan_policy_p,
                               &self->scan, each_result, &data);
    }
    // We are done using multiple threads
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

    if (err.code != AEROSPIKE_OK) {
        if (is_scan_results) {
            // Clear list from results()
            Py_DECREF(data.py_obj);
        }
        raise_exception(&err);
        return NULL;
    }

    if (is_scan_results) {
        return data.py_obj;
    }
    else {
        Py_INCREF(Py_None);
        return Py_None;
    }
}
