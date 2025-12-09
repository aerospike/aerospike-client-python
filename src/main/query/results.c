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
#include <pthread.h>
#include <stdbool.h>

#include <aerospike/aerospike_query.h>
#include <aerospike/as_error.h>
#include <aerospike/as_query.h>
#include <aerospike/as_arraylist.h>

#include "client.h"
#include "conversions.h"
#include "exceptions.h"
#include "query.h"
#include "policy.h"

#undef TRACE
#define TRACE()

typedef struct {
    PyObject *py_results;
    AerospikeClient *client;
} LocalData;

static bool each_result(const as_val *val, void *udata)
{
    if (!val) {
        return false;
    }

    PyObject *py_results = NULL;
    LocalData *data = (LocalData *)udata;
    py_results = data->py_results;
    PyObject *py_result = NULL;

    as_error err;

    PyGILState_STATE gstate;
    gstate = PyGILState_Ensure();

    val_to_pyobject(data->client, &err, val, &py_result);

    if (py_result) {
        PyList_Append(py_results, py_result);
        Py_DECREF(py_result);
    }
    PyGILState_Release(gstate);

    return true;
}

PyObject *AerospikeQuery_Results(AerospikeQuery *self, PyObject *args,
                                 PyObject *kwds)
{
    PyObject *py_policy = NULL;
    PyObject *py_results = NULL;
    PyObject *py_options = NULL;

    static char *kwlist[] = {"policy", "options", NULL};

    LocalData data;
    data.client = self->client;

    if (PyArg_ParseTupleAndKeywords(args, kwds, "|OO:results", kwlist,
                                    &py_policy, &py_options) == false) {
        return NULL;
    }

    as_error err;
    as_error_init(&err);

    as_policy_query query_policy;
    as_policy_query *query_policy_p = NULL;

    // For converting expressions.
    as_exp exp_list;
    as_exp *exp_list_p = NULL;

    as_partition_filter partition_filter = {0};
    as_partition_filter *partition_filter_p = NULL;
    as_partitions_status *ps = NULL;

    if (!self || !self->client->as) {
        as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
        goto CLEANUP;
    }

    if (!self->client->is_conn_16) {
        as_error_update(&err, AEROSPIKE_ERR_CLUSTER,
                        "No connection to aerospike cluster");
        goto CLEANUP;
    }

    // Convert python policy object to as_policy_query
    pyobject_to_policy_query(
        self->client, &err, py_policy, &query_policy, &query_policy_p,
        &self->client->as->config.policies.query, &exp_list, &exp_list_p);
    if (err.code != AEROSPIKE_OK) {
        goto CLEANUP;
    }

    if (set_query_options(&err, py_options, &self->query) != AEROSPIKE_OK) {
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
            else {
                goto CLEANUP;
            }
        }
    }
    as_error_reset(&err);

    py_results = PyList_New(0);
    data.py_results = py_results;

    Py_BEGIN_ALLOW_THREADS

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

CLEANUP: /*??trace()*/
    if (exp_list_p) {
        as_exp_destroy(exp_list_p);
    }

    if (err.code != AEROSPIKE_OK) {
        Py_XDECREF(py_results);
        raise_exception(&err);
        return NULL;
    }

    if (self->query.apply.arglist) {
        as_arraylist_destroy((as_arraylist *)self->query.apply.arglist);
    }
    self->query.apply.arglist = NULL;

    return py_results;
}
