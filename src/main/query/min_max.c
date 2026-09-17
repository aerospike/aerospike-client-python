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
#include <stdbool.h>

#include <aerospike/aerospike_query.h>
#include <aerospike/as_error.h>
#include <aerospike/as_query.h>
#include <aerospike/as_val.h>

#include "client.h"
#include "conversions.h"
#include "exceptions.h"
#include "policy.h"
#include "query.h"

/**
 * Shared implementation for Query.min()/Query.max(). Both are thin wrappers
 * around the vendored C client's aerospike_query_min()/aerospike_query_max(),
 * which build on the same order_by()/top_k(1) mechanism as a regular Top-K
 * query (see order_by.c) - including mutating this query's select/order_by/
 * top_k fields as a side effect, and inheriting all of order_by()'s deferred
 * (server/C-client-side) validation.
 */
static PyObject *query_min_or_max(AerospikeQuery *self, PyObject *args,
                                  PyObject *kwds, bool find_max)
{
    char *bin_name = NULL;
    unsigned int type = 0;
    PyObject *py_policy = NULL;

    static char *kwlist[] = {"bin", "type", "policy", NULL};

    as_error err;
    as_error_init(&err);

    as_policy_query query_policy;
    as_policy_query *query_policy_p = NULL;
    as_exp *exp_list_p = NULL;
    as_val *value = NULL;
    PyObject *py_value = NULL;

    if (!self || !self->client || !self->client->as) {
        as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
        goto CLEANUP;
    }

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "sI|O:min/max", kwlist,
                                     &bin_name, &type, &py_policy)) {
        as_error_update(
            &err, AEROSPIKE_ERR_PARAM,
            "min()/max() expects (bin: str, type: int, policy: dict = None)");
        goto CLEANUP;
    }

    if (!self->client->is_conn_16) {
        as_error_update(&err, AEROSPIKE_ERR_CLUSTER,
                        "No connection to aerospike cluster");
        goto CLEANUP;
    }

    pyobject_to_policy_query(
        self->client, &err, py_policy, &query_policy, &query_policy_p,
        &self->client->as->config.policies.query, &exp_list_p);
    if (err.code != AEROSPIKE_OK) {
        goto CLEANUP;
    }

    Py_BEGIN_ALLOW_THREADS

    if (find_max) {
        aerospike_query_max(self->client->as, &err, query_policy_p,
                            &self->query, bin_name,
                            (as_query_order_by_type)type, &value);
    }
    else {
        aerospike_query_min(self->client->as, &err, query_policy_p,
                            &self->query, bin_name,
                            (as_query_order_by_type)type, &value);
    }

    Py_END_ALLOW_THREADS

    if (err.code != AEROSPIKE_OK) {
        goto CLEANUP;
    }

    if (value) {
        val_to_pyobject(self->client, &err, value, &py_value);
        as_val_destroy(value);
        if (err.code != AEROSPIKE_OK) {
            goto CLEANUP;
        }
    }
    else {
        Py_INCREF(Py_None);
        py_value = Py_None;
    }

CLEANUP:
    if (exp_list_p) {
        as_exp_destroy(exp_list_p);
    }

    if (err.code != AEROSPIKE_OK) {
        Py_XDECREF(py_value);
        raise_exception(&err);
        return NULL;
    }

    return py_value;
}

PyObject *AerospikeQuery_Min(AerospikeQuery *self, PyObject *args,
                             PyObject *kwds)
{
    return query_min_or_max(self, args, kwds, false);
}

PyObject *AerospikeQuery_Max(AerospikeQuery *self, PyObject *args,
                             PyObject *kwds)
{
    return query_min_or_max(self, args, kwds, true);
}
