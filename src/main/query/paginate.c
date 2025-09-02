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
#include <stdbool.h>

#include <aerospike/as_error.h>

#include "client.h"
#include "exceptions.h"
#include "query.h"

PyObject *AerospikeQuery_Paginate(AerospikeQuery *self)
{
    PyObject *py_value = NULL;
    as_error err;
    as_error_init(&err);

    if (!self || !self->client->as) {
        as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid query object.");
        goto CLEANUP;
    }

    if (!self->client->is_conn_16) {
        as_error_update(&err, AEROSPIKE_ERR_CLUSTER,
                        "No connection to aerospike cluster.");
        goto CLEANUP;
    }

    as_query_set_paginate(&self->query, true);

    py_value = PyBool_FromLong(true);

CLEANUP:
    if (err.code != AEROSPIKE_OK) {
        raise_exception(&err);
        return NULL;
    }

    return py_value;
}

PyObject *AerospikeQuery_Is_Done(AerospikeQuery *self)
{
    PyObject *py_value = NULL;
    as_error err;
    as_error_init(&err);
    bool query_done = 0;

    if (!self || !self->client->as) {
        as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid query object.");
        goto CLEANUP;
    }

    if (!self->client->is_conn_16) {
        as_error_update(&err, AEROSPIKE_ERR_CLUSTER,
                        "No connection to aerospike cluster.");
        goto CLEANUP;
    }

    query_done = as_query_is_done(&self->query);
    py_value = PyBool_FromLong(query_done);

CLEANUP:
    if (err.code != AEROSPIKE_OK) {
        raise_exception(&err);
        return NULL;
    }

    return py_value;
}
