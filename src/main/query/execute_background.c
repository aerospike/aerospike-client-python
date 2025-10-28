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
#include <aerospike/as_scan.h>

#include "client.h"
#include "conversions.h"
#include "exceptions.h"
#include "policy.h"
#include "query.h"

PyObject *AerospikeQuery_ExecuteBackground(AerospikeQuery *self, PyObject *args,
                                           PyObject *kwds)
{
    PyObject *py_policy = NULL;

    as_policy_write write_policy;
    as_policy_write *write_policy_p = NULL;

    uint64_t query_id = 0;

    static char *kwlist[] = {"policy", NULL};

    if (PyArg_ParseTupleAndKeywords(args, kwds, "|O:execute_background", kwlist,
                                    &py_policy) == false) {
        return NULL;
    }

    if (py_policy == Py_None) {
        py_policy = NULL;
    }

    as_error err;
    as_error_init(&err);

    if (!self || !self->client->as) {
        as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
        goto CLEANUP;
    }
    if (!self->client->is_conn_16) {
        as_error_update(&err, AEROSPIKE_ERR_CLUSTER,
                        "No connection to aerospike cluster");
        goto CLEANUP;
    }

    if (py_policy) {
        as_policy_write_copy_and_set_from_pyobject(
            self->client, &err, py_policy, &write_policy,
            &self->client->as->config.policies.write,
            self->client->validate_keys);
        if (err.code != AEROSPIKE_OK) {
            goto CLEANUP;
        }
        write_policy_p = &write_policy;
    }

    Py_BEGIN_ALLOW_THREADS
    aerospike_query_background(self->client->as, &err, write_policy_p,
                               &self->query, &query_id);
    Py_END_ALLOW_THREADS

CLEANUP:

    if (write_policy_p) {
        as_exp_destroy(write_policy_p->base.filter_exp);
        ;
    }

    if (err.code != AEROSPIKE_OK) {
        raise_exception(&err);
        return NULL;
    }

    return PyLong_FromUnsignedLongLong(query_id);
}
