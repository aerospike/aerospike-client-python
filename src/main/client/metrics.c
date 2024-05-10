/*******************************************************************************
 * Copyright 2013-2024 Aerospike, Inc.
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

#include <aerospike/as_metrics.h>

#include "metrics.h"
#include "conversions.h"
#include "policy.h"

PyObject *AerospikeClient_EnableMetrics(AerospikeClient *self, PyObject *args,
                                        PyObject *kwds)
{
    as_error err;
    as_error_init(&err);

    PyObject *py_metrics_policy = NULL;
    as_metrics_policy metrics_policy;

    // Python Function Keyword Arguments
    static char *kwlist[] = {"policy", NULL};

    // Python Function Argument Parsing
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "|O:enable_metrics", kwlist,
                                     &py_metrics_policy)) {
        return NULL;
    }

    as_status status = init_and_set_as_metrics_policy_using_pyobject(
        &err, py_metrics_policy, &metrics_policy);
    if (status != AEROSPIKE_OK) {
        goto error;
    }

    Py_BEGIN_ALLOW_THREADS
    aerospike_enable_metrics(self->as, &err, &metrics_policy);
    Py_END_ALLOW_THREADS

    if (err.code != AEROSPIKE_OK) {
        free_py_listener_data(
            (PyListenerData *)metrics_policy.metrics_listeners.udata);
        goto error;
    }

    Py_INCREF(Py_None);
    return Py_None;

error:
    raise_exception(&err);
    return NULL;
}

PyObject *AerospikeClient_DisableMetrics(AerospikeClient *self, PyObject *args)
{
    as_error err;
    as_error_init(&err);

    Py_BEGIN_ALLOW_THREADS
    aerospike_disable_metrics(self->as, &err);
    Py_END_ALLOW_THREADS

    if (err.code != AEROSPIKE_OK) {
        goto error;
    }

    Py_INCREF(Py_None);
    return Py_None;

error:
    raise_exception(&err);
    return NULL;
}
