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
        goto RAISE_EXCEPTION_WITHOUT_AS_ERROR;
    }

    as_status status = init_and_set_as_metrics_policy_using_pyobject(
        &err, py_metrics_policy, &metrics_policy);
    if (status != AEROSPIKE_OK) {
        goto RAISE_EXCEPTION_USING_AS_ERROR;
    }

    // 2 scenarios:
    // 1. If the user does not pass their own MetricsListeners object to client.enable_metrics(), udata is NULL
    // 2. Otherwise, udata is non-NULL and set to heap-allocated PyListenerData
    bool free_udata_as_py_listener_data =
        metrics_policy.metrics_listeners.udata != NULL;

    Py_BEGIN_ALLOW_THREADS
    aerospike_enable_metrics(self->as, &err, &metrics_policy);
    Py_END_ALLOW_THREADS

    if (err.code != AEROSPIKE_OK) {
        // In the above scenario #1, when udata is NULL before aerospike_enable_metrics() is called:
        // It is possible for aerospike_enable_metrics() -> as_cluster_enable_metrics() -> as_metrics_writer_create()
        // to fail before it assigns a heap allocated value to metrics_policy.metrics_listeners.udata while setting up
        // the metrics writer.
        // In that case, we don't want to free udata where udata is NULL
        if (free_udata_as_py_listener_data) {
            free_py_listener_data(
                (PyListenerData *)metrics_policy.metrics_listeners.udata);
        }
        goto RAISE_EXCEPTION_USING_AS_ERROR;
    }

    Py_INCREF(Py_None);
    return Py_None;

RAISE_EXCEPTION_USING_AS_ERROR:
    raise_exception(&err);
RAISE_EXCEPTION_WITHOUT_AS_ERROR:
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
