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
#include <aerospike/as_log_macros.h>
#include <aerospike/as_metrics.h>
#include <aerospike/aerospike_stats.h>

#include "metrics.h"
#include "conversions.h"
#include "exceptions.h"
#include "policy.h"

// Extended metrics

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

    // To be passed into C client
    as_metrics_policy *metrics_policy_ref;
    // If enable_metrics() succeeds, our heap-allocated udata will be free'd later when metrics is disabled (like when client.close() is called)
    bool free_udata_as_py_listener_data = false;

    if (py_metrics_policy == NULL || py_metrics_policy == Py_None) {
        // Use C client's config metrics policy
        metrics_policy_ref = NULL;
    }
    else {
        // Set a transaction-level metrics policy
        as_metrics_policy_init(&metrics_policy);
        metrics_policy_ref = &metrics_policy;
        int retval = set_as_metrics_policy_using_pyobject(
            &err, py_metrics_policy, &metrics_policy);
        if (retval != 0) {
            goto CLEANUP_ON_ERROR;
        }
    }

    // 2 scenarios:
    // 1. If the user passes their own MetricsPolicy and MetricsListeners object to client.enable_metrics(), udata is NOT NULL and set to heap-allocated PyListenerData
    // 2. Otherwise, udata is NULL.
    free_udata_as_py_listener_data =
        metrics_policy_ref && metrics_policy.metrics_listeners.udata != NULL;

    Py_BEGIN_ALLOW_THREADS
    aerospike_enable_metrics(self->as, &err, metrics_policy_ref);
    Py_END_ALLOW_THREADS

CLEANUP_ON_ERROR:
    if (metrics_policy_ref) {
        // This means we initialized metrics_policy earlier
        as_metrics_policy_destroy(metrics_policy_ref);
    }

    if (err.code != AEROSPIKE_OK) {
        if (err.code == AEROSPIKE_METRICS_CONFLICT) {
            as_log_warn(err.message);
            as_error_reset(&err);
            // Even though we aren't raising an exception, the C client's enable_metrics() failed
            // so we still have to clean up udata now (see below)
        }

        if (free_udata_as_py_listener_data) {
            free_py_listener_data(
                (PyListenerData *)metrics_policy.metrics_listeners.udata);
        }
    }

    if (err.code != AEROSPIKE_OK) {
        raise_exception(&err);
        return NULL;
    }
    else {
        Py_INCREF(Py_None);
        return Py_None;
    }
}

PyObject *AerospikeClient_DisableMetrics(AerospikeClient *self, PyObject *args)
{
    as_error err;
    as_error_init(&err);

    Py_BEGIN_ALLOW_THREADS
    aerospike_disable_metrics(self->as, &err);
    Py_END_ALLOW_THREADS

    if (err.code == AEROSPIKE_METRICS_CONFLICT) {
        as_log_warn(err.message);
        as_error_reset(&err);
    }

    if (err.code != AEROSPIKE_OK) {
        raise_exception(&err);
        return NULL;
    }
    else {
        Py_INCREF(Py_None);
        return Py_None;
    }
}

// Regular metrics

PyObject *AerospikeClient_GetStats(AerospikeClient *self)
{
    as_cluster_stats stats;

    Py_BEGIN_ALLOW_THREADS
    aerospike_stats(self->as, &stats);
    Py_END_ALLOW_THREADS

    as_error err;
    as_error_init(&err);
    PyObject *py_cluster_stats =
        create_py_cluster_stats_from_as_cluster_stats(&err, &stats);

    aerospike_stats_destroy(&stats);

    if (py_cluster_stats == NULL && err.code != AEROSPIKE_OK) {
        raise_exception(&err);
        return NULL;
    }

    // A Python native exception can also be raised in this case.
    return py_cluster_stats;
}
