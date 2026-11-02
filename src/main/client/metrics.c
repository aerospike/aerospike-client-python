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

static void metrics_session_call_quiet(PyObject *session, const char *method)
{
    if (session == NULL) {
        return;
    }
    PyObject *result = PyObject_CallMethod(session, method, NULL);
    if (result == NULL) {
        PyErr_Clear();
        return;
    }
    Py_DECREF(result);
}

static PyObject *metrics_session_ensure(AerospikeClient *self)
{
    if (self->metrics_session != NULL) {
        Py_INCREF(self->metrics_session);
        return self->metrics_session;
    }

    PyObject *module = PyImport_ImportModule("aerospike_helpers.metrics.export");
    if (module == NULL) {
        return NULL;
    }
    PyObject *cls = PyObject_GetAttrString(module, "ExportSession");
    Py_DECREF(module);
    if (cls == NULL) {
        return NULL;
    }
    PyObject *session = PyObject_CallObject(cls, NULL);
    Py_DECREF(cls);
    if (session == NULL) {
        return NULL;
    }
    self->metrics_session = session;
    Py_INCREF(self->metrics_session);
    return self->metrics_session;
}

void AerospikeClient_StopMetricsExport(AerospikeClient *self)
{
    if (self == NULL) {
        return;
    }
    metrics_session_call_quiet(self->metrics_session, "stop");
}

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

    PyObject *session = metrics_session_ensure(self);
    if (session == NULL) {
        return NULL;
    }

    PyObject *policy_arg =
        (py_metrics_policy == NULL) ? Py_None : py_metrics_policy;
    PyObject *prepared =
        PyObject_CallMethod(session, "prepare_enable", "O", policy_arg);
    if (prepared == NULL) {
        Py_DECREF(session);
        return NULL;
    }

    // To be passed into C client. None means the C client's configured policy.
    PyObject *effective_policy = (prepared == Py_None) ? NULL : prepared;
    as_metrics_policy *metrics_policy_ref = NULL;
    // If enable_metrics() succeeds, our heap-allocated udata will be free'd later when metrics is disabled (like when client.close() is called)
    bool free_udata_as_py_listener_data = false;
    bool metrics_conflict = false;

    if (effective_policy == NULL) {
        // Use C client's config metrics policy
        metrics_policy_ref = NULL;
    }
    else {
        // Set a transaction-level metrics policy
        as_metrics_policy_init(&metrics_policy);
        metrics_policy_ref = &metrics_policy;
        int retval = set_as_metrics_policy_using_pyobject(
            &err, effective_policy, &metrics_policy);
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
            metrics_conflict = true;
            // Even though we aren't raising an exception, the C client's enable_metrics() failed
            // so we still have to clean up udata now (see below)
        }

        if (free_udata_as_py_listener_data) {
            free_py_listener_data(
                (PyListenerData *)metrics_policy.metrics_listeners.udata);
        }
    }

    if (err.code != AEROSPIKE_OK) {
        // Let abort run even if policy conversion left a Python exception set.
        PyObject *prev_type = NULL;
        PyObject *prev_value = NULL;
        PyObject *prev_tb = NULL;
        PyErr_Fetch(&prev_type, &prev_value, &prev_tb);
        metrics_session_call_quiet(session, "abort_enable");
        PyErr_Restore(prev_type, prev_value, prev_tb);
        Py_DECREF(prepared);
        Py_DECREF(session);
        raise_exception(&err);
        return NULL;
    }

    if (metrics_conflict) {
        metrics_session_call_quiet(session, "abort_enable");
    }
    else {
        PyObject *started = PyObject_CallMethod(session, "start", NULL);
        if (started == NULL) {
            Py_DECREF(prepared);
            Py_DECREF(session);
            return NULL;
        }
        Py_DECREF(started);
    }

    Py_DECREF(prepared);
    Py_DECREF(session);
    Py_INCREF(Py_None);
    return Py_None;
}

PyObject *AerospikeClient_DisableMetrics(AerospikeClient *self, PyObject *args)
{
    as_error err;
    as_error_init(&err);

    // Pause export before the C client runs its disable listener and closes
    // the file writer. Registered exporters stay registered.
    AerospikeClient_StopMetricsExport(self);

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

PyObject *AerospikeClient_MetricsSnapshot(AerospikeClient *self)
{
    PyObject *session = metrics_session_ensure(self);
    if (session == NULL) {
        return NULL;
    }
    PyObject *snapshot = PyObject_CallMethod(session, "peek", NULL);
    Py_DECREF(session);
    return snapshot;
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
