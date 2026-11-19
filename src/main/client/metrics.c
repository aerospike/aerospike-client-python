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

#include "metrics.h"

#include <aerospike/aerospike_stats.h>
#include <aerospike/as_cluster.h>
#include <aerospike/as_latency.h>
#include <aerospike/as_log_macros.h>
#include <aerospike/as_metrics.h>

#include <stdlib.h>
#include <string.h>

#include "conversions.h"
#include "exceptions.h"
#include "policy.h"

// Established-client metrics: snapshot export, deprecated listeners, and the
// report_dir file exporter. Operational metrics, usage counters, and the
// operational sampler stay at the C client defaults unless the policy sets them.
// Usage counts feature.api.blocking, deferred, and background on public API calls.

typedef struct PyMetricsExporter {
    as_metrics_exporter base;
    PyObject *py_exporter;
} PyMetricsExporter;

struct PyMetricsExporterList {
    PyMetricsExporter **items;
    uint32_t count;
};

static bool client_metrics_enabled(AerospikeClient *self)
{
    return self && self->as && self->as->cluster &&
           self->as->cluster->metrics_enabled;
}

static int set_new_attr(as_error *err, PyObject *py_obj, const char *name,
                        PyObject *py_value)
{
    if (!py_value) {
        as_error_update(err, AEROSPIKE_ERR,
                        "Unable to create metrics snapshot field %s", name);
        return -1;
    }

    int result = PyObject_SetAttrString(py_obj, name, py_value);
    Py_DECREF(py_value);
    if (result == -1) {
        PyErr_Clear();
        as_error_update(err, AEROSPIKE_ERR,
                        "Unable to set metrics snapshot field %s", name);
        return -1;
    }
    return 0;
}

static PyObject *as_metrics_conn_snapshot_to_pyobject(
    as_error *err, const as_metrics_conn_snapshot *conn_snapshot)
{
    // The snapshot has the same counters as as_conn_stats, so both share one
    // conversion and its field names.
    as_conn_stats stats = {.in_use = conn_snapshot->in_use,
                           .in_pool = conn_snapshot->in_pool,
                           .opened = conn_snapshot->opened,
                           .closed = conn_snapshot->closed,
                           .recovered = conn_snapshot->recovered,
                           .aborted = conn_snapshot->aborted};
    PyObject *py_conn_stats =
        create_py_conn_stats_from_as_conn_stats(err, &stats);
    if (!py_conn_stats) {
        // err describes the failure, like the other helpers here.
        PyErr_Clear();
    }
    return py_conn_stats;
}

static PyObject *as_metrics_latency_buckets_to_pyobject(
    as_error *err, const as_metrics_latency_snapshot *latency_snapshot)
{
    uint8_t count =
        latency_snapshot->buckets ? latency_snapshot->bucket_count : 0;
    PyObject *py_list = PyList_New(count);
    if (!py_list) {
        as_error_update(err, AEROSPIKE_ERR,
                        "Failed to create latency bucket list");
        return NULL;
    }

    for (uint8_t i = 0; i < count; i++) {
        PyObject *py_bucket =
            PyLong_FromUnsignedLongLong(latency_snapshot->buckets[i]);
        if (!py_bucket || PyList_SetItem(py_list, i, py_bucket) == -1) {
            Py_XDECREF(py_bucket);
            Py_DECREF(py_list);
            PyErr_Clear();
            as_error_update(err, AEROSPIKE_ERR,
                            "Failed to append latency bucket at index %u", i);
            return NULL;
        }
    }
    return py_list;
}

static PyObject *as_metrics_latency_snapshot_to_pyobject(
    as_error *err, const as_metrics_latency_snapshot *latency_snapshot)
{
    PyObject *py_latency_snapshot = create_class_instance_from_module(
        err, "aerospike_helpers.metrics", "LatencySnapshot", NULL);
    if (!py_latency_snapshot) {
        return NULL;
    }

    PyObject *py_buckets =
        as_metrics_latency_buckets_to_pyobject(err, latency_snapshot);
    if (set_new_attr(err, py_latency_snapshot, "buckets", py_buckets) != 0) {
        Py_DECREF(py_latency_snapshot);
        return NULL;
    }

    const char *field_names[] = {"sum", "count", "min", "max"};
    uint64_t values[] = {latency_snapshot->sum, latency_snapshot->count,
                         latency_snapshot->min, latency_snapshot->max};
    for (unsigned long i = 0; i < sizeof(field_names) / sizeof(field_names[0]);
         i++) {
        if (set_new_attr(err, py_latency_snapshot, field_names[i],
                         PyLong_FromUnsignedLongLong(values[i])) != 0) {
            Py_DECREF(py_latency_snapshot);
            return NULL;
        }
    }
    return py_latency_snapshot;
}

static PyObject *as_metrics_latencies_to_pyobject(
    as_error *err, const as_metrics_latency_snapshot *latency_snapshots)
{
    PyObject *py_latencies = PyDict_New();
    if (!py_latencies) {
        as_error_update(err, AEROSPIKE_ERR,
                        "Failed to create latency dictionary");
        return NULL;
    }
    for (uint8_t i = 0; i < AS_LATENCY_TYPE_MAX; i++) {
        const char *latency_name = as_latency_type_to_string(i);
        PyObject *py_latency_snapshot =
            as_metrics_latency_snapshot_to_pyobject(err, &latency_snapshots[i]);
        if (!py_latency_snapshot) {
            Py_DECREF(py_latencies);
            return NULL;
        }
        int result = PyDict_SetItemString(py_latencies, latency_name,
                                          py_latency_snapshot);
        Py_DECREF(py_latency_snapshot);
        if (result == -1) {
            PyErr_Clear();
            Py_DECREF(py_latencies);
            as_error_update(err, AEROSPIKE_ERR, "Failed to set latencies[%s]",
                            latency_name);
            return NULL;
        }
    }
    return py_latencies;
}

static PyObject *as_metrics_namespace_snapshot_to_pyobject(
    as_error *err, const as_metrics_namespace_snapshot *namespace_snapshot)
{
    PyObject *py_namespace_snapshot = create_class_instance_from_module(
        err, "aerospike_helpers.metrics", "NamespaceSnapshot", NULL);
    if (!py_namespace_snapshot) {
        return NULL;
    }

    if (set_new_attr(err, py_namespace_snapshot, "name",
                     PyUnicode_FromString(namespace_snapshot->name
                                              ? namespace_snapshot->name
                                              : "")) != 0) {
        goto error;
    }

    const char *counter_names[] = {
        "errors",         "client_timeouts", "server_timeouts", "key_busy",
        "record_too_big", "device_overload", "bytes_in",        "bytes_out"};
    uint64_t counters[] = {namespace_snapshot->errors,
                           namespace_snapshot->client_timeouts,
                           namespace_snapshot->server_timeouts,
                           namespace_snapshot->key_busy,
                           namespace_snapshot->record_too_big,
                           namespace_snapshot->device_overload,
                           namespace_snapshot->bytes_in,
                           namespace_snapshot->bytes_out};
    for (unsigned long i = 0;
         i < sizeof(counter_names) / sizeof(counter_names[0]); i++) {
        if (set_new_attr(err, py_namespace_snapshot, counter_names[i],
                         PyLong_FromUnsignedLongLong(counters[i])) != 0) {
            goto error;
        }
    }

    // namespace_snapshot->latencies is the per-attempt series. Command latency
    // is snapshot->command_latencies.
    PyObject *py_latencies =
        as_metrics_latencies_to_pyobject(err, namespace_snapshot->latencies);
    if (!py_latencies) {
        goto error;
    }
    if (set_new_attr(err, py_namespace_snapshot, "latencies", py_latencies) !=
        0) {
        goto error;
    }
    return py_namespace_snapshot;

error:
    Py_DECREF(py_namespace_snapshot);
    return NULL;
}

static PyObject *as_metrics_node_snapshots_to_pyobject(
    as_error *err, as_metrics_node_snapshot **node_snapshots, uint32_t count)
{
    PyObject *py_list = PyList_New(count);
    if (!py_list) {
        as_error_update(err, AEROSPIKE_ERR,
                        "Failed to create node snapshot list");
        return NULL;
    }
    if (!node_snapshots && count > 0) {
        Py_DECREF(py_list);
        as_error_update(err, AEROSPIKE_ERR,
                        "Metrics node snapshot list is missing");
        return NULL;
    }

    for (uint32_t i = 0; i < count; i++) {
        const as_metrics_node_snapshot *node_snapshot = node_snapshots[i];
        PyObject *py_node_snapshot = create_class_instance_from_module(
            err, "aerospike_helpers.metrics", "NodeSnapshot", NULL);
        if (!py_node_snapshot) {
            Py_DECREF(py_list);
            return NULL;
        }

        if (set_new_attr(err, py_node_snapshot, "name",
                         PyUnicode_FromString(
                             node_snapshot->name ? node_snapshot->name : "")) !=
                0 ||
            set_new_attr(err, py_node_snapshot, "address",
                         PyUnicode_FromString(node_snapshot->address
                                                  ? node_snapshot->address
                                                  : "")) != 0 ||
            set_new_attr(err, py_node_snapshot, "port",
                         PyLong_FromUnsignedLong(node_snapshot->port)) != 0) {
            Py_DECREF(py_node_snapshot);
            Py_DECREF(py_list);
            return NULL;
        }

        // This client does not run async commands, so node_snapshot->async is
        // not copied.
        PyObject *py_sync =
            as_metrics_conn_snapshot_to_pyobject(err, &node_snapshot->sync);
        if (set_new_attr(err, py_node_snapshot, "sync", py_sync) != 0) {
            Py_DECREF(py_node_snapshot);
            Py_DECREF(py_list);
            return NULL;
        }
        if (set_new_attr(err, py_node_snapshot, "conn_open_failures",
                         PyLong_FromUnsignedLong(
                             node_snapshot->conn_open_failures)) != 0 ||
            set_new_attr(err, py_node_snapshot, "conn_tls_handshake_failures",
                         PyLong_FromUnsignedLong(
                             node_snapshot->conn_tls_handshake_failures)) !=
                0 ||
            set_new_attr(err, py_node_snapshot, "conn_auth_failures",
                         PyLong_FromUnsignedLong(
                             node_snapshot->conn_auth_failures)) != 0) {
            Py_DECREF(py_node_snapshot);
            Py_DECREF(py_list);
            return NULL;
        }

        PyObject *py_conn_latency = as_metrics_latency_snapshot_to_pyobject(
            err, &node_snapshot->conn_latency);
        if (set_new_attr(err, py_node_snapshot, "conn_latency",
                         py_conn_latency) != 0) {
            Py_DECREF(py_node_snapshot);
            Py_DECREF(py_list);
            return NULL;
        }

        PyObject *py_namespaces = PyList_New(node_snapshot->namespace_count);
        if (!py_namespaces) {
            Py_DECREF(py_node_snapshot);
            Py_DECREF(py_list);
            as_error_update(err, AEROSPIKE_ERR,
                            "Failed to create namespace snapshot list");
            return NULL;
        }
        for (uint32_t j = 0; j < node_snapshot->namespace_count; j++) {
            PyObject *py_namespace_snapshot =
                as_metrics_namespace_snapshot_to_pyobject(
                    err, &node_snapshot->namespaces[j]);
            if (!py_namespace_snapshot ||
                PyList_SetItem(py_namespaces, j, py_namespace_snapshot) == -1) {
                Py_XDECREF(py_namespace_snapshot);
                Py_DECREF(py_namespaces);
                Py_DECREF(py_node_snapshot);
                Py_DECREF(py_list);
                PyErr_Clear();
                return NULL;
            }
        }
        if (set_new_attr(err, py_node_snapshot, "namespaces", py_namespaces) !=
                0 ||
            PyList_SetItem(py_list, i, py_node_snapshot) == -1) {
            Py_XDECREF(py_node_snapshot);
            Py_DECREF(py_list);
            PyErr_Clear();
            return NULL;
        }
    }
    return py_list;
}

static PyObject *
as_metrics_snapshot_to_pyobject(as_error *err,
                                const as_metrics_snapshot *snapshot)
{
    PyObject *py_snapshot = create_class_instance_from_module(
        err, "aerospike_helpers.metrics", "MetricsSnapshot", NULL);
    if (!py_snapshot) {
        return NULL;
    }

    if (set_new_attr(err, py_snapshot, "timestamp",
                     PyUnicode_FromString(snapshot->timestamp)) != 0 ||
        set_new_attr(err, py_snapshot, "metrics_enabled",
                     PyBool_FromLong(snapshot->metrics_enabled)) != 0 ||
        set_new_attr(err, py_snapshot, "operational_metrics_enabled",
                     PyBool_FromLong(snapshot->operational_metrics_enabled)) !=
            0 ||
        set_new_attr(err, py_snapshot, "usage_metrics_enabled",
                     PyBool_FromLong(snapshot->usage_metrics_enabled)) != 0 ||
        set_new_attr(err, py_snapshot, "cluster_name",
                     PyUnicode_FromString(snapshot->cluster_name
                                              ? snapshot->cluster_name
                                              : "")) != 0 ||
        set_new_attr(err, py_snapshot, "client_type",
                     PyUnicode_FromString(
                         snapshot->client_type ? snapshot->client_type : "")) !=
            0 ||
        set_new_attr(err, py_snapshot, "client_version",
                     PyUnicode_FromString(snapshot->client_version
                                              ? snapshot->client_version
                                              : "")) != 0 ||
        set_new_attr(err, py_snapshot, "app_id",
                     PyUnicode_FromString(snapshot->app_id ? snapshot->app_id
                                                           : "")) != 0) {
        goto error;
    }

    PyObject *py_labels = PyDict_New();
    if (!py_labels) {
        as_error_update(err, AEROSPIKE_ERR, "Failed to create metrics labels");
        goto error;
    }
    for (uint32_t i = 0; i < snapshot->label_count; i++) {
        const char *name =
            snapshot->labels[i].name ? snapshot->labels[i].name : "";
        const char *value =
            snapshot->labels[i].value ? snapshot->labels[i].value : "";
        PyObject *py_value = PyUnicode_FromString(value);
        if (!py_value ||
            PyDict_SetItemString(py_labels, name, py_value) == -1) {
            Py_XDECREF(py_value);
            Py_DECREF(py_labels);
            PyErr_Clear();
            as_error_update(err, AEROSPIKE_ERR, "Failed to copy metrics label");
            goto error;
        }
        Py_DECREF(py_value);
    }
    if (set_new_attr(err, py_snapshot, "labels", py_labels) != 0) {
        goto error;
    }

    if (set_new_attr(err, py_snapshot, "recover_queue_size",
                     PyLong_FromUnsignedLong(snapshot->recover_queue_size)) !=
            0 ||
        set_new_attr(err, py_snapshot, "invalid_node_count",
                     PyLong_FromUnsignedLong(snapshot->invalid_node_count)) !=
            0 ||
        set_new_attr(
            err, py_snapshot, "validation_open_failures",
            PyLong_FromUnsignedLong(snapshot->validation_open_failures)) != 0 ||
        set_new_attr(err, py_snapshot, "validation_tls_handshake_failures",
                     PyLong_FromUnsignedLong(
                         snapshot->validation_tls_handshake_failures)) != 0 ||
        set_new_attr(
            err, py_snapshot, "validation_auth_failures",
            PyLong_FromUnsignedLong(snapshot->validation_auth_failures)) != 0 ||
        set_new_attr(err, py_snapshot, "delay_queue_timeout_count",
                     PyLong_FromUnsignedLongLong(
                         snapshot->delay_queue_timeout_count)) != 0 ||
        set_new_attr(err, py_snapshot, "command_count",
                     PyLong_FromUnsignedLongLong(snapshot->command_count)) !=
            0 ||
        set_new_attr(err, py_snapshot, "retry_count",
                     PyLong_FromUnsignedLongLong(snapshot->retry_count)) != 0 ||
        set_new_attr(err, py_snapshot, "api_blocking",
                     PyLong_FromUnsignedLongLong(snapshot->api_blocking)) !=
            0 ||
        set_new_attr(err, py_snapshot, "api_deferred",
                     PyLong_FromUnsignedLongLong(snapshot->api_deferred)) !=
            0 ||
        set_new_attr(err, py_snapshot, "api_background",
                     PyLong_FromUnsignedLongLong(snapshot->api_background)) !=
            0 ||
        set_new_attr(err, py_snapshot, "cpu",
                     PyLong_FromUnsignedLong(snapshot->cpu)) != 0 ||
        set_new_attr(err, py_snapshot, "mem",
                     PyLong_FromUnsignedLongLong(snapshot->mem)) != 0 ||
        set_new_attr(err, py_snapshot, "latency_columns",
                     PyLong_FromUnsignedLong(snapshot->latency_columns)) != 0 ||
        set_new_attr(err, py_snapshot, "latency_shift",
                     PyLong_FromUnsignedLong(snapshot->latency_shift)) != 0 ||
        set_new_attr(err, py_snapshot, "latency_unit",
                     PyLong_FromUnsignedLong(snapshot->latency_unit)) != 0 ||
        set_new_attr(err, py_snapshot, "nodes_departed_possibly_truncated",
                     PyBool_FromLong(
                         snapshot->nodes_departed_possibly_truncated)) != 0) {
        goto error;
    }

    PyObject *py_command_latencies =
        as_metrics_latencies_to_pyobject(err, snapshot->command_latencies);
    if (!py_command_latencies) {
        goto error;
    }
    if (set_new_attr(err, py_snapshot, "command_latencies",
                     py_command_latencies) != 0) {
        goto error;
    }

    // This client does not run async commands, so event-loop gauges are not copied.
    PyObject *py_nodes = as_metrics_node_snapshots_to_pyobject(
        err, snapshot->nodes, snapshot->nodes_count);
    PyObject *py_departed = as_metrics_node_snapshots_to_pyobject(
        err, snapshot->nodes_departed, snapshot->nodes_departed_count);
    if (!py_nodes || !py_departed) {
        Py_XDECREF(py_nodes);
        Py_XDECREF(py_departed);
        goto error;
    }
    // set_new_attr consumes the value reference, including on failure.
    if (set_new_attr(err, py_snapshot, "nodes", py_nodes) != 0) {
        Py_DECREF(py_departed);
        goto error;
    }
    if (set_new_attr(err, py_snapshot, "nodes_departed", py_departed) != 0) {
        goto error;
    }
    return py_snapshot;

error:
    Py_DECREF(py_snapshot);
    return NULL;
}

static as_status py_metrics_export(as_metrics_exporter *exporter, as_error *err,
                                   const as_metrics_snapshot *snapshot)
{
    PyMetricsExporter *py_exporter = (PyMetricsExporter *)exporter;
    PyGILState_STATE state = PyGILState_Ensure();
    as_status status = AEROSPIKE_OK;

    PyObject *py_snapshot = as_metrics_snapshot_to_pyobject(err, snapshot);
    if (!py_snapshot) {
        if (PyErr_Occurred()) {
            error_from_raised_exception(err, AEROSPIKE_ERR,
                                        "Unable to build the metrics snapshot");
        }
        else if (err->code == AEROSPIKE_OK) {
            as_error_set_message(err, AEROSPIKE_ERR,
                                 "Failed to build metrics snapshot");
        }
        status = err->code == AEROSPIKE_OK ? AEROSPIKE_ERR : err->code;
        goto done;
    }

    PyObject *py_result = PyObject_CallMethod(py_exporter->py_exporter,
                                              "export", "O", py_snapshot);
    Py_DECREF(py_snapshot);
    if (!py_result) {
        status = error_from_raised_exception(
            err, AEROSPIKE_ERR, "Metrics exporter's export() raised");
        goto done;
    }
    Py_DECREF(py_result);

done:
    PyGILState_Release(state);
    return status;
}

void py_metrics_exporter_list_destroy(PyMetricsExporterList *list)
{
    if (!list) {
        return;
    }

    PyGILState_STATE state = PyGILState_Ensure();
    for (uint32_t i = 0; i < list->count; i++) {
        Py_XDECREF(list->items[i]->py_exporter);
        free(list->items[i]);
    }
    PyGILState_Release(state);
    free(list->items);
    free(list);
}

void aerospike_client_release_active_metrics_exporters(AerospikeClient *self)
{
    if (!self) {
        return;
    }
    py_metrics_exporter_list_destroy(self->active_metrics_exporters);
    self->active_metrics_exporters = NULL;
}

void aerospike_client_release_all_metrics_exporters(AerospikeClient *self)
{
    if (!self) {
        return;
    }
    py_metrics_exporter_list_destroy(self->config_metrics_exporters);
    py_metrics_exporter_list_destroy(self->active_metrics_exporters);
    self->config_metrics_exporters = NULL;
    self->active_metrics_exporters = NULL;
}

int py_metrics_exporters_from_pyobject(as_error *err,
                                       PyObject *py_metrics_policy,
                                       as_metrics_policy *metrics_policy,
                                       PyMetricsExporterList **out)
{
    if (out) {
        *out = NULL;
    }

    PyObject *py_exporters =
        PyObject_GetAttrString(py_metrics_policy, "exporters");
    if (!py_exporters) {
        return as_error_update(err, AEROSPIKE_ERR_PARAM,
                               "Unable to fetch exporters attribute");
    }
    if (py_exporters == Py_None) {
        Py_DECREF(py_exporters);
        return 0;
    }
    if (!PyList_Check(py_exporters)) {
        Py_DECREF(py_exporters);
        return as_error_update(err, AEROSPIKE_ERR_PARAM,
                               "MetricsPolicy.exporters must be a list type");
    }

    Py_ssize_t count = PyList_GET_SIZE(py_exporters);
    if (count == 0) {
        Py_DECREF(py_exporters);
        return 0;
    }

    PyMetricsExporterList *list = calloc(1, sizeof(PyMetricsExporterList));
    if (!list) {
        Py_DECREF(py_exporters);
        return as_error_update(err, AEROSPIKE_ERR_CLIENT,
                               "Failed to allocate metrics exporters");
    }
    list->items = calloc((size_t)count, sizeof(PyMetricsExporter *));
    if (!list->items) {
        free(list);
        Py_DECREF(py_exporters);
        return as_error_update(err, AEROSPIKE_ERR_CLIENT,
                               "Failed to allocate metrics exporters");
    }

    for (Py_ssize_t i = 0; i < count; i++) {
        PyObject *py_exporter = PyList_GET_ITEM(py_exporters, i);
        PyObject *py_export = PyObject_GetAttrString(py_exporter, "export");
        if (!py_export || !PyCallable_Check(py_export)) {
            Py_XDECREF(py_export);
            PyErr_Clear();
            py_metrics_exporter_list_destroy(list);
            Py_DECREF(py_exporters);
            return as_error_update(
                err, AEROSPIKE_ERR_PARAM,
                "MetricsPolicy.exporters must contain objects with a callable "
                "export method");
        }
        Py_DECREF(py_export);

        PyMetricsExporter *wrapper = calloc(1, sizeof(PyMetricsExporter));
        if (!wrapper) {
            py_metrics_exporter_list_destroy(list);
            Py_DECREF(py_exporters);
            return as_error_update(err, AEROSPIKE_ERR_CLIENT,
                                   "Failed to allocate metrics exporter");
        }
        wrapper->base.export_fn = py_metrics_export;
        wrapper->py_exporter = py_exporter;
        Py_INCREF(py_exporter);
        list->items[list->count++] = wrapper;
        as_metrics_policy_add_exporter(metrics_policy, &wrapper->base);
    }

    Py_DECREF(py_exporters);
    if (out) {
        *out = list;
    }
    else {
        py_metrics_exporter_list_destroy(list);
    }
    return 0;
}

// Extended metrics

PyObject *AerospikeClient_EnableMetrics(AerospikeClient *self, PyObject *args,
                                        PyObject *kwds)
{
    as_error err;
    as_error_init(&err);

    PyObject *py_metrics_policy = NULL;
    as_metrics_policy metrics_policy;
    PyMetricsExporterList *new_exporters = NULL;

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
        int retval = as_metrics_policy_set_using_pyobject(
            &err, py_metrics_policy, &metrics_policy, &new_exporters);
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
        // This means we initialized metrics_policy earlier.
        // Exporter objects are not freed here. The application owns them.
        as_metrics_policy_destroy(metrics_policy_ref);
    }

    if (err.code == AEROSPIKE_METRICS_CONFLICT) {
        as_log_warn(err.message);
        as_error_reset(&err);
        // Enable did not install the new policy. Drop the new wrappers.
        // Listener udata was not retained by the C client.
        py_metrics_exporter_list_destroy(new_exporters);
        new_exporters = NULL;
        if (free_udata_as_py_listener_data) {
            free_py_listener_data(
                (PyListenerData *)metrics_policy.metrics_listeners.udata);
        }
    }
    else if (err.code != AEROSPIKE_OK) {
        py_metrics_exporter_list_destroy(new_exporters);
        new_exporters = NULL;
        if (free_udata_as_py_listener_data) {
            free_py_listener_data(
                (PyListenerData *)metrics_policy.metrics_listeners.udata);
        }
        // runtime_enable disables any previous metrics before it can fail.
        // If metrics are no longer enabled, those wrappers are no longer called.
        if (!client_metrics_enabled(self)) {
            aerospike_client_release_active_metrics_exporters(self);
        }
    }
    else {
        aerospike_client_release_active_metrics_exporters(self);
        self->active_metrics_exporters = new_exporters;
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
    else if (!client_metrics_enabled(self)) {
        // Disable already pushed the final snapshot. Command-level exporters
        // are no longer referenced. Config exporters stay for a later connect.
        aerospike_client_release_active_metrics_exporters(self);
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

PyObject *AerospikeClient_GetMetricsSnapshot(AerospikeClient *self)
{
    as_error err;
    as_error_init(&err);
    as_metrics_snapshot *snapshot = NULL;

    Py_BEGIN_ALLOW_THREADS
    aerospike_get_metrics_snapshot(self->as, &err, &snapshot);
    Py_END_ALLOW_THREADS

    if (err.code != AEROSPIKE_OK || snapshot == NULL) {
        if (snapshot) {
            as_metrics_snapshot_destroy(snapshot);
        }
        raise_exception(&err);
        return NULL;
    }

    PyObject *py_snapshot = as_metrics_snapshot_to_pyobject(&err, snapshot);
    as_metrics_snapshot_destroy(snapshot);
    if (py_snapshot == NULL) {
        raise_exception(&err);
        return NULL;
    }
    return py_snapshot;
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
