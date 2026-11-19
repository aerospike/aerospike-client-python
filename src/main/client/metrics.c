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

#include <stdio.h>
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

// Snapshot helpers
//
// The helpers that build a Python snapshot describe a failure in err, but they
// leave the Python exception raised by the failed C-API call alone instead of
// clearing it. What happens to that exception depends on the caller:
// - get_metrics_snapshot() passes err to raise_exception(), which chains the
//   exception to the one it raises, so the user sees both.
// - py_metrics_export() returns into C code, which cannot carry a Python
//   exception, so it appends the exception to err's message.

// Describes in err that an attribute of a snapshot object could not be created.
// The exception raised by the failed C-API call stays raised, as described
// above. The C-API docs only say that these calls raise an exception, so its
// message is unspecified: it can differ between Python versions, can be empty
// (MemoryError), and does not always say which snapshot type or attribute was
// being created. err's message adds that context to the exception's message.
static int set_new_attr_error(as_error *err, PyObject *py_obj,
                              const char *action, const char *name)
{
    // The snapshot classes are heap types, so tp_name is the class name, like
    // NodeSnapshot.
    const char *snapshot_type_name = Py_TYPE(py_obj)->tp_name;
    as_error_update(err, AEROSPIKE_ERR, "Unable to create %s: could not %s %s",
                    snapshot_type_name, action, name);
    return -1;
}

// Consumes the reference to py_value, including on failure.
// py_value must not be NULL: the caller checks the call that created it.
static int set_new_attr(as_error *err, PyObject *py_obj, const char *name,
                        PyObject *py_value)
{
    int result = PyObject_SetAttrString(py_obj, name, py_value);
    Py_DECREF(py_value);
    if (result == -1) {
        return set_new_attr_error(err, py_obj, "set", name);
    }
    return 0;
}

static int set_new_str_attr(as_error *err, PyObject *py_obj, const char *name,
                            const char *value)
{
    PyObject *py_value = PyUnicode_FromString(value);
    if (!py_value) {
        return set_new_attr_error(err, py_obj, "create the value of", name);
    }
    return set_new_attr(err, py_obj, name, py_value);
}

static int set_new_uint64_attr(as_error *err, PyObject *py_obj,
                               const char *name, uint64_t value)
{
    PyObject *py_value = PyLong_FromUnsignedLongLong(value);
    if (!py_value) {
        return set_new_attr_error(err, py_obj, "create the value of", name);
    }
    return set_new_attr(err, py_obj, name, py_value);
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
    return create_py_conn_stats_from_as_conn_stats(err, &stats);
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
        if (!py_bucket) {
            Py_DECREF(py_list);
            as_error_update(err, AEROSPIKE_ERR,
                            "Failed to create latency bucket at index %u", i);
            return NULL;
        }
        // PyList_SetItem steals the reference to py_bucket, even on failure.
        int result = PyList_SetItem(py_list, i, py_bucket);
        if (result == -1) {
            Py_DECREF(py_list);
            as_error_update(err, AEROSPIKE_ERR,
                            "Failed to set latency bucket at index %u in list",
                            i);
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
    if (!py_buckets) {
        Py_DECREF(py_latency_snapshot);
        return NULL;
    }
    int result = set_new_attr(err, py_latency_snapshot, "buckets", py_buckets);
    if (result != 0) {
        Py_DECREF(py_latency_snapshot);
        return NULL;
    }

    const char *field_names[] = {"sum", "count", "min", "max"};
    uint64_t values[] = {latency_snapshot->sum, latency_snapshot->count,
                         latency_snapshot->min, latency_snapshot->max};
    for (unsigned long i = 0; i < sizeof(field_names) / sizeof(field_names[0]);
         i++) {
        result = set_new_uint64_attr(err, py_latency_snapshot, field_names[i],
                                     values[i]);
        if (result != 0) {
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

    const char *name = namespace_snapshot->name ? namespace_snapshot->name : "";
    int result = set_new_str_attr(err, py_namespace_snapshot, "name", name);
    if (result != 0) {
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
        result = set_new_uint64_attr(err, py_namespace_snapshot,
                                     counter_names[i], counters[i]);
        if (result != 0) {
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
    result =
        set_new_attr(err, py_namespace_snapshot, "latencies", py_latencies);
    if (result != 0) {
        goto error;
    }
    return py_namespace_snapshot;

error:
    Py_DECREF(py_namespace_snapshot);
    return NULL;
}

static PyObject *as_metrics_node_snapshot_to_pyobject(
    as_error *err, const as_metrics_node_snapshot *node_snapshot)
{
    PyObject *py_node_snapshot = create_class_instance_from_module(
        err, "aerospike_helpers.metrics", "NodeSnapshot", NULL);
    if (!py_node_snapshot) {
        return NULL;
    }

    const char *string_names[] = {"name", "address"};
    const char *string_values[] = {node_snapshot->name, node_snapshot->address};
    for (unsigned long i = 0;
         i < sizeof(string_names) / sizeof(string_names[0]); i++) {
        const char *value = string_values[i] ? string_values[i] : "";
        int result =
            set_new_str_attr(err, py_node_snapshot, string_names[i], value);
        if (result != 0) {
            goto error;
        }
    }

    const char *number_names[] = {"port", "conn_open_failures",
                                  "conn_tls_handshake_failures",
                                  "conn_auth_failures"};
    uint64_t number_values[] = {node_snapshot->port,
                                node_snapshot->conn_open_failures,
                                node_snapshot->conn_tls_handshake_failures,
                                node_snapshot->conn_auth_failures};
    for (unsigned long i = 0;
         i < sizeof(number_names) / sizeof(number_names[0]); i++) {
        int result = set_new_uint64_attr(err, py_node_snapshot, number_names[i],
                                         number_values[i]);
        if (result != 0) {
            goto error;
        }
    }

    // This client does not run async commands, so node_snapshot->async is
    // not copied.
    PyObject *py_sync =
        as_metrics_conn_snapshot_to_pyobject(err, &node_snapshot->sync);
    if (!py_sync) {
        goto error;
    }
    int result = set_new_attr(err, py_node_snapshot, "sync", py_sync);
    if (result != 0) {
        goto error;
    }

    PyObject *py_conn_latency = as_metrics_latency_snapshot_to_pyobject(
        err, &node_snapshot->conn_latency);
    if (!py_conn_latency) {
        goto error;
    }
    result =
        set_new_attr(err, py_node_snapshot, "conn_latency", py_conn_latency);
    if (result != 0) {
        goto error;
    }

    PyObject *py_namespaces = PyList_New(node_snapshot->namespace_count);
    if (!py_namespaces) {
        as_error_update(err, AEROSPIKE_ERR,
                        "Failed to create namespace snapshot list");
        goto error;
    }
    for (uint32_t i = 0; i < node_snapshot->namespace_count; i++) {
        PyObject *py_namespace_snapshot =
            as_metrics_namespace_snapshot_to_pyobject(
                err, &node_snapshot->namespaces[i]);
        if (!py_namespace_snapshot) {
            Py_DECREF(py_namespaces);
            goto error;
        }
        // PyList_SetItem steals the reference, even on failure.
        result = PyList_SetItem(py_namespaces, i, py_namespace_snapshot);
        if (result == -1) {
            Py_DECREF(py_namespaces);
            as_error_update(
                err, AEROSPIKE_ERR,
                "Failed to set namespace snapshot at index %u in list", i);
            goto error;
        }
    }
    result = set_new_attr(err, py_node_snapshot, "namespaces", py_namespaces);
    if (result != 0) {
        goto error;
    }
    return py_node_snapshot;

error:
    Py_DECREF(py_node_snapshot);
    return NULL;
}

static PyObject *as_metrics_node_snapshots_to_pyobject(
    as_error *err, as_metrics_node_snapshot **node_snapshots, uint32_t count)
{
    // The caller checked that node_snapshots is not NULL when count > 0.
    PyObject *py_list = PyList_New(count);
    if (!py_list) {
        as_error_update(err, AEROSPIKE_ERR,
                        "Failed to create node snapshot list");
        return NULL;
    }

    for (uint32_t i = 0; i < count; i++) {
        PyObject *py_node_snapshot =
            as_metrics_node_snapshot_to_pyobject(err, node_snapshots[i]);
        if (!py_node_snapshot) {
            Py_DECREF(py_list);
            return NULL;
        }
        // PyList_SetItem steals the reference, even on failure.
        int result = PyList_SetItem(py_list, i, py_node_snapshot);
        if (result == -1) {
            Py_DECREF(py_list);
            as_error_update(err, AEROSPIKE_ERR,
                            "Failed to set node snapshot at index %u in list",
                            i);
            return NULL;
        }
    }
    return py_list;
}

static PyObject *
as_metrics_snapshot_to_pyobject(as_error *err,
                                const as_metrics_snapshot *snapshot)
{
    // Check the input before creating any Python object, so an invalid
    // snapshot does not allocate anything.
    if (!snapshot->nodes && snapshot->nodes_count > 0) {
        as_error_update(err, AEROSPIKE_ERR,
                        "Metrics node snapshot list is missing");
        return NULL;
    }
    if (!snapshot->nodes_departed && snapshot->nodes_departed_count > 0) {
        as_error_update(err, AEROSPIKE_ERR,
                        "Metrics departed node snapshot list is missing");
        return NULL;
    }

    PyObject *py_snapshot = create_class_instance_from_module(
        err, "aerospike_helpers.metrics", "MetricsSnapshot", NULL);
    if (!py_snapshot) {
        return NULL;
    }

    const char *string_names[] = {"timestamp", "cluster_name", "client_type",
                                  "client_version", "app_id"};
    const char *string_values[] = {snapshot->timestamp, snapshot->cluster_name,
                                   snapshot->client_type,
                                   snapshot->client_version, snapshot->app_id};
    for (unsigned long i = 0;
         i < sizeof(string_names) / sizeof(string_names[0]); i++) {
        const char *value = string_values[i] ? string_values[i] : "";
        int result = set_new_str_attr(err, py_snapshot, string_names[i], value);
        if (result != 0) {
            goto error;
        }
    }

    const char *bool_names[] = {
        "metrics_enabled", "operational_metrics_enabled",
        "usage_metrics_enabled", "nodes_departed_possibly_truncated"};
    bool bool_values[] = {snapshot->metrics_enabled,
                          snapshot->operational_metrics_enabled,
                          snapshot->usage_metrics_enabled,
                          snapshot->nodes_departed_possibly_truncated};
    for (unsigned long i = 0; i < sizeof(bool_names) / sizeof(bool_names[0]);
         i++) {
        // PyBool_FromLong() cannot fail
        PyObject *py_value = PyBool_FromLong(bool_values[i]);
        int result = set_new_attr(err, py_snapshot, bool_names[i], py_value);
        if (result != 0) {
            goto error;
        }
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
        if (!py_value) {
            Py_DECREF(py_labels);
            as_error_update(err, AEROSPIKE_ERR,
                            "Failed to create value of metrics label %s", name);
            goto error;
        }
        int result = PyDict_SetItemString(py_labels, name, py_value);
        Py_DECREF(py_value);
        if (result == -1) {
            Py_DECREF(py_labels);
            as_error_update(err, AEROSPIKE_ERR,
                            "Failed to set metrics label %s", name);
            goto error;
        }
    }
    int result = set_new_attr(err, py_snapshot, "labels", py_labels);
    if (result != 0) {
        goto error;
    }

    const char *number_names[] = {"recover_queue_size",
                                  "invalid_node_count",
                                  "validation_open_failures",
                                  "validation_tls_handshake_failures",
                                  "validation_auth_failures",
                                  "delay_queue_timeout_count",
                                  "command_count",
                                  "retry_count",
                                  "api_blocking",
                                  "api_deferred",
                                  "api_background",
                                  "cpu",
                                  "mem",
                                  "latency_columns",
                                  "latency_shift",
                                  "latency_unit"};
    uint64_t number_values[] = {snapshot->recover_queue_size,
                                snapshot->invalid_node_count,
                                snapshot->validation_open_failures,
                                snapshot->validation_tls_handshake_failures,
                                snapshot->validation_auth_failures,
                                snapshot->delay_queue_timeout_count,
                                snapshot->command_count,
                                snapshot->retry_count,
                                snapshot->api_blocking,
                                snapshot->api_deferred,
                                snapshot->api_background,
                                snapshot->cpu,
                                snapshot->mem,
                                snapshot->latency_columns,
                                snapshot->latency_shift,
                                snapshot->latency_unit};
    for (unsigned long i = 0;
         i < sizeof(number_names) / sizeof(number_names[0]); i++) {
        result = set_new_uint64_attr(err, py_snapshot, number_names[i],
                                     number_values[i]);
        if (result != 0) {
            goto error;
        }
    }

    PyObject *py_command_latencies =
        as_metrics_latencies_to_pyobject(err, snapshot->command_latencies);
    if (!py_command_latencies) {
        goto error;
    }
    result = set_new_attr(err, py_snapshot, "command_latencies",
                          py_command_latencies);
    if (result != 0) {
        goto error;
    }

    // This client does not run async commands, so event-loop gauges are not copied.
    PyObject *py_nodes = as_metrics_node_snapshots_to_pyobject(
        err, snapshot->nodes, snapshot->nodes_count);
    if (!py_nodes) {
        goto error;
    }
    PyObject *py_departed = as_metrics_node_snapshots_to_pyobject(
        err, snapshot->nodes_departed, snapshot->nodes_departed_count);
    if (!py_departed) {
        Py_DECREF(py_nodes);
        goto error;
    }
    // See set_new_attr() for who owns py_nodes if this fails
    result = set_new_attr(err, py_snapshot, "nodes", py_nodes);
    if (result != 0) {
        Py_DECREF(py_departed);
        goto error;
    }
    result = set_new_attr(err, py_snapshot, "nodes_departed", py_departed);
    if (result != 0) {
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
            // The exception cannot be chained here, so move it into err's
            // message (see "Snapshot helpers" above). If err has a message,
            // keep it as the prefix.
            char context[AS_ERROR_MESSAGE_MAX_SIZE] =
                "Unable to build the metrics snapshot";
            if (err->code != AEROSPIKE_OK) {
                snprintf(context, sizeof(context), "%s", err->message);
            }
            error_from_raised_exception(err, AEROSPIKE_ERR, context);
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

    // Check every exporter before allocating anything for them
    for (Py_ssize_t i = 0; i < count; i++) {
        PyObject *py_exporter = PyList_GET_ITEM(py_exporters, i);
        PyObject *py_export = PyObject_GetAttrString(py_exporter, "export");
        if (!py_export) {
            Py_DECREF(py_exporters);
            return as_error_update(
                err, AEROSPIKE_ERR_PARAM,
                "MetricsPolicy.exporters[%zd] must have an export method", i);
        }
        int is_callable = PyCallable_Check(py_export);
        Py_DECREF(py_export);
        if (!is_callable) {
            Py_DECREF(py_exporters);
            return as_error_update(
                err, AEROSPIKE_ERR_PARAM,
                "MetricsPolicy.exporters[%zd].export must be callable", i);
        }
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

    if (err.code != AEROSPIKE_OK) {
        if (snapshot) {
            as_metrics_snapshot_destroy(snapshot);
        }
        raise_exception(&err);
        return NULL;
    }
    if (snapshot == NULL) {
        as_error_update(&err, AEROSPIKE_ERR,
                        "The client did not return a metrics snapshot");
        raise_exception(&err);
        return NULL;
    }

    PyObject *py_snapshot = as_metrics_snapshot_to_pyobject(&err, snapshot);
    as_metrics_snapshot_destroy(snapshot);
    if (py_snapshot == NULL) {
        // If err is not set, a snapshot class raised a Python exception, which
        // is returned as is. Otherwise, raise_exception() chains the exception
        // raised by the failed C-API call (see "Snapshot helpers" above).
        if (err.code != AEROSPIKE_OK) {
            raise_exception(&err);
        }
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
