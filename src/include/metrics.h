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

#include <Python.h>

#include <aerospike/as_error.h>
#include <aerospike/as_metrics.h>

#include "types.h"

PyObject *AerospikeClient_EnableMetrics(AerospikeClient *self, PyObject *args,
                                        PyObject *kwds);
PyObject *AerospikeClient_DisableMetrics(AerospikeClient *self, PyObject *args);

PyObject *AerospikeClient_GetStats(AerospikeClient *self);

/*
 * Convert MetricsPolicy.exporters into C exporters appended to metrics_policy.
 * On success, *out owns the wrapper list (NULL when no exporters were set).
 * On failure, *out is NULL and nothing is left allocated.
 */
int py_metrics_exporters_from_pyobject(as_error *err,
                                       PyObject *py_metrics_policy,
                                       as_metrics_policy *metrics_policy,
                                       PyMetricsExporterList **out);

void py_metrics_exporter_list_destroy(PyMetricsExporterList *list);

/* Release exporters installed by enable_metrics(). */
void aerospike_client_release_active_metrics_exporters(AerospikeClient *self);

/* Release config and enable_metrics() exporters. */
void aerospike_client_release_all_metrics_exporters(AerospikeClient *self);
