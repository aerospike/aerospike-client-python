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

#include <aerospike/as_error.h>
#include <aerospike/as_metrics.h>

#include "types.h"

PyObject *AerospikeClient_EnableMetrics(AerospikeClient *self, PyObject *args,
                                        PyObject *kwds);
PyObject *AerospikeClient_DisableMetrics(AerospikeClient *self, PyObject *args);
PyObject *AerospikeClient_GetMetricsSnapshot(AerospikeClient *self);

PyObject *AerospikeClient_GetStats(AerospikeClient *self);

/*
 * Convert MetricsPolicy.exporters into C exporters appended to metrics_policy.
 * metrics_policy->exporters must be empty. If err was populated, nothing is
 * left allocated.
 */
as_status as_metrics_policy_add_exporters_from_pyobject(
    as_error *err, PyObject *py_metrics_policy,
    as_metrics_policy *metrics_policy);

/*
 * as_metrics_policy_destroy() frees metrics_policy->exporters but not the
 * exporters in it. Call this before aerospike_destroy() or as_config_destroy()
 * for a config that had exporters added by
 * as_metrics_policy_add_exporters_from_pyobject(). That ends their lifetime
 * with the C config's. Frees the exporters and the vector.
 */
void as_metrics_policy_destroy_py_exporters(as_metrics_policy *metrics_policy);

/*
 * For a policy that is destroyed while the cluster still uses its exporters,
 * which is the case for enable_metrics(). Returns the exporters in
 * metrics_policy->exporters as a NULL-terminated array, which the caller owns.
 * Returns NULL if there are none, or if err was populated, in which case the
 * exporters were freed.
 */
PyMetricsExporter **
create_py_metrics_exporters_from_policy(as_error *err,
                                        as_metrics_policy *metrics_policy);

// exporters is a NULL-terminated array, or NULL
void py_metrics_exporters_destroy(PyMetricsExporter **exporters);

/*
 * Release the exporters installed by enable_metrics().
 * The cluster calls them, so they are owned by whatever owns the cluster: the
 * AerospikeGlobalHosts of a shared connection, which outlives any one client,
 * and otherwise the client.
 */
void aerospike_client_release_active_metrics_exporters(AerospikeClient *self);
