/*******************************************************************************
 * Copyright 2013-2020 Aerospike, Inc.
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

#include <aerospike/aerospike_index.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/as_error.h>
#include <aerospike/as_exp.h>
#include <aerospike/as_log_macros.h>

#include "client.h"
#include "conversions.h"

as_partitions_status *parts_setup(uint16_t part_begin, uint16_t part_count,
                                  const as_digest *digest)
{
    as_partitions_status *parts_all =
        cf_malloc(sizeof(as_partitions_status) +
                  (sizeof(as_partition_status) * part_count));

    memset(parts_all, 0,
           sizeof(as_partitions_status) +
               (sizeof(as_partition_status) * part_count));
    parts_all->ref_count = 1;
    parts_all->part_begin = part_begin;
    parts_all->part_count = part_count;
    parts_all->done = false;
    parts_all->retry = true;

    for (uint16_t i = 0; i < part_count; i++) {
        as_partition_status *ps = &parts_all->parts[i];
        ps->part_id = part_begin + i;
        ps->retry = true;
        ps->digest.init = false;
        ps->bval = 0;
    }

    if (digest && digest->init) {
        parts_all->parts[0].digest = *digest;
    }

    return parts_all;
}

/*
* convert_partition_filter
* Converts partition filter from python object into partition_filter struct.
* Initiates the conversion from intermediate_partition_filter structs to partition_filter.
* builds the partition filter.
*/
as_status convert_partition_filter(AerospikeClient *self,
                                   PyObject *py_partition_filter,
                                   as_partition_filter *filter,
                                   as_partitions_status **pss, as_error *err)
{
    as_partitions_status *parts_all = NULL;
    as_partition_status *ps = NULL;

    // TODO what if py_partition_filter is NULL?

    if (!PyDict_Check(py_partition_filter)) {
        as_error_update(
            err, AEROSPIKE_ERR_PARAM,
            "invalid partition_filter policy, partition_filter must be a dict");
        goto ERROR_CLEANUP;
    }

    PyObject *begin = PyDict_GetItemString(py_partition_filter, "begin");
    PyObject *count = PyDict_GetItemString(py_partition_filter, "count");
    PyObject *digest = PyDict_GetItemString(py_partition_filter, "digest");
    PyObject *parts_stat =
        PyDict_GetItemString(py_partition_filter, "partition_status");

    if (parts_stat && !PyDict_Check(parts_stat)) {
        as_error_update(
            err, AEROSPIKE_ERR_PARAM,
            "invalid partition_filter policy, partition_status must be a dict");
        goto ERROR_CLEANUP;
    }

    long tmp_begin = 0;
    if (begin && PyLong_Check(begin)) {
        tmp_begin = PyLong_AsLong(begin);
    }
    else if (begin) {
        as_error_update(err, AEROSPIKE_ERR_PARAM,
                        "invalid partition_filter policy begin, begin must \
						be an int between 0 and %d inclusive",
                        CLUSTER_NPARTITIONS - 1);
        goto ERROR_CLEANUP;
    }

    if (PyErr_Occurred() && PyErr_ExceptionMatches(PyExc_OverflowError)) {
        as_error_update(err, AEROSPIKE_ERR_PARAM,
                        "invalid begin for partition id: %d, \
						begin must fit in long",
                        begin);
        goto ERROR_CLEANUP;
    }

    if (tmp_begin >= CLUSTER_NPARTITIONS || tmp_begin < 0) {
        as_error_update(err, AEROSPIKE_ERR_PARAM,
                        "invalid partition_filter policy begin, begin must \
						be an int between 0 and %d inclusive",
                        CLUSTER_NPARTITIONS - 1);
        goto ERROR_CLEANUP;
    }

    filter->begin = tmp_begin;

    long tmp_count = CLUSTER_NPARTITIONS;
    if (count && PyLong_Check(count)) {
        tmp_count = PyLong_AsLong(count);
    }
    else if (count) {
        as_error_update(err, AEROSPIKE_ERR_PARAM,
                        "invalid partition_filter policy count, count must \
						be an int between 1 and %d inclusive",
                        CLUSTER_NPARTITIONS);
        goto ERROR_CLEANUP;
    }

    if (PyErr_Occurred() && PyErr_ExceptionMatches(PyExc_OverflowError)) {
        as_error_update(err, AEROSPIKE_ERR_PARAM,
                        "invalid count for partition id: %d, \
						count must fit in long",
                        count);
        goto ERROR_CLEANUP;
    }

    if (tmp_count > CLUSTER_NPARTITIONS || tmp_count < 1) {
        as_error_update(err, AEROSPIKE_ERR_PARAM,
                        "invalid partition_filter policy count, count must \
						be an int between 1 and %d inclusive",
                        CLUSTER_NPARTITIONS);
        goto ERROR_CLEANUP;
    }

    filter->count = tmp_count;

    if (filter->begin + filter->count > CLUSTER_NPARTITIONS) {
        as_error_update(err, AEROSPIKE_ERR_PARAM,
                        "invalid partition filter range,\
						begin: %u count: %u, valid range when begin + count <= %d",
                        filter->begin, filter->count, CLUSTER_NPARTITIONS);
        goto ERROR_CLEANUP;
    }

    filter->digest.init = 0;

    if (parts_all) {
        *pss = parts_all;
    }

    return err->code;

ERROR_CLEANUP:

    if (parts_all) {
        free(parts_all);
    }

    return err->code;
}
