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
#include "partitions_status.h"

/*
* convert_partition_filter
* Converts partition filter from python object into partition_filter struct.
* Initiates the conversion from intermediate_partition_filter structs to partition_filter.
* builds the partition filter.
*/
as_status convert_partition_filter(AerospikeClient *self,
                                   PyObject *py_partition_filter,
                                   as_partition_filter *filter, as_error *err)
{
    // TODO what if py_partition_filter is NULL?

    if (!PyDict_Check(py_partition_filter)) {
        as_error_update(
            err, AEROSPIKE_ERR_PARAM,
            "invalid partition_filter policy, partition_filter must be a dict");
        goto EXIT;
    }

    PyObject *begin = PyDict_GetItemString(py_partition_filter, "begin");
    PyObject *count = PyDict_GetItemString(py_partition_filter, "count");
    PyObject *digest = PyDict_GetItemString(py_partition_filter, "digest");
    PyObject *parts_stat =
        PyDict_GetItemString(py_partition_filter, "partition_status");

    if (parts_stat == Py_None) {
        parts_stat = NULL;
    }

    if (parts_stat && !PyObject_TypeCheck(
                          parts_stat, &AerospikePartitionsStatusObject_Type)) {
        as_error_update(err, AEROSPIKE_ERR_PARAM,
                        "invalid partition_filter policy, partition_status "
                        "must be of type aerospike.PartitionsStatus");
        goto EXIT;
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
        goto EXIT;
    }

    if (PyErr_Occurred() && PyErr_ExceptionMatches(PyExc_OverflowError)) {
        as_error_update(err, AEROSPIKE_ERR_PARAM,
                        "invalid begin for partition id: %d, \
						begin must fit in long",
                        tmp_begin);
        goto EXIT;
    }

    if (tmp_begin >= CLUSTER_NPARTITIONS || tmp_begin < 0) {
        as_error_update(err, AEROSPIKE_ERR_PARAM,
                        "invalid partition_filter policy begin, begin must \
						be an int between 0 and %d inclusive",
                        CLUSTER_NPARTITIONS - 1);
        goto EXIT;
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
        goto EXIT;
    }

    if (PyErr_Occurred() && PyErr_ExceptionMatches(PyExc_OverflowError)) {
        as_error_update(err, AEROSPIKE_ERR_PARAM,
                        "invalid count for partition id: %d, \
						count must fit in long",
                        tmp_count);
        goto EXIT;
    }

    if (tmp_count > CLUSTER_NPARTITIONS || tmp_count < 1) {
        as_error_update(err, AEROSPIKE_ERR_PARAM,
                        "invalid partition_filter policy count, count must \
						be an int between 1 and %d inclusive",
                        CLUSTER_NPARTITIONS);
        goto EXIT;
    }

    filter->count = tmp_count;

    if (filter->begin + filter->count > CLUSTER_NPARTITIONS) {
        as_error_update(err, AEROSPIKE_ERR_PARAM,
                        "invalid partition filter range,\
						begin: %u count: %u, valid range when begin + count <= %d",
                        filter->begin, filter->count, CLUSTER_NPARTITIONS);
        goto EXIT;
    }

    filter->digest.init = 0;
    if (digest && PyDict_Check(digest)) {

        // TODO check these for overflow
        PyObject *init = PyDict_GetItemString(digest, "init");
        if (init && PyLong_Check(init)) {
            filter->digest.init = PyLong_AsLong(init);
        }

        PyObject *value = PyDict_GetItemString(digest, "value");
        if (value && PyUnicode_Check(value)) {
            strncpy((char *)filter->digest.value,
                    (char *)PyUnicode_AsUTF8(value), AS_DIGEST_VALUE_SIZE);
        }
    }

    if (parts_stat) {
        filter->parts_all =
            ((AerospikePartitionsStatusObject *)parts_stat)->parts_all;
    }

EXIT:

    return err->code;
}
