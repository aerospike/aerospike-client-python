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

as_partitions_status*
parts_setup(uint16_t part_begin, uint16_t part_count, const as_digest* digest)
{
	as_partitions_status* parts_all = cf_malloc(sizeof(as_partitions_status) +
											   (sizeof(as_partition_status) * part_count));

	memset(parts_all, 0, 
						sizeof(as_partitions_status) +
						(sizeof(as_partition_status) * part_count));
	parts_all->ref_count = 1;
	parts_all->part_begin = part_begin;
	parts_all->part_count = part_count;
	parts_all->done = false;

	for (uint16_t i = 0; i < part_count; i++) {
		as_partition_status* ps = &parts_all->parts[i];
		ps->part_id = part_begin + i;
		ps->done = false;
		ps->digest.init = false;
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
								   as_partitions_status **pss,
								   as_error *err)
{
	as_partitions_status *part_all = NULL;
	as_partition_status *ps = NULL;

	// TODO what if py_partition_filter is NULL?

	if ( !PyDict_Check(py_partition_filter)) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM,
						"invalid partition_filter policy, partition_filter must be a dict");
	}

	PyObject *begin = PyDict_GetItemString(py_partition_filter, "begin");
	PyObject *count = PyDict_GetItemString(py_partition_filter, "count");
	PyObject *digest = PyDict_GetItemString(py_partition_filter, "digest");
	PyObject *parts_stat = PyDict_GetItemString(py_partition_filter, "partition_status");

	if ( parts_stat && !PyDict_Check(parts_stat)) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM,
						"invalid partition_filter policy, partition_status must be a dict");
	}

	filter->begin = 0;
	if (begin && PyLong_Check(begin)) {

		long tmp_begin = PyLong_AsLong(begin);

		if (tmp_begin < CLUSTER_NPARTITIONS) {
			filter->begin = tmp_begin;
		}
	}

	filter->count = CLUSTER_NPARTITIONS;
	if (count && PyLong_Check(count)) {
		
		long tmp_count= PyLong_AsLong(count);

		if (tmp_count <= CLUSTER_NPARTITIONS) {
			filter->count = tmp_count;
		}
	}

	filter->digest.init = 0;
	if (digest && PyDict_Check(digest)) {

		// TODO check these for overflow
		PyObject *init = PyDict_GetItemString(digest, "init");
		if (init && PyLong_Check(init)) {
			filter->digest.init = PyInt_AsLong(init);
		}

		PyObject *value = PyDict_GetItemString(digest, "value");
		if (value && PyString_Check(value)) {
			strncpy((char *)filter->digest.value, PyString_AsString(value),
					AS_DIGEST_VALUE_SIZE);
		}
	}

	part_all = parts_setup(
							filter->begin, filter->count, //cluster->n_partitions, 
							&filter->digest);


	if (parts_stat && PyDict_Check(parts_stat)) {
		for (uint16_t i = 0; i < part_all->part_count; i++) {
			ps = &part_all->parts[i];

			PyObject *key = PyLong_FromLong(ps->part_id);
			PyObject *id = PyDict_GetItem(parts_stat, key);

			if (!id || !PyTuple_Check(id)) {
				as_log_info("invalid id for part_id: %d\n", ps->part_id);
				continue;
			}

			PyObject *init = PyTuple_GetItem(id, 1);
			if (init && PyLong_Check(init)) {
				ps->digest.init = PyInt_AsLong(init);
			} else {
				as_log_info("invalid init for part_id: %d\n", ps->part_id);
			}

			PyObject *done = PyTuple_GetItem(id, 2);
			if (done && PyLong_Check(done)) {
				ps->done = (bool) PyInt_AsLong(done);
			} else {
				as_log_info("invalid done for part_id: %d\n", ps->part_id);
			}
			
			PyObject *value = PyTuple_GetItem(id, 3);
			if (PyByteArray_Check(value)) {
				uint8_t *bytes_array = (uint8_t *)PyByteArray_AsString(value);
				//uint32_t bytes_array_len = (uint32_t)PyByteArray_Size(value);
				memcpy(ps->digest.value, bytes_array, AS_DIGEST_VALUE_SIZE);
			} else {
				as_log_info("invalid value for part_id: %d\n", ps->part_id);
			}
		}
	}

	if (part_all)
		*pss = part_all;

	return err->code;
}