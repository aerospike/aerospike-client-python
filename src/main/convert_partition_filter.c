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

#include "client.h"
#include "conversions.h"

extern as_partitions_status*
parts_create(uint16_t part_begin, uint16_t part_count, const as_digest* digest);

/*
* convert_partition_filter
* Converts partition filter from python object into partition_filter struct.
* Initiates the conversion from intermediate_partition_filter structs to partition_filter.
* builds the partition filter.
*/
as_status convert_partition_filter(AerospikeClient *self,
								   PyObject *py_partition_filter,
								   as_partition_filter *filter, 
								   as_partitions_status **ps,
								   as_error *err)
{

	PyObject *begin = PyDict_GetItemString(py_partition_filter, "begin");
	PyObject *count = PyDict_GetItemString(py_partition_filter, "count");
	PyObject *digest = PyDict_GetItemString(py_partition_filter, "digest");
	PyObject *parts_stat = PyDict_GetItemString(py_partition_filter, "partition_status");
	int parts_valid = 0;
	as_partitions_status *part_all = NULL;

	if (parts_stat && PyDict_Check(parts_stat)) {
		parts_valid = 1;
	}

	if (begin && PyLong_Check(begin)) {
		filter->begin = 0;
		filter->count = 0;
		filter->digest.init = 0;

		filter->begin = PyInt_AsLong(begin);

		if (count && PyLong_Check(count)) {
			filter->count = PyInt_AsLong(count);
		}

		if (digest && PyDict_Check(digest)) {
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
		part_all = parts_create(
								filter->begin, filter->count, //cluster->n_partitions, 
								&filter->digest);
		part_all->part_begin = filter->begin;
		part_all->part_count = filter->count;

		for (uint16_t i = 0; i < part_all->part_count; i++) {
			as_partition_status* ps = &part_all->parts[i];
			ps->part_id = filter->begin + i;
			ps->done = false;
			ps->digest.init = false;

			if (!parts_valid) continue;

			PyObject *key = PyLong_FromLong(ps->part_id);
			PyObject *id = PyDict_GetItem(parts_stat, key);

			if (!id || !PyTuple_Check(id)) continue;

			PyObject *init = PyTuple_GetItem(id, 1);
			if (init && PyLong_Check(init)) {
				ps->digest.init = PyInt_AsLong(init);
			}
			PyObject *done = PyTuple_GetItem(id, 2);
			if (done && PyLong_Check(done)) {
				ps->done = (bool) PyInt_AsLong(done);
			}
			PyObject *value = PyTuple_GetItem(id, 3);
			if (value && PyString_Check(value)) {
				strncpy((char *)ps->digest.value, PyString_AsString(value),
						AS_DIGEST_VALUE_SIZE);
			}
		}
	}
	else {
		as_error_update(err, AEROSPIKE_ERR_PARAM,
						"Invalid scan partition policy");
	}

	if (part_all)
		*ps = part_all;

	return err->code;
}
