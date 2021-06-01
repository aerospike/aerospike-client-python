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
#include <aerospike/as_vector.h>
#include <aerospike/as_geojson.h>
#include <aerospike/as_msgpack_ext.h>

#include "client.h"
#include "conversions.h"
#include "serializer.h"
#include "exceptions.h"
#include "policy.h"
#include "cdt_operation_utils.h"
#include "geo.h"
#include "cdt_types.h"

/*
* convert_partition_filter
* Converts partition filter from python object into partition_filter struct.
* Initiates the conversion from intermediate_partition_filter structs to partition_filter.
* builds the partition filter.
*/
as_status convert_partition_filter(AerospikeClient * self, PyObject * py_partition_filter, as_partition_filter * filter, as_error * err) {
	
	PyObject * begin = PyDict_GetItemString(py_partition_filter, "begin");
	PyObject * count = PyDict_GetItemString(py_partition_filter, "count");
	PyObject * digest = PyDict_GetItemString(py_partition_filter, "digest");

	if (begin && PyInt_Check(begin)) {
		filter->begin = 0;
		filter->count = 0;
		filter->digest.init = 0;
	
		filter->begin = PyInt_AsLong(begin);

		if (count && PyInt_Check(count)) {
			filter->count = PyInt_AsLong(count);
		}

		if (digest && PyDict_Check(digest)) {
			PyObject * init = PyDict_GetItemString(digest, "init");
			if (init && PyInt_Check(init)) {
				filter->digest.init = PyInt_AsLong(init);
			}
			PyObject * value = PyDict_GetItemString(digest, "value");
			if (value && PyString_Check(value)) {
				strncpy((char*)filter->digest.value, PyString_AsString(value), AS_DIGEST_VALUE_SIZE);
			}
		}
	} else {
		as_error_update(err, AEROSPIKE_ERR_PARAM, "Invalid scan partition policy");
	}

	return err->code;
}
