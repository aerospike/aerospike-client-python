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
* convert_batchrecords
* Converts a Python BatchRecords object into a C client as_batch_records struct
*/
as_status convert_exp_list(AerospikeClient *self, PyObject *py_obj, as_error *err)
{
	if (!py_obj) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT,
							   "py_obj value is null");
	}

    char *FIELD_NAME_BATCH_RECORDS = "batch_records";
    PyObject *py_batch_records = PyObject_GetAttrString(py_obj, FIELD_NAME_BATCH_RECORDS);

    Py_ssize_t py_batch_records_size = PyList_Size(py_batch_records);
    as_batch_records batch_records = as_batch_records

    for (Py_ssize_t i = 0; i < batch_records; i++) {

    }

}