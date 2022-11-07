/*******************************************************************************
 * Copyright 2013-2021 Aerospike, Inc.
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

#pragma once

#include <Python.h>
#include <stdbool.h>

#include "types.h"
#include "client.h"

/*******************************************************************************
 * FUNCTIONS
 ******************************************************************************/

PyTypeObject *AerospikeGeospatial_Ready(void);

PyObject *AerospikeGeospatial_Wrap(AerospikeGeospatial *self, PyObject *args,
								   PyObject *kwds);

PyObject *AerospikeGeospatial_Unwrap(AerospikeGeospatial *self, PyObject *args,
									 PyObject *kwds);

PyObject *AerospikeGeospatial_Loads(AerospikeGeospatial *self, PyObject *args,
									PyObject *kwds);

PyObject *AerospikeGeospatial_Dumps(AerospikeGeospatial *self, PyObject *args,
									PyObject *kwds);

void store_geodata(AerospikeGeospatial *self, as_error *err,
				   PyObject *py_geodata);

PyObject *AerospikeGeospatial_DoDumps(PyObject *geo_data, as_error *err);

PyObject *AerospikeGeospatial_DoLoads(PyObject *py_geodata, as_error *err);

AerospikeGeospatial *Aerospike_Set_Geo_Data(PyObject *parent, PyObject *args,
											PyObject *kwds);

AerospikeGeospatial *Aerospike_Set_Geo_Json(PyObject *parent, PyObject *args,
											PyObject *kwds);

PyObject *AerospikeGeospatial_New(as_error *err, PyObject *value);
