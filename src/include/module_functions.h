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
#include "types.h"

/**
 * Calculates the digest of a key
 *
 *		aerospike.calc_digest()
 *
 */
PyObject *Aerospike_Calc_Digest(PyObject *self, PyObject *args, PyObject *kwds);

/**
 * Get partition ID for given digest
 *
 *		aerospike.get_partition_id(digest)
 *
 */
PyObject *Aerospike_Get_Partition_Id(PyObject *self, PyObject *args);

/**
 * check whether async supported or not
 *
 *		aerospike.is_async_supoorted()
 *
 */
PyObject *Aerospike_Is_AsyncSupported(PyObject *self);

/*******************************************************************************
 * Aerospike initialization
 ******************************************************************************/

PyObject *AerospikeInitAsync(PyObject *self, PyObject *args, PyObject *kwds);

