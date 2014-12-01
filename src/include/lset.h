/*******************************************************************************
 * Copyright 2013-2014 Aerospike, Inc.
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

#include <aerospike/as_ldt.h>

#include "types.h"
#include "client.h"

/*******************************************************************************
 * FUNCTIONS
 ******************************************************************************/

PyTypeObject * AerospikeLSet_Ready(void);

AerospikeLSet * AerospikeLSet_New(AerospikeClient * client, PyObject * args, PyObject * kwds);

/*******************************************************************************
 * OPERATIONS
 ******************************************************************************/

/**
 * LSET : Add operation
 */
PyObject * AerospikeLSet_Add(AerospikeLSet * self, PyObject * args, PyObject * kwds);

/**
 * LSET : Add_all operation
 */
PyObject * AerospikeLSet_Add_All(AerospikeLSet * self, PyObject * args, PyObject * kwds);

/**
 * LSET : Get operation
 */
PyObject * AerospikeLSet_Get(AerospikeLSet * self, PyObject * args, PyObject * kwds);

PyObject * AerospikeLSet_Filter(AerospikeLSet * self, PyObject * args, PyObject * kwds);

PyObject * AerospikeLSet_Destroy(AerospikeLSet * self, PyObject * args, PyObject * kwds);

PyObject * AerospikeLSet_Exists(AerospikeLSet * self, PyObject * args, PyObject * kwds);

PyObject * AerospikeLSet_Remove(AerospikeLSet * self, PyObject * args, PyObject * kwds);

PyObject * AerospikeLSet_Size(AerospikeLSet * self, PyObject * args, PyObject * kwds);

PyObject * AerospikeLSet_Config(AerospikeLSet * self, PyObject * args, PyObject * kwds);
