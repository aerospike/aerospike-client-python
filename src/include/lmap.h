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

PyTypeObject * AerospikeLMap_Ready(void);

AerospikeLMap * AerospikeLMap_New(AerospikeClient * client, PyObject * args, PyObject * kwds);

/*******************************************************************************
 * OPERATIONS
 ******************************************************************************/

/**
 * LMAP : Add operation
 */
PyObject * AerospikeLMap_Add(AerospikeLMap * self, PyObject * args, PyObject * kwds);

/**
 * LMAP : Add_all operation
 */
PyObject * AerospikeLMap_Add_All(AerospikeLMap * self, PyObject * args, PyObject * kwds);

/**
 * LMAP : Get operation
 */
PyObject * AerospikeLMap_Get(AerospikeLMap * self, PyObject * args, PyObject * kwds);

PyObject * AerospikeLMap_Filter(AerospikeLMap * self, PyObject * args, PyObject * kwds);

PyObject * AerospikeLMap_Destroy(AerospikeLMap * self, PyObject * args, PyObject * kwds);

PyObject * AerospikeLMap_Remove(AerospikeLMap * self, PyObject * args, PyObject * kwds);

PyObject * AerospikeLMap_Size(AerospikeLMap * self, PyObject * args, PyObject * kwds);

PyObject * AerospikeLMap_Config(AerospikeLMap * self, PyObject * args, PyObject * kwds);
