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

PyTypeObject * AerospikeLList_Ready(void);

AerospikeLList * AerospikeLList_New(AerospikeClient * client, PyObject * args, PyObject * kwds);

/*******************************************************************************
 * OPERATIONS
 ******************************************************************************/

/**
 * LList : Add operation
 */
PyObject * AerospikeLList_Add(AerospikeLList * self, PyObject * args, PyObject * kwds);

/**
 * LList : Add_all operation
 */
PyObject * AerospikeLList_Add_All(AerospikeLList * self, PyObject * args, PyObject * kwds);

/**
 * LList : Get operation
 */
PyObject * AerospikeLList_Get(AerospikeLList * self, PyObject * args, PyObject * kwds);

PyObject * AerospikeLList_Filter(AerospikeLList * self, PyObject * args, PyObject * kwds);

PyObject * AerospikeLList_Destroy(AerospikeLList * self, PyObject * args, PyObject * kwds);

PyObject * AerospikeLList_Remove(AerospikeLList * self, PyObject * args, PyObject * kwds);

PyObject * AerospikeLList_Size(AerospikeLList * self, PyObject * args, PyObject * kwds);

PyObject * AerospikeLList_Config(AerospikeLList * self, PyObject * args, PyObject * kwds);
