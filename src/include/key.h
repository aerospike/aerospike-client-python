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

#include <aerospike/as_key.h>

#include "types.h"
#include "client.h"

/*******************************************************************************
 * FUNCTIONS
 ******************************************************************************/

PyTypeObject * AerospikeKey_Ready(void);

AerospikeKey * AerospikeKey_New(AerospikeClient * client, PyObject * args, PyObject * kwds);

/*******************************************************************************
 * OPERATIONS
 ******************************************************************************/

/**
 * Performs a `remove` operation. This will remove the record with the
 * specified key.
 *
 *		client.key(ns,set,key).apply(module, function, arglist)
 *
 */
PyObject * AerospikeKey_Apply(AerospikeKey * self, PyObject * args, PyObject * kwds);

/**
 * Performs a `exists` operation. This will check the existence of the record
 * with the specified key.
 *
 *		client.key(ns,set,key).exists()
 *
 */
PyObject * AerospikeKey_Exists(AerospikeKey * self, PyObject * args, PyObject * kwds);

/**
 * Performs a `get` operation. This will read a record with the specified key.
 *
 *		client.key(ns,set,key).get()
 *
 */
PyObject * AerospikeKey_Get(AerospikeKey * self, PyObject * args, PyObject * kwds);

/**
 * Performs a `select` operation. This will select specified bins of
 * the requested record.
 *
 *		client.key(ns,set,key).select("a","b","c")
 */
// PyObject * AerospikeKey_Select(AerospikeKey * self, PyObject * args, PyObject * kwds);

/**
 * Performs a `put` operation. This will select specified bins of the
 * requested record.
 *
 *		client.key(ns,set,key).put({
 *			"a": 123,
 *			"b": "xyz",
 *			"c": [1,2,3]
 *		})
 *
 */
PyObject * AerospikeKey_Put(AerospikeKey * self, PyObject * args, PyObject * kwds);

/**
 * Performs a `remove` operation. This will remove the record with the
 * specified key.
 *
 *		client.key(ns,set,key).remove()
 *
 */
PyObject * AerospikeKey_Remove(AerospikeKey * self, PyObject * args, PyObject * kwds);
