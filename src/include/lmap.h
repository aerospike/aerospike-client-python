/*******************************************************************************
 * Copyright 2013-2015 Aerospike, Inc.
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
 * LMAP OPERATIONS
 ******************************************************************************/

/**
 * Performs `put` operation. This will put an object
 * to the lmap.
 *
 * lmap.put(key, value)
 */
PyObject * AerospikeLMap_Put(AerospikeLMap * self, PyObject * args, PyObject * kwds);

/**
 * Performs `put_many` operation. This will put a map containing
 * values to put to the lmap.
 *
 * llist.put_many(values)
 */
PyObject * AerospikeLMap_Put_Many(AerospikeLMap * self, PyObject * args, PyObject * kwds);

/**
 * Performs `get` operation. This will get
 * an object from the map.
 *
 * lmap.get(key)
 */
PyObject * AerospikeLMap_Get(AerospikeLMap * self, PyObject * args, PyObject * kwds);

/**
 * Performs `filter` operation. This will scan the map
 * and apply a predicate filter.
 *
 * lmap.filter(udf_name, args)
 */
PyObject * AerospikeLMap_Filter(AerospikeLMap * self, PyObject * args, PyObject * kwds);

/**
 * Performs `destroy` operation. This will delete the entire
 * lmap.
 *
 * lmap.destroy()
 */
PyObject * AerospikeLMap_Destroy(AerospikeLMap * self, PyObject * args, PyObject * kwds);

/**
 * Performs `remove` operation. This will remove an object
 * from the map.
 *
 * lmap.remove(key)
 */
PyObject * AerospikeLMap_Remove(AerospikeLMap * self, PyObject * args, PyObject * kwds);

/**
 * Performs `size` operation. This will get the current item
 * count of the map.
 *
 * lmap.size()
 */
PyObject * AerospikeLMap_Size(AerospikeLMap * self, PyObject * args, PyObject * kwds);

/**
 * Performs `config` operation. This will get the configuration
 * parameters of the map.
 *
 * lmap.config()
 */
PyObject * AerospikeLMap_Config(AerospikeLMap * self, PyObject * args, PyObject * kwds);
