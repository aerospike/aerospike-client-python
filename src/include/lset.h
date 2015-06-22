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

PyTypeObject * AerospikeLSet_Ready(void);

AerospikeLSet * AerospikeLSet_New(AerospikeClient * client, PyObject * args, PyObject * kwds);

/*******************************************************************************
 * LSET OPERATIONS
 ******************************************************************************/

/**
 * Performs `add` operation. This will add an object
 * to the lset.
 *
 * lset.add(value)
 */
PyObject * AerospikeLSet_Add(AerospikeLSet * self, PyObject * args, PyObject * kwds);

/**
 * Performs `add_many` operation. This will add
 * a list of objects to the set.
 *
 * lset.add_many(values)
 */
PyObject * AerospikeLSet_Add_Many(AerospikeLSet * self, PyObject * args, PyObject * kwds);

/**
 * Performs `get` operation. This will get
 * an object from the set.
 *
 * lset.get(value)
 */
PyObject * AerospikeLSet_Get(AerospikeLSet * self, PyObject * args, PyObject * kwds);

/**
 * Performs `filter` operation. This will scan the set
 * and apply a predicate filter.
 *
 * lset.filter(udf_name, args)
 */
PyObject * AerospikeLSet_Filter(AerospikeLSet * self, PyObject * args, PyObject * kwds);

/**
 * Performs `destroy` operation. This will delete the entire
 * lset.
 *
 * lset.destroy()
 */
PyObject * AerospikeLSet_Destroy(AerospikeLSet * self, PyObject * args, PyObject * kwds);

/**
 * Performs `exists` operation. Test existence of an object
 * in the lset.
 *
 * lset.exists(element)
 */
PyObject * AerospikeLSet_Exists(AerospikeLSet * self, PyObject * args, PyObject * kwds);

/**
 * Performs `remove` operation. This will remove an object
 * from the set.
 *
 * lset.remove(element)
 */
PyObject * AerospikeLSet_Remove(AerospikeLSet * self, PyObject * args, PyObject * kwds);

/**
 * Performs `size` operation. This will get the current item
 * count of the set.
 *
 * lset.size()
 */
PyObject * AerospikeLSet_Size(AerospikeLSet * self, PyObject * args, PyObject * kwds);


/**
 * Performs `config` operation. This will get the configuration
 * parameters of the set.
 *
 * lset.config()
 */
PyObject * AerospikeLSet_Config(AerospikeLSet * self, PyObject * args, PyObject * kwds);
