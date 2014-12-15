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
 * LLIST OPERATIONS
 ******************************************************************************/

/**
 * Performs `add` operation. This will add an object
 * to the llist.
 *
 * llist.add(value)
 */
PyObject * AerospikeLList_Add(AerospikeLList * self, PyObject * args, PyObject * kwds);

/**
 * Performs `add_many` operation. This will add
 * a list of objects to the list.
 *
 * llist.add_many(values)
 */
PyObject * AerospikeLList_Add_Many(AerospikeLList * self, PyObject * args, PyObject * kwds);

/**
 * Performs `get` operation. This will get
 * an object from the list.
 *
 * llist.get(value)
 */
PyObject * AerospikeLList_Get(AerospikeLList * self, PyObject * args, PyObject * kwds);

/**
 * Performs `filter` operation. This will scan the list
 * and apply a predicate filter.
 *
 * llist.filter(udf_name, args)
 */
PyObject * AerospikeLList_Filter(AerospikeLList * self, PyObject * args, PyObject * kwds);

/**
 * Performs `destroy` operation. This will delete the entire
 * llist.
 *
 * llist.destroy()
 */
PyObject * AerospikeLList_Destroy(AerospikeLList * self, PyObject * args, PyObject * kwds);

/**
 * Performs `remove` operation. This will remove an object
 * from the list.
 *
 * llist.remove(element)
 */
PyObject * AerospikeLList_Remove(AerospikeLList * self, PyObject * args, PyObject * kwds);

/**
 * Performs `size` operation. This will get the current item
 * count of the list.
 *
 * llist.size()
 */
PyObject * AerospikeLList_Size(AerospikeLList * self, PyObject * args, PyObject * kwds);

/**
 * Performs `config` operation. This will get the configuration
 * parameters of the list.
 *
 * llist.config()
 */
PyObject * AerospikeLList_Config(AerospikeLList * self, PyObject * args, PyObject * kwds);
