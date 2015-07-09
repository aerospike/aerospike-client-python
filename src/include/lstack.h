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

PyTypeObject * AerospikeLStack_Ready(void);

AerospikeLStack * AerospikeLStack_New(AerospikeClient * client, PyObject * args, PyObject * kwds);

/*******************************************************************************
 * LSTACK OPERATIONS
 ******************************************************************************/

/**
 * Performs `push` operation. This will push an object
 * onto the lstack.
 *
 * lstack.push(value)
 */
PyObject * AerospikeLStack_Push(AerospikeLStack * self, PyObject * args, PyObject * kwds);

/**
 * Performs `push_many` operation. This will push
 * a list of objects on the stack.
 *
 * lstack.push_many(values)
 */
PyObject * AerospikeLStack_Push_Many(AerospikeLStack * self, PyObject * args, PyObject * kwds);

/**
 * Performs `peek` operation. This will fetch the top
 * N elements from the stack.
 *
 * lstack.peek(peek_count)
 */
PyObject * AerospikeLStack_Peek(AerospikeLStack * self, PyObject * args, PyObject * kwds);

/**
 * Performs `filter` operation. This will scan the stack
 * and apply a predicate filter.
 *
 * lstack.filter(udf_name, args)
 */
PyObject * AerospikeLStack_Filter(AerospikeLStack * self, PyObject * args, PyObject * kwds);

/**
 * Performs `destroy` operation. This will delete the entire
 * lstack.
 *
 * lstack.destroy()
 */
PyObject * AerospikeLStack_Destroy(AerospikeLStack * self, PyObject * args, PyObject * kwds);

/**
 * Performs `get_capacity` operation. This will get the current
 * capacity limit setting.
 *
 * lstack.get_capacity()
 */
PyObject * AerospikeLStack_Get_Capacity(AerospikeLStack * self, PyObject * args, PyObject * kwds);

/**
 * Performs `set_capacity` operation. This will set the max
 * capacity for the lstack.
 *
 * lstack.set_capacity(capacity)
 */
PyObject * AerospikeLStack_Set_Capacity(AerospikeLStack * self, PyObject * args, PyObject * kwds);

/**
 * Performs `size` operation. This will get the current item
 * count of the stack.
 *
 * lstack.size()
 */
PyObject * AerospikeLStack_Size(AerospikeLStack * self, PyObject * args, PyObject * kwds);

/**
 * Performs `config` operation. This will get the configuration
 * parameters of the stack.
 *
 * lstack.config()
 */
PyObject * AerospikeLStack_Config(AerospikeLStack * self, PyObject * args, PyObject * kwds);
