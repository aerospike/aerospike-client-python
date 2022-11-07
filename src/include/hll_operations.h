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
#include <aerospike/as_error.h>
#include <aerospike/as_vector.h>
#include "types.h"

as_status add_new_hll_op(AerospikeClient *self, as_error *err,
						 PyObject *op_dict, as_vector *unicodeStrVector,
						 as_static_pool *static_pool, as_operations *ops,
						 long operation_code, long *ret_type,
						 int serializer_type);
