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
#ifndef __COMMON_H__
#define __COMMON_H_

#include <Python.h>
#include <stdbool.h>
#include "aerospike/as_error.h"
#include "types.h"
typedef struct {
    as_error error;
    PyObject * callback;
}user_serializer_callback;
extern user_serializer_callback user_serializer_call_info, user_deserializer_call_info;
/**
 * Sets the serializer
 *
 *		client.set_serializer()
 *
 */

PyObject * AerospikeClient_Set_Serializer(AerospikeClient * self, PyObject * args, PyObject * kwds);
/**
 * Sets the deserializer
 *
 *		client.set_deserializer()
 *
 */
PyObject * AerospikeClient_Set_Deserializer(AerospikeClient * self, PyObject * args, PyObject * kwds);
#endif
