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
#ifndef __COMMON_H__
#define __COMMON_H__

#include <Python.h>
#include <stdbool.h>
#include "aerospike/as_error.h"
#include "types.h"
/*typedef struct {
    as_error error;
    PyObject * callback;
}user_serializer_callback;*/

/**
 * Sets the serializer
 *
 *		client.set_serializer()
 *
 */

PyObject *AerospikeClient_Set_Serializer(AerospikeClient *self, PyObject *args,
										 PyObject *kwds);
/**
 * Sets the deserializer
 *
 *		client.set_deserializer()
 *
 */
PyObject *AerospikeClient_Set_Deserializer(AerospikeClient *self,
										   PyObject *args, PyObject *kwds);

/**
 * Serializes Py_Object (value) into as_bytes using serialization logic
 * based on serializer_policy.
 */
PyObject *AerospikeClient_Unset_Serializers(AerospikeClient *self,
											PyObject *args, PyObject *kwds);
/**
 * Unsets the serializer and deserializer
 *
 *		client.unset_serializers()
 *
 */
extern as_status serialize_based_on_serializer_policy(AerospikeClient *self,
													  int32_t serializer_policy,
													  as_bytes **bytes,
													  PyObject *value,
													  as_error *error_p);

/**
 * Deserializes Py_Object (value) into as_bytes using Deserialization logic
 * based on serializer_policy.
 */
extern as_status deserialize_based_on_as_bytes_type(AerospikeClient *self,
													as_bytes *bytes,
													PyObject **retval,
													as_error *error_p);
#endif
