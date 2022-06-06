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

#include <Python.h>
#include <structmember.h>
#include <stdbool.h>
#include <unistd.h>

#include "nullobject.h"

static PyObject *AerospikeNullObject_Type_New(PyTypeObject *parent,
											  PyObject *args, PyObject *kwds);

static void AerospikeNullObject_Type_Dealloc(AerospikeNullObject *self)
{
	PyObject_Del(self);
}

/*******************************************************************************
 * PYTHON TYPE DESCRIPTOR
 ******************************************************************************/

static PyTypeObject AerospikeNullObject_Type = {
	PyVarObject_HEAD_INIT(NULL, 0) 
	.tp_name = "aerospike.null",
	.tp_basicsize = sizeof(AerospikeNullObject),
	.tp_dealloc = (destructor)AerospikeNullObject_Type_Dealloc,
	.tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
	.tp_doc = "The nullobject when used with put() works as a removebin()\n",
	.tp_new = AerospikeNullObject_Type_New
};

static PyObject *AerospikeNullObject_Type_New(PyTypeObject *parent,
											  PyObject *args, PyObject *kwds)
{
	return (PyObject *)PyObject_New(AerospikeNullObject, parent);
}

PyTypeObject *AerospikeNullObject_Ready()
{
	return PyType_Ready(&AerospikeNullObject_Type) == 0
			   ? &AerospikeNullObject_Type
			   : NULL;
}
