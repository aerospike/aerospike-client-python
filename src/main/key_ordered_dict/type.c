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

#include "key_ordered_dict.h"

/*******************************************************************************
 * PYTHON TYPE METHODS
 ******************************************************************************/

static PyMethodDef AerospikeKeyOrderedDict_Type_Methods[] = {{NULL}};

/*******************************************************************************
 * PYTHON TYPE HOOKS
 ******************************************************************************/

static int AerospikeKeyOrderedDict_Type_Init(AerospikeQuery *self,
											 PyObject *args, PyObject *kwds)
{
	return PyDict_Type.tp_init((PyObject *)self, args, kwds);
}

/*******************************************************************************
 * PYTHON TYPE DESCRIPTOR
 ******************************************************************************/

static PyTypeObject AerospikeKeyOrderedDict_Type = {
	PyVarObject_HEAD_INIT(NULL, 0).tp_name = "aerospike.KeyOrderedDict",
	.tp_basicsize = sizeof(AerospikeKeyOrderedDict),
	.tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
	.tp_doc = "The KeyOrderedDict class is a dictionary that directly maps\n"
			  "to a key ordered map on the Aerospike server.\n"
			  "This assists in matching key ordered maps\n"
			  "through various read operations.\n",
	.tp_methods = AerospikeKeyOrderedDict_Type_Methods,
	.tp_init = (initproc)AerospikeKeyOrderedDict_Type_Init};

PyTypeObject *AerospikeKeyOrderedDict_Ready()
{
	AerospikeKeyOrderedDict_Type.tp_base = &PyDict_Type;
	return PyType_Ready(&AerospikeKeyOrderedDict_Type) == 0
			   ? &AerospikeKeyOrderedDict_Type
			   : NULL;
}

PyObject *AerospikeKeyOrderedDict_Get_Type()
{
	return (PyObject *)&AerospikeKeyOrderedDict_Type;
}