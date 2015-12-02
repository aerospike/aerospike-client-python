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

#include <Python.h>
#include <structmember.h>
#include <stdbool.h>
#include <unistd.h>

#include "nullobject.h"

static PyObject * AerospikeNullObject_Type_New(PyTypeObject * parent, PyObject * args, PyObject * kwds);

static void AerospikeNullObject_Type_Dealloc(AerospikeNullObject * self)
{
	PyObject_Del(self);
}

/*******************************************************************************
 * PYTHON TYPE DESCRIPTOR
 ******************************************************************************/

static PyTypeObject AerospikeNullObject_Type = {
	PyObject_HEAD_INIT(NULL)

	 .ob_size			= 0,
	.tp_name			= "aerospike.null",
	.tp_basicsize		= sizeof(AerospikeNullObject),
	.tp_itemsize		= 0,
	.tp_dealloc			= (destructor) AerospikeNullObject_Type_Dealloc,
	.tp_print			= 0,
	.tp_getattr			= 0,
	.tp_setattr			= 0,
	.tp_compare			= 0,
	.tp_repr			= 0,
	.tp_as_number		= 0,
	.tp_as_sequence		= 0,
	.tp_as_mapping		= 0,
	.tp_hash			= 0,
	.tp_call			= 0,
	.tp_str				= 0,
	.tp_getattro		= 0,
	.tp_setattro		= 0,
	.tp_as_buffer		= 0,
	.tp_flags			= Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
	.tp_doc				=
		"The nullobject when used with put() works as a removebin()\n",
	.tp_traverse		= 0,
	.tp_clear			= 0,
	.tp_richcompare		= 0,
	.tp_weaklistoffset	= 0,
	.tp_iter			= 0,
	.tp_iternext		= 0,
	.tp_methods			= 0,
	.tp_members			= 0,
	.tp_getset			= 0,
	.tp_base			= 0,
	.tp_dict			= 0,
	.tp_descr_get		= 0,
	.tp_descr_set		= 0,
	.tp_dictoffset		= 0,
	.tp_init			= 0,
	.tp_alloc			= 0,
	.tp_new				= AerospikeNullObject_Type_New
};

static PyObject * AerospikeNullObject_Type_New(PyTypeObject * parent, PyObject * args, PyObject * kwds)
{
	return (PyObject *) PyObject_New(AerospikeNullObject, parent);
}

PyObject * AerospikeNullObject_New()
{
	return AerospikeNullObject_Type_New(&AerospikeNullObject_Type, Py_None, Py_None);
}
