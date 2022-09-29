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

#include <aerospike/aerospike.h>
#include <aerospike/as_config.h>
#include <aerospike/as_error.h>
#include <aerospike/as_policy.h>

#include "admin.h"
#include "client.h"
#include "policy.h"
#include "conversions.h"
#include "exceptions.h"
#include "global_hosts.h"

static PyObject *AerospikeGlobalHosts_Type_New(PyTypeObject *type,
											   PyObject *args, PyObject *kwds)
{
	AerospikeGlobalHosts *self = NULL;

	self = (AerospikeGlobalHosts *)PyObject_New(AerospikeGlobalHosts, type);

	return (PyObject *)self;
}

static void AerospikeGlobalHosts_Type_Dealloc(PyObject *self)
{
	PyObject_Del(self);
}

/*******************************************************************************
 * PYTHON TYPE DESCRIPTOR
 ******************************************************************************/

static PyTypeObject AerospikeGlobalHosts_Type = {
	PyVarObject_HEAD_INIT(NULL, 0) 0, // tp_name
	sizeof(AerospikeGlobalHosts),	  // tp_basicsize
	0,								  // tp_itemsize
	(destructor)AerospikeGlobalHosts_Type_Dealloc,
	// tp_dealloc
	0, // tp_print
	0, // tp_getattr
	0, // tp_setattr
	0, // tp_compare
	0, // tp_repr
	0, // tp_as_number
	0, // tp_as_sequence
	0, // tp_as_mapping
	0, // tp_hash
	0, // tp_call
	0, // tp_str
	0, // tp_getattro
	0, // tp_setattro
	0, // tp_as_buffer
	Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
	// tp_flags
	"The Global Host stores the persistent objects\n",
	// tp_doc
	0,							   // tp_traverse
	0,							   // tp_clear
	0,							   // tp_richcompare
	0,							   // tp_weaklistoffset
	0,							   // tp_iter
	0,							   // tp_iternext
	0,							   // tp_methods
	0,							   // tp_members
	0,							   // tp_getset
	0,							   // tp_base
	0,							   // tp_dict
	0,							   // tp_descr_get
	0,							   // tp_descr_set
	0,							   // tp_dictoffset
	0,							   // tp_init
	0,							   // tp_alloc
	AerospikeGlobalHosts_Type_New, // tp_new
	0,							   // tp_free
	0,							   // tp_is_gc
	0							   // tp_bases
};

AerospikeGlobalHosts *AerospikeGobalHosts_New(aerospike *as)
{
	AerospikeGlobalHosts *self =
		(AerospikeGlobalHosts *)AerospikeGlobalHosts_Type.tp_new(
			&AerospikeGlobalHosts_Type, Py_None, Py_None);
	self->as = as;
	self->shm_key = as->config.shm_key;
	self->ref_cnt = 1;
	Py_INCREF((PyObject *)self);
	return self;
}

void AerospikeGlobalHosts_Del(PyObject *self)
{
	AerospikeGlobalHosts_Type_Dealloc(self);
}
