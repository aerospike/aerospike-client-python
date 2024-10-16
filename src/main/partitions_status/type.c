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
#include "partitions_status.h"

#include "nullobject.h"

/*******************************************************************************
 * PYTHON TYPE DESCRIPTOR
 ******************************************************************************/

static void AerospikePartitionsStatusObject_Type_Dealloc(
    AerospikePartitionsStatusObject *self)
{
    if (self->parts_all != NULL) {
        as_partitions_status_release(self->parts_all);
    }
    Py_TYPE(self)->tp_free((PyObject *)self);
}

PyObject *AerospikePartitionsStatusObject_Type_New(PyTypeObject *type,
                                                   PyObject *args,
                                                   PyObject *kwds)
{
    AerospikePartitionsStatusObject *self =
        (AerospikePartitionsStatusObject *)type->tp_alloc(type, 0);
    if (self == NULL) {
        return NULL;
    }
    return (PyObject *)self;
}

PyObject *create_py_partitions_status_object(as_partitions_status *parts_all)
{
    if (parts_all == NULL) {
        return NULL;
    }
    parts_all = as_partitions_status_reserve(parts_all);
    AerospikePartitionsStatusObject *py_parts_all = PyObject_New(
        AerospikePartitionsStatusObject, &AerospikePartitionsStatusObject_Type);
    py_parts_all->parts_all = parts_all;
    return py_parts_all;
}

PyTypeObject AerospikePartitionsStatusObject_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
        FULLY_QUALIFIED_TYPE_NAME("PartitionsStatus"), // tp_name
    sizeof(AerospikePartitionsStatusObject),           // tp_basicsize
    0,                                                 // tp_itemsize
    (destructor)AerospikePartitionsStatusObject_Type_Dealloc,
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
    "The nullobject when used with put() works as a removebin()\n",
    // tp_doc
    0,                                        // tp_traverse
    0,                                        // tp_clear
    0,                                        // tp_richcompare
    0,                                        // tp_weaklistoffset
    0,                                        // tp_iter
    0,                                        // tp_iternext
    0,                                        // tp_methods
    0,                                        // tp_members
    0,                                        // tp_getset
    0,                                        // tp_base
    0,                                        // tp_dict
    0,                                        // tp_descr_get
    0,                                        // tp_descr_set
    0,                                        // tp_dictoffset
    0,                                        // tp_init
    0,                                        // tp_alloc
    AerospikePartitionsStatusObject_Type_New, // tp_new
    0,                                        // tp_free
    0,                                        // tp_is_gc
    0                                         // tp_bases
};

PyTypeObject *AerospikePartitionsStatusObject_Ready()
{
    return PyType_Ready(&AerospikePartitionsStatusObject_Type) == 0
               ? &AerospikePartitionsStatusObject_Type
               : NULL;
}
