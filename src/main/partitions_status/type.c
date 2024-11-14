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
// TODO: Don't need to include all types
#include "types.h"
#include "partitions_status.h"

// Partition status object

PyObject *AerospikePartitionStatusObject_Type_New(PyTypeObject *type,
                                                  PyObject *args,
                                                  PyObject *kwds)
{
    AerospikePartitionStatusObject *self =
        (AerospikePartitionStatusObject *)type->tp_alloc(type, 0);
    if (self == NULL) {
        return NULL;
    }
    return (PyObject *)self;
}

// TODO: make sure memory garbage collection works as intended
static void AerospikePartitionStatusObject_Type_Dealloc(
    AerospikePartitionStatusObject *self)
{
    Py_TYPE(self)->tp_free((PyObject *)self);
}

static PyObject *AerospikePartitionStatus__getitem__(PyObject *self,
                                                     PyObject *py_key)
{
    // TODO: py_key is always non-NULL?
    if (!PyUnicode_Check(py_key)) {
        PyErr_SetString(PyExc_TypeError, "Key must be a string type");
        return NULL;
    }

    const char *key = PyUnicode_AsUTF8(py_key);
    if (!key) {
        return NULL;
    }
    AerospikePartitionStatusObject *py_partition_status =
        (AerospikePartitionStatusObject *)self;
    if (!strcmp(key, "bval")) {
        uint64_t bval = py_partition_status->part_status->bval;
        PyObject *py_bval = PyLong_FromUnsignedLongLong((uint64_t)bval);
        if (!py_bval) {
            return NULL;
        }
        return py_bval;
    }
    // TODO: remove
    return NULL;
}

static PyMethodDef AerospikePartitionStatus_Type_Methods[] = {
    {.ml_name = "__getitem__",
     .ml_meth = AerospikePartitionStatus__getitem__,
     .ml_flags = METH_O},
    {NULL}};

PyTypeObject AerospikePartitionStatusObject_Type = {
    PyVarObject_HEAD_INIT(NULL, 0).tp_name =
        FULLY_QUALIFIED_TYPE_NAME("PartitionStatus"),
    .tp_basicsize = sizeof(AerospikePartitionStatusObject),
    .tp_dealloc = (destructor)AerospikePartitionStatusObject_Type_Dealloc,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
    .tp_new = AerospikePartitionStatusObject_Type_New,
    .tp_methods = AerospikePartitionStatus_Type_Methods};

static PyObject *
create_py_partition_status_object(as_partition_status *part_status)
{
    AerospikePartitionStatusObject *py_part_status =
        (AerospikePartitionStatusObject *)PyObject_CallObject(
            (PyObject *)&AerospikePartitionStatusObject_Type, NULL);
    if (py_part_status == NULL) {
        return NULL;
    }

    py_part_status->part_status = part_status;
    return (PyObject *)py_part_status;
}

// Partitions status object

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

// We don't want the user to define a PartitionsStatus object in the public API
// This object should only be created internally by an API method
// parts_all may be NULL if the partitions status isn't being tracked by the C client
// Returns Optional[aerospike.PartitionsStatus] or sets as_error on error
PyObject *create_py_partitions_status_object(as_error *err,
                                             as_partitions_status *parts_all)
{
    if (parts_all == NULL) {
        Py_RETURN_NONE;
    }

    AerospikePartitionsStatusObject *py_parts_all =
        (AerospikePartitionsStatusObject *)PyObject_CallObject(
            (PyObject *)&AerospikePartitionsStatusObject_Type, NULL);
    if (py_parts_all == NULL) {
        PyErr_Clear();
        as_error_update(
            err, AEROSPIKE_ERR_CLIENT,
            "Unable to create new aerospike.PartitionsStatus object");
        return NULL;
    }

    parts_all = as_partitions_status_reserve(parts_all);
    py_parts_all->parts_all = parts_all;
    return (PyObject *)py_parts_all;
}

static PyObject *AerospikePartitionsStatus__getitem__(PyObject *self,
                                                      PyObject *py_key)
{
    // assume py_key is non-NULL
    bool is_valid_key_type = PyUnicode_Check(py_key) || PyLong_Check(py_key);
    if (!is_valid_key_type) {
        PyErr_SetString(PyExc_TypeError, "Key is an invalid type");
        return NULL;
    }

    AerospikePartitionsStatusObject *py_partitions_status =
        (AerospikePartitionsStatusObject *)self;
    if (PyUnicode_Check(py_key)) {
        const char *key = PyUnicode_AsUTF8(py_key);
        if (!key) {
            return NULL;
        }
        if (!strcmp(key, "retry")) {
            bool retry = py_partitions_status->parts_all->retry;
            PyObject *py_retry = PyBool_FromLong(retry);
            if (!py_retry) {
                return NULL;
            }
            return py_retry;
        }
    }
    else if (PyLong_Check(py_key)) {
        unsigned long partition_id = PyLong_AsUnsignedLong(py_key);
        as_partition_status *part_status =
            &py_partitions_status->parts_all->parts[partition_id];
        PyObject *py_partition_status =
            create_py_partition_status_object(part_status);
        if (py_partition_status == NULL) {
            return NULL;
        }
        return py_partition_status;
    }

    // TODO: remove
    Py_RETURN_NONE;
}

static PyMethodDef AerospikePartitionsStatus_Type_Methods[] = {
    {.ml_name = "__getitem__",
     .ml_meth = AerospikePartitionsStatus__getitem__,
     .ml_flags = METH_O},
    {NULL}};

PyTypeObject AerospikePartitionsStatusObject_Type = {
    PyVarObject_HEAD_INIT(NULL, 0).tp_name =
        FULLY_QUALIFIED_TYPE_NAME("PartitionsStatus"),
    .tp_basicsize = sizeof(AerospikePartitionsStatusObject),
    .tp_dealloc = (destructor)AerospikePartitionsStatusObject_Type_Dealloc,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
    .tp_new = AerospikePartitionsStatusObject_Type_New,
    .tp_methods = AerospikePartitionsStatus_Type_Methods};

PyTypeObject *AerospikePartitionsStatusObject_Ready()
{
    return PyType_Ready(&AerospikePartitionsStatusObject_Type) == 0
               ? &AerospikePartitionsStatusObject_Type
               : NULL;
}
