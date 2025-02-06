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

PyObject *AerospikeException_New(void);
void raise_exception(as_error *err);
void raise_exception_with_status(as_error *err, PyObject *py_status,
                                 bool is_commit_status);
PyObject *raise_exception_old(as_error *err);
void remove_exception(as_error *err);
void set_aerospike_exc_attrs_using_tuple_of_attrs(PyObject *py_exc,
                                                  PyObject *py_tuple);
void set_aerospike_exc_attrs_using_tuple_of_attrs_and_mrt_statuses(
    PyObject *py_exc, PyObject *py_tuple, PyObject *py_commit_status,
    PyObject *py_abort_status);
