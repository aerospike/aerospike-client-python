/*******************************************************************************
 * Copyright 2013-2019 Aerospike, Inc.
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
#include <stdbool.h>

#include <aerospike/aerospike_index.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/as_error.h>
#include <aerospike/as_predexp.h>

#include "client.h"
#include "conversions.h"
#include "exceptions.h"

/**********
* TODO
* create function that parses each pr4d from list of preds
* it then calls apropriate C macro
***********/

as_status convert_predexp_list(PyObject* py_predexp_list, as_predexp_list* predexp_list, as_error* err) {
	Py_ssize_t size = PyList_Size(py_predexp_list);
	printf("size: %d", (int)size);
    PyObject * py_pred_tuple = NULL;
    long op = 0;
    long result_type = 0;
    PyObject * fixed = NULL;
    long length_children = 0;

    for ( int i = 0; i < size; ++i ) {
        py_pred_tuple = PyList_GetItem(py_predexp_list, (Py_ssize_t)i);
        op = PyInt_AsLong(PyTuple_GetItem(py_pred_tuple, 0));
        result_type = PyInt_AsLong(PyTuple_GetItem(py_pred_tuple, 1));
        fixed = PyInt_AsLong(PyTuple_GetItem(py_pred_tuple, 2));
        length_children = PyInt_AsLong(PyTuple_GetItem(py_pred_tuple, 3));
        printf("op: %d, rt: %d, f: %d, lnchild: %d\n", op, result_type, fixed, length_children);
    }
}