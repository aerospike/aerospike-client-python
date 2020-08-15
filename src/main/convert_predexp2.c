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
#include <aerospike/as_vector.h>

#include "client.h"
#include "conversions.h"
#include "exceptions.h"

/**********
* TODO
* solve ndefined symbol: AS_PX_BIN_INT on import
***********/

 // EXPR OPS
#define EQ 1
#define NE 2
#define GT 3
#define GE 4
#define LT 5
#define LE 6
#define CMP_REGEX 7
#define CMP_GEO 8

#define AND 16
#define OR 17
#define NOT 18

#define META_DIGEST_MOD 64
#define META_DEVICE_SIZE 65
#define META_LAST_UPDATE_TIME 66
#define META_VOID_TIME 67
#define META_TTL 68
#define META_SET_NAME 69
#define META_KEY_EXISTS 70

#define REC_KEY 80
#define BIN 81
#define BIN_TYPE 82
#define BIN_EXISTS 83

#define CALL 127

// RESULT TYPES
#define BOOLEAN 1
#define INTEGER 2
#define STRING 3
#define LIST 4
#define MAP 5
#define BLOB 6
#define FLOAT 7
#define GEOJSON 8
#define HLL 9

struct pred_op {
	long op;
	long result_type;
	PyObject * fixed;
	long num_children;
};

int* call_pred_macro(struct pred_op * pred, as_error * err) {
	switch (pred->op) {
		case BIN:
			switch (pred->result_type) {
				case INTEGER:
					return AS_PX_BIN_INT(PyTuple_GetItem(pred->fixed, 0));
				break;
			}
		break;
	}
}

as_status convert_predexp_list(PyObject* py_predexp_list, as_predexp_list* predexp_list, as_error* err) {
	Py_ssize_t size = PyList_Size(py_predexp_list);
	printf("size: %d", (int)size);
    PyObject * py_pred_tuple = NULL;
    long op = 0;
    long result_type = 0;
    PyObject * fixed = NULL;
    long num_children = 0;
	as_vector pred_queue;
	as_vector_inita(&pred_queue, sizeof(struct pred_op), size);
	struct pred_op pred;
	struct pred_op * pred_ptr = NULL;

    for ( int i = 0; i < size; ++i ) {
        py_pred_tuple = PyList_GetItem(py_predexp_list, (Py_ssize_t)i);
        op = PyInt_AsLong(PyTuple_GetItem(py_pred_tuple, 0));
        result_type = PyInt_AsLong(PyTuple_GetItem(py_pred_tuple, 1));
        fixed = PyTuple_GetItem(py_pred_tuple, 2);
        num_children = PyInt_AsLong(PyTuple_GetItem(py_pred_tuple, 3));
        printf("op: %d, rt: %d, f: %d, lnchild: %d\n", op, result_type, fixed, num_children);
		pred.op = op;
		pred.result_type = result_type;
		pred.fixed = fixed;
		pred.num_children = num_children;
		as_vector_append(&pred_queue, (void*) &pred);
    }

	// evaluate preds

	for ( uint32_t i = 0; i < size; ++i ) {
		pred_ptr = as_vector_get(&pred_queue, i);
		printf("call_pred_macro returned: %d\n", call_pred_macro(pred_ptr, err));
	}


}