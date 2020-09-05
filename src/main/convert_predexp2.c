/*******************************************************************************
 * Copyright 2013-2020 Aerospike, Inc.
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
#include <aerospike/as_exp.h>
#include <aerospike/as_vector.h>

#include "client.h"
#include "conversions.h"
#include "exceptions.h"

/**********
* TODO
* Implement list and map ops.
* Improve Error checking.
* Improve memory handling.
***********/

 // EXPR OPS
#define VAL 0
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

// VIRTUAL OPS
#define END_VA_ARGS 128

// UTILITY CONSTANTS
#define MAX_ELEMENTS 3 //TODO find largest macro and adjust this val

typedef struct {
	long op;
	long result_type;
	union {
		char * fixed;
		int64_t num_fixed;
	};
	long num_children;
} pred_op;

int bottom = -1;

// IMP do error checking for memcpy below

#define append_array(ar_size) {\
	for (int i = 0; i < ar_size; ++i) {\
		memcpy(&((*expressions)[++bottom]), &new_entries[i], sizeof(as_exp_entry));\
	}\
}

as_status add_pred_macros(as_exp_entry ** expressions, pred_op * pred, as_error * err) {
	switch (pred->op) {
		case BIN:
			switch (pred->result_type) {
				case INTEGER:;
					as_exp_entry new_entries[] = {AS_EXP_BIN_INT(pred->fixed)};
					append_array(3);
				break;
			}
			break;
		case VAL:;
			as_exp_entry new_entries[] = {AS_EXP_VAL_INT(pred->num_fixed)};
			append_array(1);
			break;
		case EQ:;
			{
				as_exp_entry new_entries[] = {AS_EXP_CMP_EQ({},{})};
				append_array(1);
			}
			break;
		case NE:;
			{
				as_exp_entry new_entries[] = {AS_EXP_CMP_NE({},{})};
				append_array(1);
			}
			break;
		case GT:;
			{
				as_exp_entry new_entries[] = {AS_EXP_CMP_GT({},{})};
				append_array(1);
			}
			break;
		case GE:;
			{
				as_exp_entry new_entries[] = {AS_EXP_CMP_GE({},{})};
				append_array(1);
			}
			break;
		case LT:;
			{
				as_exp_entry new_entries[] = {AS_EXP_CMP_LT({},{})};
				append_array(1);
			}
			break;
		case LE:;
			{
				as_exp_entry new_entries[] = {AS_EXP_CMP_LE({},{})};
				append_array(1);
			}
			break;
		case AND:;
			{
				as_exp_entry new_entries[] = {AS_EXP_AND({})};
				append_array(1);

			}
			break;
		case OR:;
			{
				as_exp_entry new_entries[] = {AS_EXP_OR({})};
				append_array(1);

			}
			break;
		case NOT:;
			{
				as_exp_entry new_entries[] = {AS_EXP_NOT({})};
				append_array(1);

			}
			break;
		case END_VA_ARGS:;
			{
				as_exp_entry new_entries[] = {{.op=_AS_EXP_CODE_END_OF_VA_ARGS}};
				append_array(1);

			}
			break;
		case META_DIGEST_MOD:;
			{
				as_exp_entry new_entries[] = {AS_EXP_META_DIGEST_MOD(pred->num_fixed)};
				append_array(2);

			}
			break;
		case META_DEVICE_SIZE:;
			{
				as_exp_entry new_entries[] = {AS_EXP_META_DEVICE_SIZE()};
				append_array(1);

			}
			break;
		case META_LAST_UPDATE_TIME:;
			{
				as_exp_entry new_entries[] = {AS_EXP_META_LAST_UPDATE()};
				append_array(1);

			}
			break;
		case META_VOID_TIME:;
			{
				as_exp_entry new_entries[] = {AS_EXP_META_VOID_TIME()};
				append_array(1);

			}
			break;
		case META_TTL:;
			{
				as_exp_entry new_entries[] = {AS_EXP_META_TTL()};
				append_array(1);

			}
			break;
		case META_SET_NAME:;
			{
				as_exp_entry new_entries[] = {AS_EXP_META_SET_NAME()};
				append_array(1);

			}
			break;
		case META_KEY_EXISTS:;
			{
				as_exp_entry new_entries[] = {AS_EXP_META_KEY_EXIST()};
				append_array(1);

			}
			break;
	}		

	return AEROSPIKE_OK;
}

as_status convert_predexp2_list(PyObject* py_predexp_list, as_exp** predexp_list, as_error* err) {
	Py_ssize_t size = PyList_Size(py_predexp_list);
	if (size <= 0) {
		return AEROSPIKE_OK;
	}
    long op = 0;
    long result_type = 0;
	long num_children = 0;
	int child_count = 1;
	uint8_t va_flag = 0;
	PyObject * py_pred_tuple = NULL;
    PyObject * fixed = NULL;
	as_vector pred_queue;
	pred_op pred;
	pred_op * pred_p = NULL;
	as_exp_entry * c_pred_entries = NULL;

	as_vector_inita(&pred_queue, sizeof(pred_op), size);

	pred_p = (pred_op*) calloc(1, sizeof(pred_op));
	if (pred_p == NULL) {
		as_error_update(err, AEROSPIKE_ERR, "could not calloc mem for pred_p");
	}

	c_pred_entries = (as_exp_entry*) calloc((size * MAX_ELEMENTS), sizeof(as_exp_entry));
	if (c_pred_entries == NULL) {
		as_error_update(err, AEROSPIKE_ERR, "could not calloc mem for c_pred_entries");
	}

    for ( int i = 0; i < size; ++i ) {
		if (child_count == 0 && va_flag >= 1) {
			pred.op=END_VA_ARGS;
			memcpy(pred_p, &pred, sizeof(pred_op)); //TODO error check
			as_vector_append(&pred_queue, (void*) pred_p);
			--va_flag;
			continue;
		}

        py_pred_tuple = PyList_GetItem(py_predexp_list, (Py_ssize_t)i);
        op = PyInt_AsLong(PyTuple_GetItem(py_pred_tuple, 0));

        result_type = PyInt_AsLong(PyTuple_GetItem(py_pred_tuple, 1));
		if (result_type == -1 && PyErr_Occurred()) {
			PyErr_Clear();
		}

        fixed = PyTuple_GetItem(py_pred_tuple, 2);
		if (fixed != NULL && fixed != Py_None) {
			PyObject * fixed_arg0 = PyTuple_GetItem(fixed, 0);
			if (PyInt_Check(fixed_arg0)) {
				pred.num_fixed = (int64_t) PyInt_AsLong(fixed_arg0);
				if (pred.num_fixed == -1 && PyErr_Occurred()) {
					if (PyErr_ExceptionMatches(PyExc_OverflowError)) {
						return as_error_update(err, AEROSPIKE_ERR_PARAM, "integer value exceeds sys.maxsize");
					}
				}
			} 
			else if (PyLong_Check(fixed_arg0)) {
				pred.num_fixed = (int64_t) PyInt_AsLong(fixed_arg0);
				if (pred.num_fixed == -1 && PyErr_Occurred()) {
					if (PyErr_ExceptionMatches(PyExc_OverflowError)) {
						return as_error_update(err, AEROSPIKE_ERR_PARAM, "integer value exceeds sys.maxsize");
					}
				}
			}
			else {
				PyObject * py_ustr = PyUnicode_AsUTF8String(fixed_arg0); //TODO this needs py 2 string handling
				pred.fixed = PyBytes_AsString(py_ustr);
			}
		}
		if (op == AND || op == OR) {
			++va_flag;
			++size;
		}

        num_children = PyInt_AsLong(PyTuple_GetItem(py_pred_tuple, 3));
		pred.op = op;
		pred.result_type = result_type;
		pred.num_children = num_children;
		memcpy(pred_p, &pred, sizeof(pred_op)); //TODO error check
		as_vector_append(&pred_queue, (void*) pred_p);
		if (va_flag) {
			child_count += num_children - 1;
		}
    }

	for ( int i = 0; i < size; ++i ) {
		pred_op * pred = (pred_op *) as_vector_get(&pred_queue, (uint32_t)i);
		add_pred_macros(&c_pred_entries, pred, err);
	}

	*predexp_list = as_exp_build(c_pred_entries, bottom + 1);
	return AEROSPIKE_OK;
}