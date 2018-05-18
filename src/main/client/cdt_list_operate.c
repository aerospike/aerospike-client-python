/*******************************************************************************
 * Copyright 2013-2018 Aerospike, Inc.
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

#define AS_PY_BIN_KEY "bin"
#define AS_PY_VAL_KEY "val"
#define AS_PY_INDEX_KEY "index"
#define AS_PY_COUNT_KEY "count"
#define AS_PY_RANK_KEY "rank"
#define AS_PY_LIST_RETURN_KEY "return_type"

#include <Python.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <aerospike/as_operations.h>
#include <aerospike/as_list_operations.h>

#include "client.h"
#include "conversions.h"
#include "exceptions.h"
#include "policy.h"
#include "serializer.h"
#include "cdt_list_operations.h"

/*
This handles
	(_op) == OP_LIST_GET_BY_INDEX ||\
	(_op) == OP_LIST_BY_INDEX_RANGE ||\
	(_op) == OP_LIST_GET_BY_RANK ||\
	(_op) == OP_LIST_BY_RANK_RANGE ||\
	(_op) == OP_LIST_GET_BY_VALUE ||\
	(_op) == OP_LIST_GET_BY_VALUE_LIST ||\
	(_op) == OP_LIST_GET_BY_VALUE_RANGE ||\
	(_op) == OP_LIST_REMOVE_BY_INDEX ||\
	(_op) == OP_LIST_REMOVE_BY_INDEX_RANGE ||\
	(_op) == OP_LIST_REMOVE_BY_RANK ||\
	(_op) == OP_LIST_REMOVE_BY_RANK_RANGE ||\
	(_op) == OP_LIST_REMOVE_BY_VALUE ||\
	(_op) == OP_LIST_REMOVE_BY_VALUE_LIST ||\
	(_op) == OP_LIST_REMOVE_BY_VALUE_RANGE ||)
*/

/* Dictionary field extraction functions */
static as_status
get_bin(as_error * err, PyObject * op_dict, as_vector * unicodeStrVector, char** binName);

static as_status
get_val(AerospikeClient * self, as_error * err, PyObject * op_dict, as_val** val,
             as_static_pool * static_pool, int serializer_type);

static as_status
get_val_list(AerospikeClient * self, as_error * err, PyObject * op_dict, as_val** val, as_static_pool * static_pool, int serializer_type);

static as_status
get_int64_t(as_error * err, const char* key, PyObject * op_dict, int64_t* count);

static as_status
get_optional_int64_t(as_error * err, const char* key,  PyObject * op_dict, int64_t* count, bool* found);

/* NEW CDT LIST OPERATIONS */
static as_status
add_op_list_get_by_index(as_error * err, char* bin, PyObject * op_dict, as_vector * unicodeStrVector, as_operations * ops);

static as_status
add_op_list_get_by_index_range(as_error * err, char* bin, PyObject * op_dict, as_vector * unicodeStrVector, as_operations * ops);

static as_status
add_op_list_get_by_rank(as_error * err, char* bin, PyObject * op_dict, as_vector * unicodeStrVector, as_operations * ops);

static as_status
add_op_list_get_by_rank_range(as_error * err, char* bin, PyObject * op_dict, as_vector * unicodeStrVector, as_operations * ops);

static as_status
get_list_return_type(as_error * err, PyObject * op_dict, as_list_return_type* return_type);

as_status 
add_new_list_op(AerospikeClient * self, as_error * err, PyObject * op_dict, as_vector * unicodeStrVector,
		as_static_pool * static_pool, as_operations * ops, long operation_code, long * ret_type, int serializer_type)

{
    char* bin = NULL;

    if (get_bin(err, op_dict, unicodeStrVector, &bin) != AEROSPIKE_OK) {
        return err->code;
    }

    switch(operation_code) {
		case OP_LIST_GET_BY_INDEX: {
            return add_op_list_get_by_index(err, bin, op_dict, unicodeStrVector, ops);
		}

		case OP_LIST_GET_BY_INDEX_RANGE: {
            return add_op_list_get_by_index_range(err, bin, op_dict, unicodeStrVector, ops);
		}

		case OP_LIST_GET_BY_RANK: {
            return add_op_list_get_by_rank(err, bin, op_dict, unicodeStrVector, ops);
		}

		case OP_LIST_GET_BY_RANK_RANGE: {
            return add_op_list_get_by_rank_range(err, bin, op_dict, unicodeStrVector, ops);
		}

		case OP_LIST_GET_BY_VALUE: {
		}

		case OP_LIST_GET_BY_VALUE_LIST: {
		}

		case OP_LIST_GET_BY_VALUE_RANGE: {
		}

		case OP_LIST_REMOVE_BY_INDEX: {
		}

		case OP_LIST_REMOVE_BY_INDEX_RANGE: {
		}

		case OP_LIST_REMOVE_BY_RANK: {
		}

		case OP_LIST_REMOVE_BY_RANK_RANGE: {
		}

		case OP_LIST_REMOVE_BY_VALUE: {
		}

		case OP_LIST_REMOVE_BY_VALUE_LIST: {
		}

		case OP_LIST_REMOVE_BY_VALUE_RANGE: {
		}

        default:
            // This should never be possible since we only get here if we know that the operation is valid.
            return as_error_update(err, AEROSPIKE_ERR_PARAM, "Unknown operation");
    }
        


	return err->code;
}

/*
The caller of this does not own the pointer to binName, and should not free it. It is either
held by Python, or is added to the list of chars to free later.
*/
static as_status
get_bin(as_error * err, PyObject * op_dict, as_vector * unicodeStrVector, char** binName)
{
        PyObject* intermediateUnicode = NULL;

        PyObject* py_bin = PyDict_GetItemString(op_dict, AS_PY_BIN_KEY);
        if (!py_bin) {
            return as_error_update(err, AEROSPIKE_ERR_PARAM, "Operation must contain an \"op\" entry");
        }
        if (string_and_pyuni_from_pystring(py_bin, &intermediateUnicode, binName, err) != AEROSPIKE_OK) {
            return err->code;
        }
        if (intermediateUnicode) {
            /*
            If this happened, we have an  extra pyobject. For historical reasons, we are strduping it's char value,
            then decref'ing the item itself.
            and storing the char* on a list of items to delete.
            */
            char* dupStr = strdup(*binName);
            *binName = dupStr;
            as_vector_append(unicodeStrVector, dupStr);
            Py_DecRef(intermediateUnicode);
        }        
        return AEROSPIKE_OK;
}

static as_status
get_val(AerospikeClient * self, as_error * err, PyObject * op_dict, as_val** val,
             as_static_pool * static_pool, int serializer_type)
{
        PyObject* py_val = PyDict_GetItemString(op_dict, AS_PY_VAL_KEY);
        if (!val) {
            return as_error_update(err, AEROSPIKE_ERR_PARAM, "Operation must contain an \"op\" entry");
        }
        return pyobject_to_val(self, err, py_val, val, static_pool, serializer_type);
}

static as_status
get_val_list(AerospikeClient * self, as_error * err, PyObject * op_dict, as_val** val, as_static_pool * static_pool, int serializer_type)
{
        PyObject* py_val = PyDict_GetItemString(op_dict, AS_PY_VAL_KEY);
        if (!val) {
            return as_error_update(err, AEROSPIKE_ERR_PARAM, "Operation must contain an \"val\" entry");
        } if (!PyList_Check(val)) {
            return as_error_update(err, AEROSPIKE_ERR_PARAM, "Value must be a list");
        }
        return pyobject_to_val(self, err, py_val, val, static_pool, serializer_type);
}

static as_status
get_int64_t(as_error * err, const char* key, PyObject * op_dict, int64_t* count)
{
        bool found = false;
        if (get_optional_int64_t(err, key, op_dict, count, &found) != AEROSPIKE_OK) {
            return err->code;
        }
        if (!found) {
            return as_error_update(err, AEROSPIKE_ERR_PARAM, "Operation missing required entry %s", key);
        }
        return AEROSPIKE_OK;
}

static as_status
get_optional_int64_t(as_error * err, const char* key,  PyObject * op_dict, int64_t* count, bool* found)
{
        *found = false;
        PyObject* py_val = PyDict_GetItemString(op_dict, key);
        if (!py_val) {
            return AEROSPIKE_OK;
        }
        if (PyInt_Check(py_val)) {
            *count = (int64_t)PyInt_AsLong(py_val);
            if (PyErr_Occurred()) {
                if(PyErr_ExceptionMatches(PyExc_OverflowError)) {
                    return as_error_update(err, AEROSPIKE_ERR_PARAM, "count too large");
                }
                return as_error_update(err, AEROSPIKE_ERR_PARAM, "Failed to convert count");

            }
        } else if (PyLong_Check(py_val)) {
            *count = (int64_t)PyLong_AsLong(py_val);
            if (PyErr_Occurred()) {
                if(PyErr_ExceptionMatches(PyExc_OverflowError)) {
                    return as_error_update(err, AEROSPIKE_ERR_PARAM, "count too large");
                }
                return as_error_update(err, AEROSPIKE_ERR_PARAM, "Failed to convert count");
            }
        } else {
            return as_error_update(err, AEROSPIKE_ERR_PARAM, "count must be an integer");
        }
        *found = true;
        return AEROSPIKE_OK;
}

static as_status
add_op_list_get_by_index(as_error * err, char* bin, PyObject * op_dict, as_vector * unicodeStrVector, as_operations * ops)
{
            int64_t index;
            as_list_return_type return_type = AS_LIST_RETURN_VALUE;

            /* Get the index*/
            if (get_int64_t(err, AS_PY_INDEX_KEY, op_dict, &index) != AEROSPIKE_OK) {
                return err->code;
            }

            if (get_list_return_type(err, op_dict, &return_type) != AEROSPIKE_OK) {
                return err->code;
            }

            if (!as_operations_add_list_get_by_index(ops, bin, index, return_type)) {
                as_error_update(err, AEROSPIKE_ERR_CLIENT, "Failed to add get_by_list_index operation");
            }

            return err->code;
}

static as_status
add_op_list_get_by_index_range(as_error * err, char* bin, PyObject * op_dict, as_vector * unicodeStrVector, as_operations * ops)
{
            int64_t index;
            int64_t count;
            bool range_specified = false;
            bool success = false;
            as_list_return_type return_type = AS_LIST_RETURN_VALUE;

            /* Get the index*/
            if (get_int64_t(err, AS_PY_INDEX_KEY, op_dict, &index) != AEROSPIKE_OK) {
                return err->code;
            }

            /* Get the count of items, and store whether it was found in range_specified*/
            if (get_optional_int64_t(err, AS_PY_COUNT_KEY, op_dict, &count, &range_specified) != AEROSPIKE_OK) {
                return err->code;
            }
            if (get_list_return_type(err, op_dict, &return_type) != AEROSPIKE_OK) {
                return err->code;
            }

            if (range_specified) {
                success = as_operations_add_list_get_by_index_range(ops, bin, index, (uint64_t)count, return_type);
            } else {
                success = as_operations_add_list_get_by_index_range_to_end(ops, bin, index, return_type);
            }
            
            if (!success) {
                as_error_update(err, AEROSPIKE_ERR_CLIENT, "Failed to add get_by_list_index_range operation");
            }

            return err->code;
}

static as_status
add_op_list_get_by_rank(as_error * err, char* bin, PyObject * op_dict, as_vector * unicodeStrVector, as_operations * ops)
{
            int64_t rank;
            as_list_return_type return_type = AS_LIST_RETURN_VALUE;

            /* Get the index*/
            if (get_int64_t(err, AS_PY_RANK_KEY, op_dict, &rank) != AEROSPIKE_OK) {
                return err->code;
            }

            if (get_list_return_type(err, op_dict, &return_type) != AEROSPIKE_OK) {
                return err->code;
            }

            if (!as_operations_add_list_get_by_rank(ops, bin, rank, return_type)) {
                as_error_update(err, AEROSPIKE_ERR_CLIENT, "Failed to add get_by_list_index operation");
            }

            return err->code;
}

static as_status
add_op_list_get_by_rank_range(as_error * err, char* bin, PyObject * op_dict, as_vector * unicodeStrVector, as_operations * ops)
{
            int64_t rank;
            int64_t count;
            bool range_specified = false;
            bool success = false;
            as_list_return_type return_type = AS_LIST_RETURN_VALUE;

            /* Get the index*/
            if (get_int64_t(err, AS_PY_RANK_KEY, op_dict, &rank) != AEROSPIKE_OK) {
                return err->code;
            }

            /* Get the count of items, and store whether it was found in range_specified*/
            if (get_optional_int64_t(err, AS_PY_COUNT_KEY, op_dict, &count, &range_specified) != AEROSPIKE_OK) {
                return err->code;
            }
            if (get_list_return_type(err, op_dict, &return_type) != AEROSPIKE_OK) {
                return err->code;
            }

            if (range_specified) {
                success = as_operations_add_list_get_by_rank_range(ops, bin, rank, (uint64_t)count, return_type);
            } else {
                success = as_operations_add_list_get_by_rank_range_to_end(ops, bin, rank, return_type);
            }
            
            if (!success) {
                as_error_update(err, AEROSPIKE_ERR_CLIENT, "Failed to add list_get_by_rank_range operation");
            }

            return err->code;
}

static as_status
get_list_return_type(as_error * err, PyObject * op_dict, as_list_return_type* return_type)
{
    int64_t int64_return_type;

    if (get_int64_t(err, AS_PY_LIST_RETURN_KEY, op_dict, &int64_return_type) != AEROSPIKE_OK) {
        return err->code;
    }
    *return_type = (as_list_return_type)int64_return_type;
    return AEROSPIKE_OK;
}
