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
#define AS_PY_VALUES_KEY "value_list"
#define AS_PY_VAL_BEGIN_KEY "value_begin"
#define AS_PY_VAL_END_KEY "value_end"
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
get_asval(AerospikeClient * self, as_error * err, char* key, PyObject * op_dict, as_val** val,
             as_static_pool * static_pool, int serializer_type, bool required);

static as_status
get_val_list(AerospikeClient * self, as_error * err, PyObject * op_dict, as_list** list, as_static_pool * static_pool, int serializer_type);

static as_status
get_int64_t(as_error * err, const char* key, PyObject * op_dict, int64_t* count);

static as_status
get_optional_int64_t(as_error * err, const char* key,  PyObject * op_dict, int64_t* count, bool* found);

static as_status
get_list_return_type(as_error * err, PyObject * op_dict, int* return_type);

/* NEW CDT LIST OPERATIONS*/
/* GET BY */
static as_status
add_op_list_get_by_index(as_error * err, char* bin, PyObject * op_dict, as_vector * unicodeStrVector, as_operations * ops);

static as_status
add_op_list_get_by_index_range(as_error * err, char* bin, PyObject * op_dict, as_vector * unicodeStrVector, as_operations * ops);

static as_status
add_op_list_get_by_rank(as_error * err, char* bin, PyObject * op_dict, as_vector * unicodeStrVector, as_operations * ops);

static as_status
add_op_list_get_by_rank_range(as_error * err, char* bin, PyObject * op_dict, as_vector * unicodeStrVector, as_operations * ops);

static as_status
add_op_list_get_by_value(AerospikeClient* self, as_error * err, char* bin,
                         PyObject * op_dict, as_vector * unicodeStrVector, as_operations * ops,
                         as_static_pool* static_pool, int serializer_type);

static as_status
add_op_list_get_by_value_list(AerospikeClient* self, as_error * err, char* bin, PyObject * op_dict,
                              as_vector * unicodeStrVector, as_operations * ops,
                              as_static_pool* static_pool, int serializer_type);

static as_status
add_op_list_get_by_value_range(AerospikeClient* self, as_error * err, char* bin,
                               PyObject * op_dict, as_vector * unicodeStrVector, as_operations * ops,
                               as_static_pool* static_pool, int serializer_type);

/* remove by */

static as_status
add_op_list_remove_by_index(as_error * err, char* bin, PyObject * op_dict, as_vector * unicodeStrVector, as_operations * ops);

static as_status
add_op_list_remove_by_index_range(as_error * err, char* bin, PyObject * op_dict, as_vector * unicodeStrVector, as_operations * ops);

static as_status
add_op_list_remove_by_rank(as_error * err, char* bin, PyObject * op_dict, as_vector * unicodeStrVector, as_operations * ops);

static as_status
add_op_list_remove_by_rank_range(as_error * err, char* bin, PyObject * op_dict, as_vector * unicodeStrVector, as_operations * ops);


static as_status
add_op_list_remove_by_value(AerospikeClient* self, as_error * err, char* bin,
                         PyObject * op_dict, as_vector * unicodeStrVector, as_operations * ops,
                         as_static_pool* static_pool, int serializer_type);

static as_status
add_op_list_remove_by_value_list(AerospikeClient* self, as_error * err, char* bin, PyObject * op_dict,
                              as_vector * unicodeStrVector, as_operations * ops,
                              as_static_pool* static_pool, int serializer_type);

static as_status
add_op_list_remove_by_value_range(AerospikeClient* self, as_error * err, char* bin,
                               PyObject * op_dict, as_vector * unicodeStrVector, as_operations * ops,
                               as_static_pool* static_pool, int serializer_type);

/* End forwards */
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
            return add_op_list_get_by_value(self, err, bin, op_dict, unicodeStrVector, ops, static_pool, serializer_type);
		}

		case OP_LIST_GET_BY_VALUE_LIST: {
            return add_op_list_get_by_value_list(self, err, bin, op_dict, unicodeStrVector, ops, static_pool, serializer_type);
		}

		case OP_LIST_GET_BY_VALUE_RANGE: {
            return add_op_list_get_by_value_range(self, err, bin, op_dict, unicodeStrVector, ops, static_pool, serializer_type);
		}

		case OP_LIST_REMOVE_BY_INDEX: {
            return add_op_list_remove_by_index(err, bin, op_dict, unicodeStrVector, ops);
		}

		case OP_LIST_REMOVE_BY_INDEX_RANGE: {
            return add_op_list_remove_by_index_range(err, bin, op_dict, unicodeStrVector, ops);
		}

		case OP_LIST_REMOVE_BY_RANK: {
            return add_op_list_remove_by_rank(err, bin, op_dict, unicodeStrVector, ops);
		}

		case OP_LIST_REMOVE_BY_RANK_RANGE: {
            return add_op_list_remove_by_rank_range(err, bin, op_dict, unicodeStrVector, ops);
		}

		case OP_LIST_REMOVE_BY_VALUE: {
            return add_op_list_remove_by_value(self, err, bin, op_dict, unicodeStrVector, ops, static_pool, serializer_type);
		}

		case OP_LIST_REMOVE_BY_VALUE_LIST: {
            return add_op_list_remove_by_value_list(self, err, bin, op_dict, unicodeStrVector, ops, static_pool, serializer_type);
		}

		case OP_LIST_REMOVE_BY_VALUE_RANGE: {
            return add_op_list_remove_by_value_range(self, err, bin, op_dict, unicodeStrVector, ops, static_pool, serializer_type);
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
            return as_error_update(err, AEROSPIKE_ERR_PARAM, "Operation must contain a \"bin\" entry");
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
get_asval(AerospikeClient * self, as_error * err, char* key, PyObject * op_dict, as_val** val,
             as_static_pool * static_pool, int serializer_type, bool required)
{
        *val = NULL;
        PyObject* py_val = PyDict_GetItemString(op_dict, key);
        if (!py_val) {
            if (required) {  
                return as_error_update(err, AEROSPIKE_ERR_PARAM, "Operation must contain a \"%s\" entry", key);
            }
            else {
                *val = NULL;
                return AEROSPIKE_OK;
            }
        }
        
        /* If the value isn't required, None indicates that it isn't provided */
        if (py_val == Py_None && !required) {
            *val = NULL;
            return AEROSPIKE_OK;
        }
        return pyobject_to_val(self, err, py_val, val, static_pool, serializer_type);
}

static as_status
get_val_list(AerospikeClient * self, as_error * err, PyObject * op_dict, as_list** list_val, as_static_pool * static_pool, int serializer_type)
{
        *list_val = NULL;
        PyObject* py_val = PyDict_GetItemString(op_dict, AS_PY_VALUES_KEY);
        if (!py_val) {
            return as_error_update(err, AEROSPIKE_ERR_PARAM, "Operation must contain a \"values\" entry");
        }
        if (!PyList_Check(py_val)) {
            return as_error_update(err, AEROSPIKE_ERR_PARAM, "Value must be a list");
        }
        return pyobject_to_list(self, err, py_val, list_val, static_pool, serializer_type);
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
        }
        else if (PyLong_Check(py_val)) {
            *count = (int64_t)PyLong_AsLong(py_val);
            if (PyErr_Occurred()) {
                if(PyErr_ExceptionMatches(PyExc_OverflowError)) {
                    return as_error_update(err, AEROSPIKE_ERR_PARAM, "count too large");
                }

                return as_error_update(err, AEROSPIKE_ERR_PARAM, "Failed to convert count");
            }
        }
        else {
            return as_error_update(err, AEROSPIKE_ERR_PARAM, "count must be an integer");
        }
    
        *found = true;
        return AEROSPIKE_OK;
}

static as_status
get_list_return_type(as_error * err, PyObject * op_dict, int* return_type)
{
    int64_t int64_return_type;
    int py_bool_val = -1;

    if (get_int64_t(err, AS_PY_LIST_RETURN_KEY, op_dict, &int64_return_type) != AEROSPIKE_OK) {
        return err->code;
    }
    *return_type = int64_return_type;
    PyObject* py_inverted = PyDict_GetItemString(op_dict, "inverted"); //NOT A MAGIC STRING

    if (py_inverted) {
        py_bool_val = PyObject_IsTrue(py_inverted);
        /* Essentially bool(py_bool_val) failed, so we raise an exception */
        if (py_bool_val == -1) {
            return as_error_update(err, AEROSPIKE_ERR_PARAM, "Invalid inverted option");
        }
        if (py_bool_val == 1) {
            *return_type |= AS_LIST_RETURN_INVERTED;
        }
    }

    return AEROSPIKE_OK;
}

static as_status
add_op_list_get_by_index(as_error * err, char* bin, PyObject * op_dict, as_vector * unicodeStrVector, as_operations * ops)
{
            int64_t index;
            int return_type = AS_LIST_RETURN_VALUE;

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
            int return_type = AS_LIST_RETURN_VALUE;

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
            }
            else {
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
            int return_type = AS_LIST_RETURN_VALUE;

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
            int return_type = AS_LIST_RETURN_VALUE;

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
            }
            else {
                success = as_operations_add_list_get_by_rank_range_to_end(ops, bin, rank, return_type);
            }
            
            if (!success) {
                as_error_update(err, AEROSPIKE_ERR_CLIENT, "Failed to add list_get_by_rank_range operation");
            }

            return err->code;
}

static as_status
add_op_list_get_by_value(AerospikeClient* self, as_error * err, char* bin, PyObject * op_dict, as_vector * unicodeStrVector, as_operations * ops,
            as_static_pool* static_pool, int serializer_type)
{
            as_val* val = NULL;
            int return_type = AS_LIST_RETURN_VALUE;

            if (get_list_return_type(err, op_dict, &return_type) != AEROSPIKE_OK) {
                return err->code;
            }

            if (get_asval(self, err, AS_PY_VAL_KEY, op_dict, &val, static_pool, serializer_type, true) != AEROSPIKE_OK) {
                return err->code;
            }

            if (!as_operations_add_list_get_by_value(ops, bin, val, return_type)) {
                as_error_update(err, AEROSPIKE_ERR_CLIENT, "Failed to add list_get_by_value operation");
            }

            return err->code;
}

static as_status
add_op_list_get_by_value_list(AerospikeClient* self, as_error * err, char* bin, PyObject * op_dict, as_vector * unicodeStrVector, as_operations * ops,
            as_static_pool* static_pool, int serializer_type)
{
            as_list* value_list = NULL;
            int return_type = AS_LIST_RETURN_VALUE;

            if (get_list_return_type(err, op_dict, &return_type) != AEROSPIKE_OK) {
                return err->code;
            }

            if (get_val_list(self, err, op_dict, &value_list, static_pool, serializer_type) != AEROSPIKE_OK) {
                return err->code;
            }

            if (!as_operations_add_list_get_by_value_list(ops, bin, value_list, return_type)) {
                /* Failed to add the operation, we need to destroy the list of values */
                as_error_update(err, AEROSPIKE_ERR_CLIENT, "Failed to add list_get_by_value_list operation");
                as_val_destroy(value_list);
            }

            return err->code;
}

static as_status
add_op_list_get_by_value_range(AerospikeClient* self, as_error * err, char* bin, PyObject * op_dict, as_vector * unicodeStrVector, as_operations * ops,
            as_static_pool* static_pool, int serializer_type)
{
            as_val* val_begin = NULL;
            as_val* val_end = NULL;

            int return_type = AS_LIST_RETURN_VALUE;

            if (get_list_return_type(err, op_dict, &return_type) != AEROSPIKE_OK) {
                return err->code;
            }

            if (get_asval(self, err, AS_PY_VAL_BEGIN_KEY, op_dict, &val_begin, static_pool, serializer_type, false) != AEROSPIKE_OK) {
                return err->code;
            }

            if (get_asval(self, err, AS_PY_VAL_END_KEY, op_dict, &val_end, static_pool, serializer_type, false) != AEROSPIKE_OK) {
                goto ERROR;
            }

            if (!as_operations_add_list_get_by_value_range(ops, bin, val_begin, val_end, return_type)) {
                as_error_update(err, AEROSPIKE_ERR_CLIENT, "Failed to add list_get_by_value_range operation");
                goto ERROR;
            }
            return err->code;

ERROR:
    /* Free the as_vals if they exists */
    if (val_begin) {
        as_val_destroy(val_begin);
    }
    if (val_end) {
        as_val_destroy(val_end);
    }
    return err->code;
}

static as_status
add_op_list_remove_by_index(as_error * err, char* bin, PyObject * op_dict, as_vector * unicodeStrVector, as_operations * ops)
{
            int64_t index;
            int return_type = AS_LIST_RETURN_VALUE;

            /* Get the index*/
            if (get_int64_t(err, AS_PY_INDEX_KEY, op_dict, &index) != AEROSPIKE_OK) {
                return err->code;
            }

            if (get_list_return_type(err, op_dict, &return_type) != AEROSPIKE_OK) {
                return err->code;
            }

            if (!as_operations_add_list_remove_by_index(ops, bin, index, return_type)) {
                as_error_update(err, AEROSPIKE_ERR_CLIENT, "Failed to add remove_by_list_index operation");
            }

            return err->code;
}

static as_status
add_op_list_remove_by_index_range(as_error * err, char* bin, PyObject * op_dict, as_vector * unicodeStrVector, as_operations * ops)
{
            int64_t index;
            int64_t count;
            bool range_specified = false;
            bool success = false;
            int return_type = AS_LIST_RETURN_VALUE;

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
                success = as_operations_add_list_remove_by_index_range(ops, bin, index, (uint64_t)count, return_type);
            }
            else {
                success = as_operations_add_list_remove_by_index_range_to_end(ops, bin, index, return_type);
            }
            
            if (!success) {
                as_error_update(err, AEROSPIKE_ERR_CLIENT, "Failed to add remove_by_list_index_range operation");
            }

            return err->code;
}


static as_status
add_op_list_remove_by_rank(as_error * err, char* bin, PyObject * op_dict, as_vector * unicodeStrVector, as_operations * ops)
{
            int64_t rank;
            int return_type = AS_LIST_RETURN_VALUE;

            /* Get the index*/
            if (get_int64_t(err, AS_PY_RANK_KEY, op_dict, &rank) != AEROSPIKE_OK) {
                return err->code;
            }

            if (get_list_return_type(err, op_dict, &return_type) != AEROSPIKE_OK) {
                return err->code;
            }

            if (!as_operations_add_list_remove_by_rank(ops, bin, rank, return_type)) {
                as_error_update(err, AEROSPIKE_ERR_CLIENT, "Failed to add list_remove_by_rank operation");
            }

            return err->code;
}

static as_status
add_op_list_remove_by_rank_range(as_error * err, char* bin, PyObject * op_dict, as_vector * unicodeStrVector, as_operations * ops)
{
            int64_t rank;
            int64_t count;
            bool range_specified = false;
            bool success = false;
            int return_type = AS_LIST_RETURN_VALUE;

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
                success = as_operations_add_list_remove_by_rank_range(ops, bin, rank, (uint64_t)count, return_type);
            }
            else {
                success = as_operations_add_list_remove_by_rank_range_to_end(ops, bin, rank, return_type);
            }
            
            if (!success) {
                as_error_update(err, AEROSPIKE_ERR_CLIENT, "Failed to add list_remove_by_rank_range operation");
            }

            return err->code;
}

static as_status
add_op_list_remove_by_value(AerospikeClient* self, as_error * err, char* bin, PyObject * op_dict, as_vector * unicodeStrVector, as_operations * ops,
            as_static_pool* static_pool, int serializer_type)
{
            as_val* val = NULL;
            int return_type = AS_LIST_RETURN_VALUE;

            if (get_list_return_type(err, op_dict, &return_type) != AEROSPIKE_OK) {
                return err->code;
            }

            if (get_asval(self, err, AS_PY_VAL_KEY, op_dict, &val, static_pool, serializer_type, true) != AEROSPIKE_OK) {
                return err->code;
            }

            if (!as_operations_add_list_remove_by_value(ops, bin, val, return_type)) {
                as_error_update(err, AEROSPIKE_ERR_CLIENT, "Failed to add list_remove_by_value operation");
            }

            return err->code;
}

static as_status
add_op_list_remove_by_value_list(AerospikeClient* self, as_error * err, char* bin, PyObject * op_dict, as_vector * unicodeStrVector, as_operations * ops,
            as_static_pool* static_pool, int serializer_type)
{
            as_list* value_list = NULL;
            int return_type = AS_LIST_RETURN_VALUE;

            if (get_list_return_type(err, op_dict, &return_type) != AEROSPIKE_OK) {
                return err->code;
            }

            if (get_val_list(self, err, op_dict, &value_list, static_pool, serializer_type) != AEROSPIKE_OK) {
                return err->code;
            }

            if (!as_operations_add_list_remove_by_value_list(ops, bin, value_list, return_type)) {
                /* Failed to add the operation, we need to destroy the list of values */
                as_error_update(err, AEROSPIKE_ERR_CLIENT, "Failed to add list_get_by_value_list operation");
                as_val_destroy(value_list);
            }

            return err->code;
}

static as_status
add_op_list_remove_by_value_range(AerospikeClient* self, as_error * err, char* bin, PyObject * op_dict, as_vector * unicodeStrVector, as_operations * ops,
            as_static_pool* static_pool, int serializer_type)
{
            as_val* val_begin = NULL;
            as_val* val_end = NULL;

            int return_type = AS_LIST_RETURN_VALUE;

            if (get_list_return_type(err, op_dict, &return_type) != AEROSPIKE_OK) {
                return err->code;
            }

            if (get_asval(self, err, AS_PY_VAL_BEGIN_KEY, op_dict, &val_begin, static_pool, serializer_type, false) != AEROSPIKE_OK) {
                return err->code;
            }

            if (get_asval(self, err, AS_PY_VAL_END_KEY, op_dict, &val_end, static_pool, serializer_type, false) != AEROSPIKE_OK) {
                goto ERROR;
            }

            if (!as_operations_add_list_remove_by_value_range(ops, bin, val_begin, val_end, return_type)) {
                as_error_update(err, AEROSPIKE_ERR_CLIENT, "Failed to add list_remove_by_value_range operation");
                goto ERROR;
            }
            return err->code;

ERROR:
    /* Free the as_vals if they exists */
    if (val_begin) {
        as_val_destroy(val_begin);
    }
    if (val_end) {
        as_val_destroy(val_end);
    }
    return err->code;
}
