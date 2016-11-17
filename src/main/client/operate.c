/*******************************************************************************
 * Copyright 2013-2016 Aerospike, Inc.
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
#include <stdlib.h>
#include <string.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/as_key.h>
#include <aerospike/as_error.h>
#include <aerospike/as_record.h>
#include <aerospike/as_operations.h>
#include <aerospike/aerospike_info.h>
#include "client.h"
#include "conversions.h"
#include "exceptions.h"
#include "policy.h"
#include "serializer.h"
#include "geo.h"

#include <aerospike/as_double.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_geojson.h>
#include <aerospike/as_nil.h>


#define BASE_VARIABLES\
	as_error err;\
	as_error_init(&err);\
	PyObject * py_key = NULL;\
	PyObject * py_policy = NULL;\
	PyObject * py_result = NULL;\
	PyObject * py_meta = NULL;\
	as_key key;\

#define CHECK_CONNECTED(__err)\
	if (!self || !self->as) {\
		as_error_update(__err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");\
		goto CLEANUP;\
	}\
	if (!self->is_conn_16) {\
		as_error_update(__err, AEROSPIKE_ERR_CLUSTER, "No connection to aerospike cluster");\
		goto CLEANUP;\
	}

#define EXCEPTION_ON_ERROR()\
	if (err.code != AEROSPIKE_OK) {\
		PyObject * py_err = NULL;\
		error_to_pyobject(&err, &py_err);\
		PyObject *exception_type = raise_exception(&err);\
		if (PyObject_HasAttrString(exception_type, "key")) {\
			PyObject_SetAttrString(exception_type, "key", py_key);\
		}\
		if (PyObject_HasAttrString(exception_type, "bin")) {\
			PyObject_SetAttrString(exception_type, "bin", py_bin);\
		}\
		PyErr_SetObject(exception_type, py_err);\
		Py_DECREF(py_err);\
		return NULL;\
	}

#define DECREF_LIST_AND_RESULT()\
	if (py_list) {\
		Py_DECREF(py_list);\
	}\
	if (err.code != AEROSPIKE_OK) {\
		as_error_update(&err, err.code, NULL);\
		goto CLEANUP;\
	} else if (!py_result) {\
		return NULL;\
	} else {\
		Py_DECREF(py_result);\
	}

#define CONVERT_VAL_TO_AS_VAL()\
	if (pyobject_to_astype_write(self, err, py_value, &put_val,\
		static_pool, SERIALIZER_PYTHON) != AEROSPIKE_OK) {\
		return err->code;\
	}

#define CONVERT_KEY_TO_AS_VAL()\
	if (pyobject_to_astype_write(self, err, py_key, &put_key,\
			static_pool, SERIALIZER_PYTHON) != AEROSPIKE_OK) {\
		return err->code;\
	}

#define CONVERT_RANGE_TO_AS_VAL()\
	if (pyobject_to_astype_write(self, err, py_range, &put_range,\
			static_pool, SERIALIZER_PYTHON) != AEROSPIKE_OK) {\
		return err->code;\
	}
/**
 *******************************************************************************************************
 * This function will check whether operation can be performed
 * based on operation and value type.
 *
 * @param py_list               The List.
 * @param operation             The operation to perform.
 * @param py_bin                The bin name to perform operation.
 * @param py_value              The value to perform operation.
 * @param py_initial_val        The initial value for increment operation.
 *
 * Returns 0 if operation can be performed.
 *******************************************************************************************************
 */
PyObject * create_pylist(PyObject * py_list, long operation, PyObject * py_bin,
		PyObject * py_value)
{
	PyObject * dict = PyDict_New();
	py_list = PyList_New(0);
	PyDict_SetItemString(dict, "op", PyInt_FromLong(operation));
	if (operation != AS_OPERATOR_TOUCH) {
		PyDict_SetItemString(dict, "bin", py_bin);
	}
	PyDict_SetItemString(dict, "val", py_value);

	PyList_Append(py_list, dict);
	Py_DECREF(dict);

	return py_list;
}

/**
 *******************************************************************************************************
 * This function will check whether operation can be performed
 * based on operation and value type.
 *
 * @param py_value              The value to perform operations.
 * @param op                    The operation to perform.
 *
 * Returns 0 if operation can be performed.
 *******************************************************************************************************
 */
int check_type(AerospikeClient * self, PyObject * py_value, int op, as_error *err)
{
	if ((!PyInt_Check(py_value) && !PyLong_Check(py_value) && strcmp(py_value->ob_type->tp_name, "aerospike.null")) && (op == AS_OPERATOR_TOUCH)) {
		as_error_update(err, AEROSPIKE_ERR_PARAM, "Unsupported operand type(s) for touch : only int or long allowed");
		return 1;
	} else if ((!PyInt_Check(py_value) && !PyLong_Check(py_value) && (!PyFloat_Check(py_value) || !aerospike_has_double(self->as)) &&
				strcmp(py_value->ob_type->tp_name, "aerospike.null")) && op == AS_OPERATOR_INCR) {
		as_error_update(err, AEROSPIKE_ERR_PARAM, "Unsupported operand type(s) for +: only 'int' allowed");
		return 1;
	} else if ((!PyString_Check(py_value) && !PyUnicode_Check(py_value) && !PyByteArray_Check(py_value) &&
				strcmp(py_value->ob_type->tp_name, "aerospike.null")) && (op == AS_OPERATOR_APPEND || op == AS_OPERATOR_PREPEND)) {
		as_error_update(err, AEROSPIKE_ERR_PARAM, "Cannot concatenate 'str' and 'non-str' objects");
		return 1;
	} else if (!PyList_Check(py_value) && op == OP_LIST_APPEND_ITEMS) {
		as_error_update(err, AEROSPIKE_ERR_PARAM, "Value of list_append_items should be of type list");
		return 1;
	} else if (!PyList_Check(py_value) && op == OP_LIST_INSERT_ITEMS) {
		as_error_update(err, AEROSPIKE_ERR_PARAM, "Value of list_insert_items should be of type list");
		return 1;
	}
	return 0;
}

bool opRequiresIndex(int op) {
	return (op == OP_LIST_INSERT               || op == OP_LIST_INSERT_ITEMS  ||
			op == OP_LIST_POP                  || op == OP_LIST_POP_RANGE     ||
			op == OP_LIST_REMOVE               || op == OP_LIST_REMOVE_RANGE  ||
			op == OP_LIST_SET                  || op == OP_LIST_GET           ||
			op == OP_LIST_GET_RANGE            || op == OP_LIST_TRIM          ||
			op == OP_MAP_REMOVE_BY_INDEX       || op == OP_MAP_REMOVE_BY_RANK ||
			op == OP_MAP_REMOVE_BY_RANK_RANGE  || op == OP_MAP_GET_BY_INDEX   ||
			op == OP_MAP_GET_BY_INDEX_RANGE    || op == OP_MAP_GET_BY_RANK    ||
			op == OP_MAP_GET_BY_RANK_RANGE);
}

bool opRequiresValue(int op) {
	return (op != AS_OPERATOR_READ       && op != AS_OPERATOR_TOUCH     &&
			op != OP_LIST_POP            && op != OP_LIST_REMOVE        &&
			op != OP_LIST_CLEAR          && op != OP_LIST_GET           &&
			op != OP_LIST_SIZE           && op != OP_MAP_GET_BY_KEY     &&
			op != OP_MAP_SET_POLICY      && op != OP_MAP_SIZE           &&
			op != OP_MAP_CLEAR           && op != OP_MAP_REMOVE_BY_KEY  &&
			op != OP_MAP_REMOVE_BY_INDEX && op != OP_MAP_REMOVE_BY_RANK &&
			op != OP_MAP_GET_BY_KEY      && op != OP_MAP_GET_BY_INDEX);
}

bool opRequiresRange(int op) {
	return (op == OP_MAP_REMOVE_BY_VALUE_RANGE  || op == OP_MAP_GET_BY_VALUE_RANGE);
}

bool opReturnsResult(int op) {
	return (op == AS_OPERATOR_READ  || op == OP_LIST_APPEND       ||
			op == OP_LIST_SIZE      || op == OP_LIST_APPEND_ITEMS ||
			op == OP_LIST_REMOVE    || op == OP_LIST_REMOVE_RANGE ||
			op == OP_LIST_TRIM      || op == OP_LIST_CLEAR        ||
			op == OP_LIST_GET       || op == OP_LIST_GET_RANGE    ||
			op == OP_LIST_INSERT    || op == OP_LIST_INSERT_ITEMS ||
			op == OP_LIST_POP       || op == OP_LIST_POP_RANGE    ||
			op == OP_LIST_SET       || op == OP_MAP_GET_BY_KEY    ||
			op == OP_MAP_GET_BY_KEY_RANGE);
}

bool opRequiresMapPolicy(int op) {
	return (op == OP_MAP_SET_POLICY);
}

bool opRequiresKey(int op) {
	return (op == OP_MAP_PUT                 || op == OP_MAP_INCREMENT     ||
			op == OP_MAP_DECREMENT           || op == OP_MAP_REMOVE_BY_KEY ||
			op == OP_MAP_REMOVE_BY_KEY_RANGE || op == OP_MAP_GET_BY_KEY    ||
			op == OP_MAP_GET_BY_KEY_RANGE);
}

as_status add_op(AerospikeClient * self, as_error * err, PyObject * py_val, as_vector * unicodeStrVector,
		as_static_pool * static_pool, as_operations * ops, long * op, long * ret_type) {
	as_val* put_val = NULL;
	as_val* put_key = NULL;
	as_val* put_range = NULL;
	char* bin = NULL;
	char* val = NULL;
	long offset = 0;
	long ttl = 0;
	double double_offset = 0.0;
	int index = 0;
	long operation = 0;
	uint64_t return_type = AS_MAP_RETURN_NONE;
	PyObject * py_ustr = NULL;
	PyObject * py_ustr1 = NULL;
	PyObject * py_bin = NULL;

	as_map_policy map_policy;
	as_map_policy_init(&map_policy);

	PyObject *key_op = NULL, *value = NULL;
	PyObject * py_value = NULL;
	PyObject * py_key = NULL;
	PyObject * py_index = NULL;
	PyObject * py_range = NULL;
	PyObject * py_map_policy = NULL;
	PyObject * py_return_type = NULL;
	Py_ssize_t pos = 0;
	while (PyDict_Next(py_val, &pos, &key_op, &value)) {
		if (!PyString_Check(key_op)) {
			return as_error_update(err, AEROSPIKE_ERR_CLIENT, "An operation key must be a string.");
		} else {
			char * name = PyString_AsString(key_op);
			if (!strcmp(name,"op") && (PyInt_Check(value) || PyLong_Check(value))) {
				operation = PyInt_AsLong(value);
			} else if (!strcmp(name, "bin")) {
				py_bin = value;
			} else if (!strcmp(name, "index")) {
				py_index = value;
			} else if (!strcmp(name, "val")) {
				py_value = value;
			} else if (!strcmp(name, "key")) {
				py_key = value;
			} else if (!strcmp(name, "range")) {
				py_range = value;
			} else if (!strcmp(name, "map_policy")) {
				py_map_policy = value;
			} else if (!strcmp(name, "return_type")) {
				py_return_type = value;
			} else {
				return as_error_update(err, AEROSPIKE_ERR_PARAM,
						"Operation can contain only op, bin, index, key, val, return_type and map_policy keys");
			}
		}
	}

	*op = operation;

	if (py_bin) {
		if (PyUnicode_Check(py_bin)) {
			py_ustr = PyUnicode_AsUTF8String(py_bin);
			bin = strdup(PyBytes_AsString(py_ustr));
			as_vector_append(unicodeStrVector, &bin);
			Py_DECREF(py_ustr);
		} else if (PyString_Check(py_bin)) {
			bin = PyString_AsString(py_bin);
		} else if (PyByteArray_Check(py_bin)) {
			bin = PyByteArray_AsString(py_bin);
		} else {
			return as_error_update(err, AEROSPIKE_ERR_PARAM, "Bin name should be of type string");
		}

		if (self->strict_types) {
			if (strlen(bin) > AS_BIN_NAME_MAX_LEN) {
				return as_error_update(err, AEROSPIKE_ERR_BIN_NAME, "A bin name should not exceed 14 characters limit");
			}
		}
	} else if (operation != AS_OPERATOR_TOUCH) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM, "Bin is not given");
	}

	if (py_value) {
		if (self->strict_types) {
			if (check_type(self, py_value, operation, err)) {
				return err->code;
			}
		}
	} else if (opRequiresValue(operation)) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM, "Value should be given");
	}

	if (!py_key && opRequiresKey(operation)) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM, "Operation requires key parameter");
	}

	if (py_map_policy) {
		if (pyobject_to_map_policy(err, py_map_policy, &map_policy) != AEROSPIKE_OK) {
			return err->code;
		}
	} else if (opRequiresMapPolicy(operation)) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM, "Operation requires map_policy parameter");
	}

	if (!py_range && opRequiresRange(operation)) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM, "Range should be given");
	}

	if (py_return_type) {
		if (!PyInt_Check(py_return_type)) {
			return as_error_update(err, AEROSPIKE_ERR_PARAM, "Return type should be an integer");
		}
		return_type = PyInt_AsLong(py_return_type);
	}
	*ret_type = return_type;

	if (py_index) {
		if (self->strict_types && !opRequiresIndex(operation)) {
			return as_error_update(err, AEROSPIKE_ERR_PARAM, "Operation does not need an index value");
		}
		if (PyInt_Check(py_index)) {
			index = PyInt_AsLong(py_index);
		} else {
			return as_error_update(err, AEROSPIKE_ERR_PARAM, "Index should be an integer");
		}
	} else if (opRequiresIndex(operation)) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM, "Operation needs an index value");
	}

	switch(operation) {
		case AS_OPERATOR_APPEND:
			if (PyUnicode_Check(py_value)) {
				py_ustr1 = PyUnicode_AsUTF8String(py_value);
				val = strdup(PyBytes_AsString(py_ustr1));
				as_operations_add_append_str(ops, bin, val);
				as_vector_append(unicodeStrVector, &val);
				Py_DECREF(py_ustr1);
			} else if (PyString_Check(py_value)) {
				val = PyString_AsString(py_value);
				as_operations_add_append_str(ops, bin, val);
			} else if (PyByteArray_Check(py_value)) {
				as_bytes *bytes;
				GET_BYTES_POOL(bytes, static_pool, err);
				if (err->code == AEROSPIKE_OK) {
					if (serialize_based_on_serializer_policy(self, SERIALIZER_PYTHON,
							&bytes, py_value, err) != AEROSPIKE_OK) {
						return err->code;
					}
					as_operations_add_append_rawp(ops, bin, bytes->value, bytes->size, true);
				}
			} else {
				if (!self->strict_types || !strcmp(py_value->ob_type->tp_name, "aerospike.null")) {
					as_operations *pointer_ops = ops;
					as_binop *binop = &pointer_ops->binops.entries[pointer_ops->binops.size++];
					binop->op = AS_OPERATOR_APPEND;
					initialize_bin_for_strictypes(self, err, py_value, binop, bin, static_pool);
				}
			}
			break;
		case AS_OPERATOR_PREPEND:
			if (PyUnicode_Check(py_value)) {
				py_ustr1 = PyUnicode_AsUTF8String(py_value);
				val = strdup(PyBytes_AsString(py_ustr1));
				as_operations_add_prepend_str(ops, bin, val);
				as_vector_append(unicodeStrVector, &val);
				Py_DECREF(py_ustr1);
			} else if (PyString_Check(py_value)) {
				val = PyString_AsString(py_value);
				as_operations_add_prepend_str(ops, bin, val);
			} else if (PyByteArray_Check(py_value)) {
				as_bytes *bytes;
				GET_BYTES_POOL(bytes, static_pool, err);
				if (err->code == AEROSPIKE_OK) {
					if (serialize_based_on_serializer_policy(self, SERIALIZER_PYTHON,
							&bytes, py_value, err) != AEROSPIKE_OK) {
						return err->code;
					}
					as_operations_add_prepend_rawp(ops, bin, bytes->value, bytes->size, true);
				}
			} else {
				if (!self->strict_types || !strcmp(py_value->ob_type->tp_name, "aerospike.null")) {
					as_operations *pointer_ops = ops;
					as_binop *binop = &pointer_ops->binops.entries[pointer_ops->binops.size++];
					binop->op = AS_OPERATOR_PREPEND;
					initialize_bin_for_strictypes(self, err, py_value, binop, bin, static_pool);
				}
			}
			break;
		case AS_OPERATOR_INCR:
			if (PyInt_Check(py_value)) {
				offset = PyInt_AsLong(py_value);
				as_operations_add_incr(ops, bin, offset);
			} else if (PyLong_Check(py_value)) {
				offset = PyLong_AsLong(py_value);
				if (offset == -1 && PyErr_Occurred() && self->strict_types) {
					if (PyErr_ExceptionMatches(PyExc_OverflowError)) {
						return as_error_update(err, AEROSPIKE_ERR_PARAM, "integer value exceeds sys.maxsize");
					}
				}
				as_operations_add_incr(ops, bin, offset);
			} else if (PyFloat_Check(py_value)) {
				double_offset = PyFloat_AsDouble(py_value);
				as_operations_add_incr_double(ops, bin, double_offset);
			} else {
				if (!self->strict_types || !strcmp(py_value->ob_type->tp_name, "aerospike.null")) {
					as_operations *pointer_ops = ops;
					as_binop *binop = &pointer_ops->binops.entries[pointer_ops->binops.size++];
					binop->op = AS_OPERATOR_INCR;
					initialize_bin_for_strictypes(self, err, py_value, binop, bin, static_pool);
				}
			}
			break;
		case AS_OPERATOR_TOUCH:
			if (py_value) {
				if (pyobject_to_index(self, err, py_value, &ttl) != AEROSPIKE_OK) {
					return err->code;
				}
				ops->ttl = ttl;
			}
			as_operations_add_touch(ops);
			break;
		case AS_OPERATOR_READ:
			as_operations_add_read(ops, bin);
			break;
		case AS_OPERATOR_WRITE:
			CONVERT_VAL_TO_AS_VAL();
			as_operations_add_write(ops, bin, (as_bin_value *) put_val);
			break;

		//------- LIST OPERATIONS ---------
		case OP_LIST_APPEND:
			CONVERT_VAL_TO_AS_VAL();
			as_operations_add_list_append(ops, bin, put_val);
			break;
		case OP_LIST_APPEND_ITEMS:
			CONVERT_VAL_TO_AS_VAL();
			as_operations_add_list_append_items(ops, bin, (as_list*)put_val);
			break;
		case OP_LIST_INSERT:
			CONVERT_VAL_TO_AS_VAL();
			as_operations_add_list_insert(ops, bin, index, put_val);
			break;
		case OP_LIST_INSERT_ITEMS:
			CONVERT_VAL_TO_AS_VAL();
			as_operations_add_list_insert_items(ops, bin, index, (as_list*)put_val);
			break;
		case OP_LIST_POP:
			as_operations_add_list_pop(ops, bin, index);
			break;
		case OP_LIST_POP_RANGE:
			if (py_value && (pyobject_to_index(self, err, py_value, &offset) != AEROSPIKE_OK)) {
				return err->code;
			}
			as_operations_add_list_pop_range(ops, bin, index, offset);
			break;
		case OP_LIST_REMOVE:
			as_operations_add_list_remove(ops, bin, index);
			break;
		case OP_LIST_REMOVE_RANGE:
			if (py_value && (pyobject_to_index(self, err, py_value, &offset) != AEROSPIKE_OK)) {
				return err->code;
			}
			as_operations_add_list_remove_range(ops, bin, index, offset);
			break;
		case OP_LIST_CLEAR:
			as_operations_add_list_clear(ops, bin);
			break;
		case OP_LIST_SET:
			CONVERT_VAL_TO_AS_VAL();
			as_operations_add_list_set(ops, bin, index, put_val);
			break;
		case OP_LIST_GET:
			as_operations_add_list_get(ops, bin, index);
			break;
		case OP_LIST_GET_RANGE:
			if (py_value && (pyobject_to_index(self, err, py_value, &offset) != AEROSPIKE_OK)) {
				return err->code;
			}
			as_operations_add_list_get_range(ops, bin, index, offset);
			break;
		case OP_LIST_TRIM:
			if (py_value && (pyobject_to_index(self, err, py_value, &offset) != AEROSPIKE_OK)) {
				return err->code;
			}
			as_operations_add_list_trim(ops, bin, index, offset);
			break;
		case OP_LIST_SIZE:
			as_operations_add_list_size(ops, bin);
			break;

		//------- MAP OPERATIONS ---------
		case OP_MAP_SET_POLICY:
			as_operations_add_map_set_policy(ops, bin, &map_policy);
			break;
		case OP_MAP_PUT:
			CONVERT_VAL_TO_AS_VAL();
			CONVERT_KEY_TO_AS_VAL();
			as_operations_add_map_put(ops, bin, &map_policy, put_key, put_val);
			break;
		case OP_MAP_PUT_ITEMS:
			CONVERT_VAL_TO_AS_VAL();
			as_operations_add_map_put_items(ops, bin, &map_policy, (as_map *)put_val);
			break;
		case OP_MAP_INCREMENT:
			CONVERT_VAL_TO_AS_VAL();
			CONVERT_KEY_TO_AS_VAL();
			as_operations_add_map_increment(ops, bin, &map_policy, put_key, put_val);
			break;
		case OP_MAP_DECREMENT:
			CONVERT_VAL_TO_AS_VAL();
			CONVERT_KEY_TO_AS_VAL();
			as_operations_add_map_decrement(ops, bin, &map_policy, put_key, put_val);
			break;
		case OP_MAP_SIZE:
			as_operations_add_map_size(ops, bin);
			break;
		case OP_MAP_CLEAR:
			as_operations_add_map_clear(ops, bin);
			break;
		case OP_MAP_REMOVE_BY_KEY:
			CONVERT_KEY_TO_AS_VAL();
			as_operations_add_map_remove_by_key(ops, bin, put_key, return_type);
			break;
		case OP_MAP_REMOVE_BY_KEY_LIST:
			CONVERT_VAL_TO_AS_VAL();
			as_operations_add_map_remove_by_key_list(ops, bin, (as_list *)put_val, return_type);
			break;
		case OP_MAP_REMOVE_BY_KEY_RANGE:
			CONVERT_VAL_TO_AS_VAL();
			CONVERT_KEY_TO_AS_VAL();
			as_operations_add_map_remove_by_key_range(ops, bin, put_key, put_val, return_type);
			break;
		case OP_MAP_REMOVE_BY_VALUE:
			CONVERT_VAL_TO_AS_VAL();
			as_operations_add_map_remove_by_value(ops, bin, put_val, return_type);
			break;
		case OP_MAP_REMOVE_BY_VALUE_LIST:
			CONVERT_VAL_TO_AS_VAL();
			as_operations_add_map_remove_by_value_list(ops, bin, (as_list *)put_val, return_type);
			break;
		case OP_MAP_REMOVE_BY_VALUE_RANGE:
			CONVERT_VAL_TO_AS_VAL();
			CONVERT_RANGE_TO_AS_VAL();
			as_operations_add_map_remove_by_value_range(ops, bin, put_val, put_range, return_type);
			break;
		case OP_MAP_REMOVE_BY_INDEX:
			as_operations_add_map_remove_by_index(ops, bin, index, return_type);
			break;
		case OP_MAP_REMOVE_BY_INDEX_RANGE:
			if (pyobject_to_index(self, err, py_value, &offset) != AEROSPIKE_OK) {
				return err->code;
			}
			as_operations_add_map_remove_by_index_range(ops, bin, index, offset, return_type);
			break;
		case OP_MAP_REMOVE_BY_RANK:
			as_operations_add_map_remove_by_rank(ops, bin, index, return_type);
			break;
		case OP_MAP_REMOVE_BY_RANK_RANGE:
			if (pyobject_to_index(self, err, py_value, &offset) != AEROSPIKE_OK) {
				return err->code;
			}
			as_operations_add_map_remove_by_rank_range(ops, bin, index, offset, return_type);
			break;
		case OP_MAP_GET_BY_KEY:
			CONVERT_KEY_TO_AS_VAL();
			as_operations_add_map_get_by_key(ops, bin, put_key, return_type);
			break;
		case OP_MAP_GET_BY_KEY_RANGE:
			CONVERT_RANGE_TO_AS_VAL();
			CONVERT_KEY_TO_AS_VAL();
			as_operations_add_map_get_by_key_range(ops, bin, put_key, put_range, return_type);
			break;
		case OP_MAP_GET_BY_VALUE:
			CONVERT_VAL_TO_AS_VAL();
			as_operations_add_map_get_by_value(ops, bin, put_val, return_type);
			break;
		case OP_MAP_GET_BY_VALUE_RANGE:
			CONVERT_VAL_TO_AS_VAL();
			CONVERT_RANGE_TO_AS_VAL();
			as_operations_add_map_get_by_value_range(ops, bin, put_val, put_range, return_type);
			break;
		case OP_MAP_GET_BY_INDEX:
			as_operations_add_map_get_by_index(ops, bin, index, return_type);
			break;
		case OP_MAP_GET_BY_INDEX_RANGE:
			if (pyobject_to_index(self, err, py_value, &offset) != AEROSPIKE_OK) {
				return err->code;
			}
			as_operations_add_map_get_by_index_range(ops, bin, index, offset, return_type);
			break;
		case OP_MAP_GET_BY_RANK:
			as_operations_add_map_get_by_rank(ops, bin, index, return_type);
			break;
		case OP_MAP_GET_BY_RANK_RANGE:
			if (pyobject_to_index(self, err, py_value, &offset) != AEROSPIKE_OK) {
				return err->code;
			}
			as_operations_add_map_get_by_rank_range(ops, bin, index, offset, return_type);
			break;

		default:
			if (self->strict_types) {
				return as_error_update(err, AEROSPIKE_ERR_PARAM, "Invalid operation given");
			}
	}

	return err->code;
}


/**
 *******************************************************************************************************
 * This function invokes csdk's API's.
 *
 * @param self                  AerospikeClient object
 * @param err                   The as_error to be populated by the function
 *                              with the encountered error if any.
 * @param key                   The C client's as_key that identifies the record.
 * @param py_list               The list containing op, bin and value.
 * @param py_meta               The metadata for the operation.
 * @param py_policy      		Python dict used to populate the operate_policy or map_policy.
 *******************************************************************************************************
 */
static
PyObject *  AerospikeClient_Operate_Invoke(
	AerospikeClient * self, as_error *err,
	as_key * key, PyObject * py_list, PyObject * py_meta,
	PyObject * py_policy)
{
	int i = 0;
	long operation;
	long return_type = -1;
	PyObject * py_rec = NULL;
	as_record * rec = NULL;
	as_policy_operate operate_policy;
	as_policy_operate *operate_policy_p = NULL;

	as_vector * unicodeStrVector = as_vector_create(sizeof(char *), 128);

	as_operations ops;
	Py_ssize_t size = PyList_Size(py_list);
	as_operations_inita(&ops, size);

	if (py_policy) {
		if(pyobject_to_policy_operate(err, py_policy, &operate_policy, &operate_policy_p,
				&self->as->config.policies.operate) != AEROSPIKE_OK) {
			goto CLEANUP;
		}
	}

	as_static_pool static_pool;
	memset(&static_pool, 0, sizeof(static_pool));

	CHECK_CONNECTED(err);

	if (py_meta) {
		if (check_for_meta(py_meta, &ops, err) != AEROSPIKE_OK) {
			goto CLEANUP;
		}
	}

	for (i = 0; i < size; i++) {
		PyObject * py_val = PyList_GetItem(py_list, i);

		if (PyDict_Check(py_val)) {
			if (add_op(self, err, py_val, unicodeStrVector, &static_pool, &ops, &operation, &return_type) != AEROSPIKE_OK) {
				goto CLEANUP;
			}
		}
	}
	if (err->code != AEROSPIKE_OK) {
		as_error_update(err, err->code, NULL);
		goto CLEANUP;
	}

	// Initialize record
	as_record_init(rec, 0);

	Py_BEGIN_ALLOW_THREADS
	aerospike_key_operate(self->as, err, operate_policy_p, key, &ops, &rec);
	Py_END_ALLOW_THREADS

	if (err->code != AEROSPIKE_OK) {
		as_error_update(err, err->code, NULL);
		goto CLEANUP;
	}
	if (rec) {
		if (return_type == AS_MAP_RETURN_KEY_VALUE) {
			record_to_pyobject_cnvt_list_to_map(self, err, rec, key, &py_rec);
		} else {
			record_to_pyobject(self, err, rec, key, &py_rec);
		}
	}

CLEANUP:
	for (unsigned int i=0; i<unicodeStrVector->size ; i++) {
		free(as_vector_get_ptr(unicodeStrVector, i));
	}

	as_vector_destroy(unicodeStrVector);

	if (rec) {
		as_record_destroy(rec);
	}
	if (key->valuep) {
		as_key_destroy(key);
	}

	as_operations_destroy(&ops);

	if (err->code != AEROSPIKE_OK) {
		PyObject * py_err = NULL;
		error_to_pyobject(err, &py_err);
		PyObject *exception_type = raise_exception(err);
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return NULL;
	}

	if (py_rec) {
		return py_rec;
	} else {
		return PyLong_FromLong(0);
	}
}


/**
 *******************************************************************************************************
 * Multiple operations on a single record
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns tuple of bins on success if read operation is given.
 * Otherwise returns 0 on success for other operations.
 *******************************************************************************************************
 */
PyObject * AerospikeClient_Operate(AerospikeClient * self, PyObject * args, PyObject * kwds)
{
	BASE_VARIABLES
	PyObject * py_list = NULL;
	PyObject * py_bin = NULL;

	// Python Function Keyword Arguments
	static char * kwlist[] = {"key", "list", "meta", "policy", NULL};
	if (PyArg_ParseTupleAndKeywords(args, kwds, "OO|OO:operate", kwlist,
				&py_key, &py_list, &py_meta, &py_policy) == false) {
		return NULL;
	}

	CHECK_CONNECTED(&err);

	if (pyobject_to_key(&err, py_key, &key) != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	if (py_list && PyList_Check(py_list)) {
		py_result = AerospikeClient_Operate_Invoke(self, &err, &key, py_list, py_meta,
				py_policy);
	} else {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Operations should be of type list");
	}

CLEANUP:
	EXCEPTION_ON_ERROR();

	return py_result;
}


/**
 *******************************************************************************************************
 * This function invokes csdk's API's.
 *
 * @param self                  AerospikeClient object
 * @param err                   The as_error to be populated by the function
 *                              with the encountered error if any.
 * @param key                   The C client's as_key that identifies the record.
 * @param py_list               The list containing op, bin and value.
 * @param py_meta               The metadata for the operation.
 * @param operate_policy_p      The value for operate policy.
 *******************************************************************************************************
 */
static PyObject *  AerospikeClient_OperateOrdered_Invoke(
	AerospikeClient * self, as_error *err,
	as_key * key, PyObject * py_list, PyObject * py_meta,
	PyObject * py_policy)
{
	long operation;
	long return_type;
	int i = 0;
	PyObject * py_rec = NULL;
	as_policy_operate operate_policy;
	as_policy_operate *operate_policy_p = NULL;

	as_vector * unicodeStrVector = as_vector_create(sizeof(char *), 128);

	if (py_policy) {
		if(pyobject_to_policy_operate(err, py_policy, &operate_policy, &operate_policy_p,
				&self->as->config.policies.operate) != AEROSPIKE_OK) {
			goto CLEANUP;
		}
	}

	as_static_pool static_pool;
	memset(&static_pool, 0, sizeof(static_pool));

	Py_ssize_t size = PyList_Size(py_list);

	if (!self || !self->as) {
		as_error_update(err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
		goto CLEANUP;
	}

	if (err->code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	PyObject * py_rec_key = NULL;
	PyObject * py_rec_meta = NULL;
	PyObject * py_bins = NULL;

	py_bins = PyList_New(0);

	for (i = 0; i < size; i++) {
		as_operations ops;
		as_operations_init(&ops, 1);

		as_record * rec = NULL;

		if (py_meta) {
			if (check_for_meta(py_meta, &ops, err) != AEROSPIKE_OK) {
				goto LOOP_CLEANUP;
			}
		}

		PyObject * py_val = PyList_GetItem(py_list, i);
		operation = -1;
		return_type = -1;
		if (PyDict_Check(py_val)) {
			if (add_op(self, err, py_val, unicodeStrVector, &static_pool, &ops, &operation, &return_type) != AEROSPIKE_OK) {
				goto LOOP_CLEANUP;
			}
		}

		Py_BEGIN_ALLOW_THREADS
		aerospike_key_operate(self->as, err, operate_policy_p, key, &ops, &rec);
		Py_END_ALLOW_THREADS

		if (err->code != AEROSPIKE_OK) {
			as_error_update(err, err->code, NULL);
			goto LOOP_CLEANUP;
		}

		if (rec) {
			PyObject *py_rec_bins = NULL;
			if (i == 0) {
				key_to_pyobject(err, key ? key : &rec->key, &py_rec_key);
				metadata_to_pyobject(err, rec, &py_rec_meta);
			}
			bins_to_pyobject(self, err, rec, &py_rec_bins, return_type == AS_MAP_RETURN_KEY_VALUE);

			if (opReturnsResult(operation))
			{
				PyObject *py_value = PyDict_GetItemString(py_rec_bins, ops.binops.entries->bin.name);
				PyObject *py_rec_tuple = PyTuple_New(2);
				if (py_value) {
					Py_INCREF(py_value);
					PyTuple_SetItem(py_rec_tuple, 0, PyString_FromString(ops.binops.entries->bin.name));
					PyTuple_SetItem(py_rec_tuple, 1, py_value);
				} else {
					Py_INCREF(Py_None);
					PyTuple_SetItem(py_rec_tuple, 0, PyString_FromString(ops.binops.entries->bin.name));
					PyTuple_SetItem(py_rec_tuple, 1, Py_None);
				}

				PyList_Append(py_bins, py_rec_tuple);
				Py_DECREF(py_rec_tuple);
			} else {
				Py_INCREF(Py_None);
				PyList_Append(py_bins, Py_None);
			}
			Py_DECREF(py_rec_bins);
		}

LOOP_CLEANUP:
		as_operations_destroy(&ops);

		if (rec) {
			as_record_destroy(rec);
		}

		if (err->code != AEROSPIKE_OK && i == 0) {
			goto CLEANUP;
		} else if (err->code != AEROSPIKE_OK) {
			as_error_reset(err);
			break;
		}
	}

	py_rec = PyTuple_New(3);

	PyTuple_SetItem(py_rec, 0, py_rec_key);
	PyTuple_SetItem(py_rec, 1, py_rec_meta);
	PyTuple_SetItem(py_rec, 2, py_bins);

CLEANUP:
	for (unsigned int i=0; i<unicodeStrVector->size ; i++) {
		free(as_vector_get_ptr(unicodeStrVector, i));
	}
	as_vector_destroy(unicodeStrVector);

	if (key->valuep) {
		as_key_destroy(key);
	}

	if (err->code != AEROSPIKE_OK) {
		PyObject * py_err = NULL;
		error_to_pyobject(err, &py_err);
		PyObject *exception_type = raise_exception(err);
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return NULL;
	}

	if (py_rec) {
		return py_rec;
	} else {
		return PyLong_FromLong(0);
	}
}

/**
 *******************************************************************************************************
 * Multiple operations on a single record. Results are returned in an ordered
 * manner
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns tuple of bins on success if read operation is given.
 * Otherwise returns 0 on success for other operations.
 *******************************************************************************************************
 */
PyObject * AerospikeClient_OperateOrdered(AerospikeClient * self, PyObject * args, PyObject * kwds)
{
	// Initialize error
	as_error err;
	as_error_init(&err);

	// Python Function Arguments
	PyObject * py_key = NULL;
	PyObject * py_list = NULL;
	PyObject * py_policy = NULL;
	PyObject * py_result = NULL;
	PyObject * py_meta = NULL;

	as_key key;

	// Python Function Keyword Arguments
	static char * kwlist[] = {"key", "list", "meta", "policy", NULL};

	// Python Function Argument Parsing
	if (PyArg_ParseTupleAndKeywords(args, kwds, "OO|OO:operate_ordered", kwlist,
				&py_key, &py_list, &py_meta, &py_policy) == false) {
		return NULL;
	}

	CHECK_CONNECTED(&err);

	if (pyobject_to_key(&err, py_key, &key) != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	if (py_list && PyList_Check(py_list)) {
		py_result = AerospikeClient_OperateOrdered_Invoke(self, &err, &key, py_list, py_meta,
				py_policy);
	} else {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Operations should be of type list");
		goto CLEANUP;
	}

CLEANUP:
	if (err.code != AEROSPIKE_OK) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyObject *exception_type = raise_exception(&err);
		if (PyObject_HasAttrString(exception_type, "key")) {
			PyObject_SetAttrString(exception_type, "key", py_key);
		}
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return NULL;
	}
	return py_result;
}

/**
 *******************************************************************************************************
 * Appends a string to the string value in a bin.
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns an integer status. 0(Zero) is success value.
 * In case of error,appropriate exceptions will be raised.
 *******************************************************************************************************
 */
PyObject * AerospikeClient_Append(AerospikeClient * self, PyObject * args, PyObject * kwds)
{
	BASE_VARIABLES
	PyObject * py_bin = NULL;
	PyObject * py_append_str = NULL;

	// Python Function Keyword Arguments
	static char * kwlist[] = {"key", "bin", "val", "meta", "policy", NULL};
	if (PyArg_ParseTupleAndKeywords(args, kwds, "OOO|OO:append", kwlist,
				&py_key, &py_bin, &py_append_str, &py_meta, &py_policy) == false) {
		return NULL;
	}

	CHECK_CONNECTED(&err);

	if (pyobject_to_key(&err, py_key, &key) != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	PyObject * py_list = NULL;
	py_list = create_pylist(py_list, AS_OPERATOR_APPEND, py_bin, py_append_str);
	py_result = AerospikeClient_Operate_Invoke(self, &err, &key, py_list,
			py_meta, py_policy);

	DECREF_LIST_AND_RESULT();

CLEANUP:
	EXCEPTION_ON_ERROR();

	return PyLong_FromLong(0);
}

/**
 *******************************************************************************************************
 * Prepends a string to the string value in a bin
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns an integer status. 0(Zero) is success value.
 * In case of error,appropriate exceptions will be raised.
 *******************************************************************************************************
 */
PyObject * AerospikeClient_Prepend(AerospikeClient * self, PyObject * args, PyObject * kwds)
{
	BASE_VARIABLES
	PyObject * py_bin = NULL;
	PyObject * py_prepend_str = NULL;

	// Python Function Keyword Arguments
	static char * kwlist[] = {"key", "bin", "val", "meta", "policy", NULL};
	if (PyArg_ParseTupleAndKeywords(args, kwds, "OOO|OO:prepend", kwlist,
				&py_key, &py_bin, &py_prepend_str, &py_meta, &py_policy) == false) {
		return NULL;
	}

	CHECK_CONNECTED(&err);

	if (pyobject_to_key(&err, py_key, &key) != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	PyObject * py_list = NULL;
	py_list = create_pylist(py_list, AS_OPERATOR_PREPEND, py_bin, py_prepend_str);
	py_result = AerospikeClient_Operate_Invoke(self, &err, &key, py_list,
			py_meta, py_policy);

	DECREF_LIST_AND_RESULT();

CLEANUP:
	EXCEPTION_ON_ERROR();

	return PyLong_FromLong(0);
}

/**
 *******************************************************************************************************
 * Increments a numeric value in a bin.
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns an integer status. 0(Zero) is success value.
 * In case of error,appropriate exceptions will be raised.
 *******************************************************************************************************
 */
PyObject * AerospikeClient_Increment(AerospikeClient * self, PyObject * args, PyObject * kwds)
{
	BASE_VARIABLES
	PyObject * py_bin = NULL;
	PyObject * py_offset_value = 0;

	// Python Function Keyword Arguments
	static char * kwlist[] = {"key", "bin", "offset", "meta", "policy", NULL};
	if (PyArg_ParseTupleAndKeywords(args, kwds, "OOO|OO:increment", kwlist,
				&py_key, &py_bin, &py_offset_value, &py_meta,
				&py_policy) == false) {
		return NULL;
	}

	CHECK_CONNECTED(&err);

	if (pyobject_to_key(&err, py_key, &key) != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	PyObject * py_list = NULL;
	py_list = create_pylist(py_list, AS_OPERATOR_INCR, py_bin, py_offset_value);
	py_result = AerospikeClient_Operate_Invoke(self, &err, &key, py_list,
			py_meta, py_policy);

	DECREF_LIST_AND_RESULT();

CLEANUP:
	EXCEPTION_ON_ERROR();

	return PyLong_FromLong(0);
}

/**
 *******************************************************************************************************
 * Touch a record in the Aerospike DB
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns an integer status. 0(Zero) is success value.
 * In case of error,appropriate exceptions will be raised.
 *******************************************************************************************************
 */
PyObject * AerospikeClient_Touch(AerospikeClient * self, PyObject * args, PyObject * kwds)
{
	BASE_VARIABLES
	PyObject * py_touchvalue = NULL;
	PyObject * py_bin = NULL;

	// Python Function Keyword Arguments
	static char * kwlist[] = {"key", "val", "meta", "policy", NULL};
	if (PyArg_ParseTupleAndKeywords(args, kwds, "OO|OO:touch", kwlist,
				&py_key, &py_touchvalue, &py_meta, &py_policy) == false) {
		return NULL;
	}

	CHECK_CONNECTED(&err);

	if (pyobject_to_key(&err, py_key, &key) != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	PyObject * py_list = NULL;
	py_list = create_pylist(py_list, AS_OPERATOR_TOUCH, NULL, py_touchvalue);
	py_result = AerospikeClient_Operate_Invoke(self, &err, &key, py_list,
			py_meta, py_policy);

	DECREF_LIST_AND_RESULT();

CLEANUP:
	EXCEPTION_ON_ERROR();

	return PyLong_FromLong(0);
}

