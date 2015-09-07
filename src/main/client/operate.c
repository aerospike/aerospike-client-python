/*******************************************************************************
 * Copyright 2013-2015 Aerospike, Inc.
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
#include "client.h"
#include "conversions.h"
#include "exceptions.h"
#include "key.h"
#include "policy.h"

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
	if ((!PyInt_Check(py_value) && !PyLong_Check(py_value)) && (op == AS_OPERATOR_TOUCH)) {
	    as_error_update(err, AEROSPIKE_ERR_PARAM, "Unsupported operand type(s) for touch : only int or long allowed");
		return 1;
	} else if ( (!PyInt_Check(py_value) && !PyLong_Check(py_value) && (!PyFloat_Check(py_value) || !aerospike_has_double(self->as)) && !PyString_Check(py_value)) && op == AS_OPERATOR_INCR){
	    as_error_update(err, AEROSPIKE_ERR_PARAM, "Unsupported operand type(s) for +: only 'int' and 'str' allowed");
		return 1;
	} else if ((!PyString_Check(py_value) && !PyUnicode_Check(py_value)) && (op == AS_OPERATOR_APPEND || op == AS_OPERATOR_PREPEND)) {
	    as_error_update(err, AEROSPIKE_ERR_PARAM, "Cannot concatenate 'str' and 'non-str' objects");
		return 1;
	}
	return 0;
}

/**
 *******************************************************************************************************
 * This function checks for metadata and if present set it into the
 * as_operations.
 *
 * @param py_meta               The dictionary of metadata.
 * @param ops                   The as_operations object.
 * @param err                   The as_error to be populated by the function
 *                              with the encountered error if any.
 *
 * Returns nothing.
 *******************************************************************************************************
 */
static
void AerospikeClient_CheckForMeta(PyObject * py_meta, as_operations * ops, as_error *err)
{
	if ( py_meta && PyDict_Check(py_meta) ) {
		PyObject * py_gen = PyDict_GetItemString(py_meta, "gen");
		PyObject * py_ttl = PyDict_GetItemString(py_meta, "ttl");
        uint32_t ttl = 0;
        uint16_t gen = 0; 
		if ( py_ttl != NULL ){
			if ( PyInt_Check(py_ttl) ) {
				ttl = (uint32_t) PyInt_AsLong(py_ttl);
			} else if ( PyLong_Check(py_ttl) ) {
				ttl = (uint32_t) PyLong_AsLongLong(py_ttl);
			} else {
				as_error_update(err, AEROSPIKE_ERR_PARAM, "Ttl should be an int or long");
			}

            if((uint32_t)-1 == ttl) {
                as_error_update(err, AEROSPIKE_ERR_PARAM, "integer value for ttl exceeds sys.maxsize");
                return;
            }
            ops->ttl = ttl;
		}

		if( py_gen != NULL ){
			if ( PyInt_Check(py_gen) ) {
				gen = (uint16_t) PyInt_AsLong(py_gen);
			} else if ( PyLong_Check(py_gen) ) {
				gen = (uint16_t) PyLong_AsLongLong(py_gen);
			} else {
				as_error_update(err, AEROSPIKE_ERR_PARAM, "Generation should be an int or long");
			}

            if((uint16_t)-1 == gen) {
                as_error_update(err, AEROSPIKE_ERR_PARAM, "integer value for gen exceeds sys.maxsize");
                return;
            }
            ops->gen = gen;
		}
	} else {
		as_error_update(err, AEROSPIKE_ERR_PARAM, "Metadata should be of type dictionary");
	}
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
static
PyObject *  AerospikeClient_Operate_Invoke(
	AerospikeClient * self, as_error *err,
	as_key * key, PyObject * py_list, PyObject * py_meta,
	as_policy_operate * operate_policy_p)
{
	as_val* put_val = NULL;
	char* bin = NULL;
	char* val = NULL;
	long offset = 0;
    double double_offset = 0.0;
	uint32_t ttl = 0;
	long operation = 0;
	int i = 0;
	PyObject * py_rec = NULL;
	PyObject * py_ustr = NULL;
	PyObject * py_ustr1 = NULL;
	PyObject * py_bin = NULL;
	as_record * rec = NULL;

	as_static_pool static_pool;
	memset(&static_pool, 0, sizeof(static_pool));

	as_operations ops;
	Py_ssize_t size = PyList_Size(py_list);
	as_operations_inita(&ops, size);

	if (!self || !self->as) {
		as_error_update(err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
		goto CLEANUP;
	}

	if(py_meta) {
		AerospikeClient_CheckForMeta(py_meta, &ops, err);
	}

	if (err->code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	for ( i = 0; i < size; i++) {
		PyObject * py_val = PyList_GetItem(py_list, i);
		operation = -1;
		offset = 0;
        double_offset = 0.0;
		if ( PyDict_Check(py_val) ) {
			PyObject *key_op = NULL, *value = NULL;
			PyObject * py_value = NULL;
			Py_ssize_t pos = 0;
			while (PyDict_Next(py_val, &pos, &key_op, &value)) {
				if ( ! PyString_Check(key_op) ) {
					as_error_update(err, AEROSPIKE_ERR_CLIENT, "A operation key must be a string.");
					goto CLEANUP;
				} else {
					char * name = PyString_AsString(key_op);
					if(!strcmp(name,"op") && (PyInt_Check(value) || PyLong_Check(value))) {
						operation = PyInt_AsLong(value);
					} else if (!strcmp(name, "bin")) {
						py_bin = value;
					} else if(!strcmp(name, "val")) {
						py_value = value;
					} else {
						as_error_update(err, AEROSPIKE_ERR_PARAM, "operation can contain only op, bin and val keys");
						goto CLEANUP;
					}
				}
			}

			if (py_bin) {
				if (PyUnicode_Check(py_bin)) {
					py_ustr = PyUnicode_AsUTF8String(py_bin);
					bin = PyString_AsString(py_ustr);
				} else if (PyString_Check(py_bin)) {
					bin = PyString_AsString(py_bin);
				} else {
					as_error_update(err, AEROSPIKE_ERR_PARAM, "Bin name should be of type string");
					goto CLEANUP;
				}
			} else if (!py_bin && operation != AS_OPERATOR_TOUCH) {
				as_error_update(err, AEROSPIKE_ERR_PARAM, "Bin is not given");
				goto CLEANUP;
			}
			if (py_value) {
				if (check_type(self, py_value, operation, err)) {
                    goto CLEANUP;
				} else if (PyString_Check(py_value) && (operation == AS_OPERATOR_INCR)) {
                    char * incr_string = PyString_AsString(py_value);
                    int incr_value = 0, sign = 1;

                    if (strlen(incr_string) > 15) {
				        as_error_update(err, AEROSPIKE_ERR_PARAM, "Unsupported string length for increment operation");
                        goto CLEANUP;
                    }
                    if (*incr_string == '-') {
                        incr_string = incr_string + 1;
                        sign = -1;
                    } else if (*incr_string == '+') {
                        incr_string = incr_string + 1;
                        sign = 1;
                    }
                    while (*incr_string != '\0') {
                        if (*incr_string >= 48 && *incr_string <= 57) {
                            incr_value = (incr_value * 10) + (*incr_string ^ 0x30);
                        } else {
				            as_error_update(err, AEROSPIKE_ERR_PARAM, "Unsupported operand type(s) for +: 'int' and 'str'");
                            goto CLEANUP;
                        }
                        incr_string = incr_string + 1;
                    }
                    incr_value = incr_value * sign;
                    py_value = PyInt_FromLong(incr_value);
                }
			} else if ((!py_value) && (operation != AS_OPERATOR_READ)) {
				as_error_update(err, AEROSPIKE_ERR_PARAM, "Value should be given");
				goto CLEANUP;
			}

			switch(operation) {
				case AS_OPERATOR_APPEND:
					if (PyUnicode_Check(py_value)) {
						py_ustr1 = PyUnicode_AsUTF8String(py_value);
						val = PyString_AsString(py_ustr1);
					} else {
						val = PyString_AsString(py_value);
					}
					as_operations_add_append_str(&ops, bin, val);
					break;
				case AS_OPERATOR_PREPEND:
					if (PyUnicode_Check(py_value)) {
						py_ustr1 = PyUnicode_AsUTF8String(py_value);
						val = PyString_AsString(py_ustr1);
					} else {
						val = PyString_AsString(py_value);
					}
					as_operations_add_prepend_str(&ops, bin, val);
					break;
				case AS_OPERATOR_INCR:
					if (PyInt_Check(py_value)) {
                        offset = PyInt_AsLong(py_value);
                        as_operations_add_incr(&ops, bin, offset);
                    } else if ( PyLong_Check(py_value) ) {
                        offset = PyLong_AsLong(py_value);
                        if(-1 == offset) {
                            as_error_update(err, AEROSPIKE_ERR_PARAM, "integer value exceeds sys.maxsize");
                            goto CLEANUP;
                        }
                        as_operations_add_incr(&ops, bin, offset);
                    } else if (PyFloat_Check(py_value)) {
                        double_offset = PyFloat_AsDouble(py_value);
                        as_operations_add_incr_double(&ops, bin, double_offset);
                    }
                    break;
				case AS_OPERATOR_TOUCH:
					if (PyInt_Check(py_value)) {
                        ops.ttl = PyInt_AsLong(py_value);
                    } else if ( PyLong_Check(py_value) ) {
                        ttl = PyLong_AsLong(py_value);
                        if((uint32_t)-1 == ttl) {
                            as_error_update(err, AEROSPIKE_ERR_PARAM, "integer value for ttl exceeds sys.maxsize");
                            goto CLEANUP;
                        }
                        ops.ttl = ttl;
                    }
					as_operations_add_touch(&ops);
					break;
				case AS_OPERATOR_READ:
					as_operations_add_read(&ops, bin);
					break;
				case AS_OPERATOR_WRITE:
					pyobject_to_astype_write(self, err, bin, py_value, &put_val, &ops,
							&static_pool, SERIALIZER_PYTHON);
					if (err->code != AEROSPIKE_OK) {
						goto CLEANUP;
					}
					as_operations_add_write(&ops, bin, (as_bin_value *) put_val);
					break;
				default:
					as_error_update(err, AEROSPIKE_ERR_PARAM, "Invalid operation given");
			}
		}
	}

	// Initialize record
	as_record_init(rec, 0);

	aerospike_key_operate(self->as, err, operate_policy_p, key, &ops, &rec);
	if (err->code != AEROSPIKE_OK) {
		as_error_update(err, err->code, NULL);
		goto CLEANUP;
	}
	if(rec) {
		record_to_pyobject(self, err, rec, key, &py_rec);
	}

CLEANUP:
	if (py_ustr) {
		Py_DECREF(py_ustr);
	}
	if (py_ustr1) {
		Py_DECREF(py_ustr1);
	}
	if (rec) {
		as_record_destroy(rec);
	}
	if (key->valuep) {
		as_key_destroy(key);
	}
	if (put_val) {
		as_val_destroy(put_val);
	}

	if ( err->code != AEROSPIKE_OK ) {
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
 * This function converts PyObject key to as_key object, Also converts PyObject
 * policy to as_policy_operate object.
 *
 * @param err                   The as_error to be populated by the function
 *                              with the encountered error if any.
 * @param py_key                The PyObject key.
 * @param py_policy             The PyObject policy.
 * @param key_p                 The C client's as_key that identifies the record.
 * @param operate_policy_p      The as_policy_operate type pointer.
 * @param operate_policy_pp     The as_policy_operate type pointer to pointer.
 *******************************************************************************************************
 */
static
PyObject * AerospikeClient_convert_pythonObj_to_asType(
	AerospikeClient * self, as_error *err, PyObject* py_key,
	PyObject* py_policy, as_key* key_p,
	as_policy_operate* operate_policy_p,
	as_policy_operate** operate_policy_pp)
{
	pyobject_to_key(err, py_key, key_p);
	if ( err->code != AEROSPIKE_OK ) {
		goto CLEANUP;
	}

	if (py_policy) {
		pyobject_to_policy_operate(err, py_policy, operate_policy_p, operate_policy_pp,
				&self->as->config.policies.operate);
	}

CLEANUP:
	if ( err->code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(err, &py_err);
		PyObject *exception_type = raise_exception(err);
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return NULL;
	}
	return PyLong_FromLong(0);
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
	// Initialize error
	as_error err;
	as_error_init(&err);

	// Python Function Arguments
	PyObject * py_key = NULL;
	PyObject * py_bin = NULL;
	PyObject * py_policy = NULL;
	PyObject * py_result = NULL;
	PyObject * py_meta = NULL;
	PyObject * py_append_str = NULL;

	as_policy_operate operate_policy;
	as_policy_operate *operate_policy_p = NULL;
	as_key key;

	// Python Function Keyword Arguments
	static char * kwlist[] = {"key", "bin", "val", "meta", "policy", NULL};

	// Python Function Argument Parsing
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "OOO|OO:append", kwlist,
				&py_key, &py_bin, &py_append_str, &py_meta, &py_policy) == false ) {
		return NULL;
	}

	if (!self || !self->as) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
		goto CLEANUP;
	}

	if (!self->is_conn_16) {
		as_error_update(&err, AEROSPIKE_ERR_CLUSTER, "No connection to aerospike cluster");
		goto CLEANUP;
	}

	py_result = AerospikeClient_convert_pythonObj_to_asType(self, &err,
			py_key, py_policy, &key, &operate_policy, &operate_policy_p);
	if (!py_result) {
		goto CLEANUP;
	} else {
		Py_DECREF(py_result);
	}

	PyObject * py_list = NULL;
	py_list = create_pylist(py_list, AS_OPERATOR_APPEND, py_bin, py_append_str);
	py_result = AerospikeClient_Operate_Invoke(self, &err, &key, py_list,
			py_meta, operate_policy_p);

	if (py_list) {
		Py_DECREF(py_list);
	}
	if (err.code != AEROSPIKE_OK) {
		as_error_update(&err, err.code, NULL);
		goto CLEANUP;
	} else if (py_result == NULL) {
		return NULL;
	} else {
		Py_DECREF(py_result);
	}

CLEANUP:
	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyObject *exception_type = raise_exception(&err);
		if(PyObject_HasAttrString(exception_type, "key")) {
			PyObject_SetAttrString(exception_type, "key", py_key);
		} 
		if(PyObject_HasAttrString(exception_type, "bin")) {
			PyObject_SetAttrString(exception_type, "bin", py_bin);
		}
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return NULL;
	}
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
	// Initialize error
	as_error err;
	as_error_init(&err);

	// Python Function Arguments
	PyObject * py_key = NULL;
	PyObject * py_bin = NULL;
	PyObject * py_policy = NULL;
	PyObject * py_result = NULL;
	PyObject * py_meta = NULL;
	PyObject * py_prepend_str = NULL;

	as_policy_operate operate_policy;
	as_policy_operate *operate_policy_p = NULL;
	as_key key;

	// Python Function Keyword Arguments
	static char * kwlist[] = {"key", "bin", "val", "meta", "policy", NULL};

	// Python Function Argument Parsing
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "OOO|OO:prepend", kwlist,
				&py_key, &py_bin, &py_prepend_str, &py_meta, &py_policy) == false ) {
		return NULL;
	}

	if (!self || !self->as) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
		goto CLEANUP;
	}

	if (!self->is_conn_16) {
		as_error_update(&err, AEROSPIKE_ERR_CLUSTER, "No connection to aerospike cluster");
		goto CLEANUP;
	}

	py_result = AerospikeClient_convert_pythonObj_to_asType(self, &err,
			py_key, py_policy, &key, &operate_policy, &operate_policy_p);
	if (!py_result) {
		goto CLEANUP;
	} else {
		Py_DECREF(py_result);
	}

	PyObject * py_list = NULL;
	py_list = create_pylist(py_list, AS_OPERATOR_PREPEND, py_bin, py_prepend_str);
	py_result = AerospikeClient_Operate_Invoke(self, &err, &key, py_list,
			py_meta, operate_policy_p);

	if (py_list) {
		Py_DECREF(py_list);
	}
	if (err.code != AEROSPIKE_OK) {
		as_error_update(&err, err.code, NULL);
		goto CLEANUP;
	} else if (py_result == NULL) {
		return NULL;
	} else {
		Py_DECREF(py_result);
	}

CLEANUP:
	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyObject *exception_type = raise_exception(&err);
		if(PyObject_HasAttrString(exception_type, "key")) {
			PyObject_SetAttrString(exception_type, "key", py_key);
		} 
		if(PyObject_HasAttrString(exception_type, "bin")) {
			PyObject_SetAttrString(exception_type, "bin", py_bin);
		}
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return NULL;
	}

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
	// Initialize error
	as_error err;
	as_error_init(&err);

	// Python Function Arguments
	PyObject * py_key = NULL;
	PyObject * py_policy = NULL;
	PyObject * py_result = NULL;
	PyObject * py_bin = NULL;
	PyObject * py_meta = NULL;
	PyObject * py_offset_value = 0;

	as_key key;
	as_policy_operate operate_policy;
	as_policy_operate *operate_policy_p = NULL;

	// Python Function Keyword Arguments
	static char * kwlist[] = {"key", "bin", "offset", "meta",
		"policy", NULL};

	// Python Function Argument Parsing
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "OOO|OO:increment", kwlist,
				&py_key, &py_bin, &py_offset_value, &py_meta,
				&py_policy) == false ) {
		return NULL;
	}

	if (!self || !self->as) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
		goto CLEANUP;
	}

	if (!self->is_conn_16) {
		as_error_update(&err, AEROSPIKE_ERR_CLUSTER, "No connection to aerospike cluster");
		goto CLEANUP;
	}

	py_result = AerospikeClient_convert_pythonObj_to_asType(self, &err,
			py_key, py_policy, &key, &operate_policy, &operate_policy_p);

	if (!py_result) {
		goto CLEANUP;
	} else {
		Py_DECREF(py_result);
	}

	PyObject * py_list = NULL;
	py_list = create_pylist(py_list, AS_OPERATOR_INCR, py_bin, py_offset_value);
	py_result = AerospikeClient_Operate_Invoke(self, &err, &key, py_list,
			py_meta, operate_policy_p);
	
	if (py_list) {
		Py_DECREF(py_list);
	}
	if (err.code != AEROSPIKE_OK) {
		as_error_update(&err, err.code, NULL);
		goto CLEANUP;
	} else if (py_result == NULL) {
		return NULL;
	} else {
		Py_DECREF(py_result);
	}

CLEANUP:
	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyObject *exception_type = raise_exception(&err);
		if(PyObject_HasAttrString(exception_type, "key")) {
			PyObject_SetAttrString(exception_type, "key", py_key);
		} 
		if(PyObject_HasAttrString(exception_type, "bin")) {
			PyObject_SetAttrString(exception_type, "bin", py_bin);
		}
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return NULL;
	}
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
	// Initialize error
	as_error err;
	as_error_init(&err);

	// Python Function Arguments
	PyObject * py_key = NULL;
	PyObject * py_policy = NULL;
	PyObject * py_result = NULL;
	PyObject * py_meta = NULL;
	PyObject * py_touchvalue = 0;

	as_key key;
	as_policy_operate operate_policy;
	as_policy_operate *operate_policy_p = NULL;

	// Python Function Keyword Arguments
	static char * kwlist[] = {"key", "val", "meta", "policy", NULL};

	// Python Function Argument Parsing
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "OO|OO:touch", kwlist,
				&py_key, &py_touchvalue, &py_meta, &py_policy) == false ) {
		return NULL;
	}

	if (!self || !self->as) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
		goto CLEANUP;
	}

	if (!self->is_conn_16) {
		as_error_update(&err, AEROSPIKE_ERR_CLUSTER, "No connection to aerospike cluster");
		goto CLEANUP;
	}

	py_result = AerospikeClient_convert_pythonObj_to_asType(self, &err,
			py_key, py_policy, &key, &operate_policy, &operate_policy_p);
	if (!py_result) {
		goto CLEANUP;
	} else {
		Py_DECREF(py_result);
	}

	PyObject * py_list = NULL;
	py_list = create_pylist(py_list, AS_OPERATOR_TOUCH, NULL, py_touchvalue);
	py_result = AerospikeClient_Operate_Invoke(self, &err, &key, py_list,
			py_meta, operate_policy_p);

	if (py_list) {
		Py_DECREF(py_list);
	}
	if (err.code != AEROSPIKE_OK) {
		as_error_update(&err, err.code, NULL);
		goto CLEANUP;
	} else if (py_result == NULL) {
		return NULL;
	} else {
		Py_DECREF(py_result);
	}

CLEANUP:
	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyObject *exception_type = raise_exception(&err);
		if(PyObject_HasAttrString(exception_type, "key")) {
			PyObject_SetAttrString(exception_type, "key", py_key);
		} 
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return NULL;
	}
	return PyLong_FromLong(0);
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
	as_policy_operate operate_policy;
	as_policy_operate *operate_policy_p = NULL;

	// Python Function Keyword Arguments
	static char * kwlist[] = {"key", "list", "meta", "policy", NULL};

	// Python Function Argument Parsing
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "OO|OO:operate", kwlist,
				&py_key, &py_list, &py_meta, &py_policy) == false ) {
		return NULL;
	}

	if (!self || !self->as) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
		goto CLEANUP;
	}

	if (!self->is_conn_16) {
		as_error_update(&err, AEROSPIKE_ERR_CLUSTER, "No connection to aerospike cluster");
		goto CLEANUP;
	}

	py_result = AerospikeClient_convert_pythonObj_to_asType(self, &err,
			py_key, py_policy, &key, &operate_policy, &operate_policy_p);
	if (!py_result) {
		goto CLEANUP;
	} else {
		Py_DECREF(py_result);
	}

	if ( py_list != NULL && PyList_Check(py_list) ) {
		py_result = AerospikeClient_Operate_Invoke(self, &err, &key, py_list, py_meta, operate_policy_p);
	} else {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Operations should be of type list");
		goto CLEANUP;
	}

CLEANUP:
	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyObject *exception_type = raise_exception(&err);
		if(PyObject_HasAttrString(exception_type, "key")) {
			PyObject_SetAttrString(exception_type, "key", py_key);
		} 
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return NULL;
	}
	return py_result;
}
