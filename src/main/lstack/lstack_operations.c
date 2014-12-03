/*******************************************************************************
 * Copyright 2013-2014 Aerospike, Inc.
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

#include <aerospike/aerospike_lstack.h>
#include <aerospike/as_error.h>
#include <aerospike/as_ldt.h>

#include "client.h"
#include "conversions.h"
#include "lstack.h"
#include "policy.h"

PyObject * AerospikeLStack_Push(AerospikeLStack * self, PyObject * args, PyObject * kwds)
{
    PyObject* py_value = NULL;
    PyObject* py_policy = NULL;

    as_policy_apply apply_policy;
    as_policy_apply* apply_policy_p = NULL;

    //Error Initialization
    as_error err;
    as_error_init(&err);

	static char * kwlist[] = {"value", "policy", NULL};

	// Python Function Argument Parsing
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "O|O:push", kwlist, 
			&py_value, &py_policy) == false ) {
		return NULL;
	}

    if (!self || !self->client->as) {
        as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
        goto CLEANUP;
    }

    if (py_policy) {
        validate_policy_apply(&err, py_policy, &apply_policy);
    }
    if (err.code != AEROSPIKE_OK) {
        goto CLEANUP;
    }

	// Convert python policy object to as_policy_apply
	pyobject_to_policy_apply(&err, py_policy, &apply_policy, &apply_policy_p);
	if ( err.code != AEROSPIKE_OK ) {
		goto CLEANUP;
	}

    as_val * val = NULL;
    pyobject_to_val(&err, py_value, &val);
    if (err.code != AEROSPIKE_OK) {
        goto CLEANUP;
    }

    //aerospike_lstack_push(self->client->as, &err, apply_policy_p, &self->key,
            //&self->lstack, val);
    aerospike_lstack_push(self->client->as, &err, NULL, &self->key,
            &self->lstack, val);

CLEANUP:

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		return NULL;
	}
	return PyLong_FromLong(0);
}

PyObject * AerospikeLStack_Push_Many(AerospikeLStack * self, PyObject * args, PyObject * kwds)
{
    PyObject* py_arglist = NULL;
    PyObject* py_policy = NULL;

    as_error err;
    as_error_init(&err);

    as_policy_apply apply_policy;
    as_policy_apply* apply_policy_p = NULL;

	static char * kwlist[] = {"values", "policy", NULL};

	// Python Function Argument Parsing
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "O|O:push_many", kwlist, 
			&py_arglist, &py_policy)== false ) {
		return NULL;
	}

    if (!self || !self->client->as) {
        as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
        goto CLEANUP;
    }

    if (py_policy) {
        validate_policy_apply(&err, py_policy, &apply_policy);
    }
    if (err.code != AEROSPIKE_OK) {
        goto CLEANUP;
    }

	// Convert python policy object to as_policy_apply
	pyobject_to_policy_apply(&err, py_policy, &apply_policy, &apply_policy_p);
	if ( err.code != AEROSPIKE_OK ) {
		goto CLEANUP;
	}

    /*
     * Convert python list to as list 
     */
    if ( !PyList_Check(py_arglist)) {
        goto CLEANUP;
    }

    if ( !PyList_Check(py_arglist)) {
        goto CLEANUP;
    }

    as_list* arglist = NULL;
    pyobject_to_list(&err, py_arglist, &arglist);
    if (err.code != AEROSPIKE_OK) {
        goto CLEANUP;
    }

    aerospike_lstack_push_all(self->client->as, &err, apply_policy_p,
            &self->key, &self->lstack, arglist);

CLEANUP:

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		return NULL;
	}
	return PyLong_FromLong(0);
}

PyObject * AerospikeLStack_Peek(AerospikeLStack * self, PyObject * args, PyObject * kwds)
{
    long peek_count = 0;
    PyObject* py_policy = NULL;

    as_policy_apply apply_policy;
    as_policy_apply* apply_policy_p = NULL;

    as_error err;
    as_error_init(&err);

	static char * kwlist[] = {"value", "policy", NULL};

	// Python Function Argument Parsing
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "l|O:peek", kwlist, 
			&peek_count, &py_policy) == false ) {
		return NULL;
	}
    
    if (!self || !self->client->as) {
        as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
        goto CLEANUP;
    }

    if (py_policy) {
        validate_policy_apply(&err, py_policy, &apply_policy);
    }
    if (err.code != AEROSPIKE_OK) {
        goto CLEANUP;
    }

	// Convert python policy object to as_policy_apply
	pyobject_to_policy_apply(&err, py_policy, &apply_policy, &apply_policy_p);
	if ( err.code != AEROSPIKE_OK ) {
		goto CLEANUP;
	}

    /*
     * Peek values from stack
     */
    as_list* list = NULL; 
    aerospike_lstack_peek(self->client->as, &err, apply_policy_p, &self->key,
            &self->lstack, peek_count, &list);
    if (err.code != AEROSPIKE_OK) {
        goto CLEANUP;
    }

    PyObject* py_list = NULL;
    list_to_pyobject(&err, list, &py_list);

CLEANUP:

    if (list) {
        as_list_destroy(list);
    }
	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		return NULL;
	}
	return py_list;
}

PyObject * AerospikeLStack_Filter(AerospikeLStack * self, PyObject * args, PyObject * kwds)
{
    long peek_count = 0;
    char* filter_name = NULL;
    PyObject * py_args = NULL; 
    PyObject* py_policy = NULL;

    as_policy_apply apply_policy;
    as_policy_apply* apply_policy_p = NULL;

    as_error err;
    as_error_init(&err);

	/*static char * kwlist[] = {"peek_count", "udf_function_name", "args", "policy", NULL};

	// Python Function Argument Parsing
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "lsO|O:filter", kwlist, 
			&peek_count, &filter_name, &py_args, &py_policy) == false ) {
		return NULL;
	}

    if (!self || !self->client->as) {
        as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
        goto CLEANUP;
    }

    if (py_policy) {
        validate_policy_apply(&err, py_policy, &apply_policy);
    }
    if (err.code != AEROSPIKE_OK) {
        goto CLEANUP;
    }

	// Convert python policy object to as_policy_apply
	pyobject_to_policy_apply(&err, py_policy, &apply_policy, &apply_policy_p);
	if ( err.code != AEROSPIKE_OK ) {
		goto CLEANUP;
	}

    if ( !PyList_Check(py_args)) {
        goto CLEANUP;
    }
    as_list* arg_list = NULL;
    pyobject_to_list(&err, py_args, &arg_list);*/

    as_list* elements_list = NULL;
    //aerospike_lstack_filter(self->client->as, &err, apply_policy_p, &self->key,
    //        &self->lstack, peek_count, filter_name, arg_list, &elements_list);
    aerospike_lstack_filter(self->client->as, &err, apply_policy_p, &self->key,
            &self->lstack, 1, NULL, NULL, &elements_list);

    if (err.code != AEROSPIKE_OK) {
        goto CLEANUP;
    }

    PyObject* py_list = NULL;
    list_to_pyobject(&err, elements_list, &py_list);
    
CLEANUP:

    if (elements_list) {
        as_list_destroy(elements_list);
    }

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		return NULL;
	}
	return py_list;
}

PyObject * AerospikeLStack_Destroy(AerospikeLStack * self, PyObject * args, PyObject * kwds)
{
    PyObject* py_policy = NULL;
    as_policy_apply apply_policy;
    as_policy_apply* apply_policy_p = NULL;

    as_error err;
    as_error_init(&err);

	static char * kwlist[] = {"policy", NULL};

	// Python Function Argument Parsing
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "|O:destroy", kwlist, 
			&py_policy) == false ) {
		return NULL;
	}

    if (!self || !self->client->as) {
        as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
        goto CLEANUP;
    }

    if (py_policy) {
        validate_policy_apply(&err, py_policy, &apply_policy);
    }
    if (err.code != AEROSPIKE_OK) {
        goto CLEANUP;
    }

	// Convert python policy object to as_policy_apply
	pyobject_to_policy_apply(&err, py_policy, &apply_policy, &apply_policy_p);
	if ( err.code != AEROSPIKE_OK ) {
		goto CLEANUP;
	}

    aerospike_lstack_destroy(self->client->as, &err, apply_policy_p, &self->key,
            &self->lstack);

CLEANUP:

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		return NULL;
	}
	return PyLong_FromLong(0);
}

PyObject * AerospikeLStack_Get_Capacity(AerospikeLStack * self, PyObject * args, PyObject * kwds)
{
    long capacity = 0;
    PyObject* py_policy = NULL;
    as_policy_apply apply_policy;
    as_policy_apply* apply_policy_p = NULL;

    as_error err;
    as_error_init(&err);

	static char * kwlist[] = {"policy", NULL};

	// Python Function Argument Parsing
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "|O:get_capacity", kwlist, 
			&py_policy) == false ) {
		return NULL;
	}

    if (!self || !self->client->as) {
        as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
        goto CLEANUP;
    }

    if (py_policy) {
        validate_policy_apply(&err, py_policy, &apply_policy);
    }
    if (err.code != AEROSPIKE_OK) {
        goto CLEANUP;
    }

	// Convert python policy object to as_policy_apply
	pyobject_to_policy_apply(&err, py_policy, &apply_policy, &apply_policy_p);
	if ( err.code != AEROSPIKE_OK ) {
		goto CLEANUP;
	}

    aerospike_lstack_get_capacity(self->client->as, &err, apply_policy_p, &self->key,
            &self->lstack, &capacity);

CLEANUP:

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		return NULL;
	}

	return PyLong_FromLong(capacity);
}

PyObject * AerospikeLStack_Set_Capacity(AerospikeLStack * self, PyObject * args, PyObject * kwds)
{
    long capacity = 0;
    PyObject* py_policy = NULL;
    as_policy_apply apply_policy;
    as_policy_apply* apply_policy_p = NULL;

    as_error err;
    as_error_init(&err);

	static char * kwlist[] = {"capacity", "policy", NULL};

	// Python Function Argument Parsing
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "l|O:set_capacity", kwlist, 
			&capacity, &py_policy) == false ) {
		return NULL;
	}

    if (!self || !self->client->as) {
        as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
        goto CLEANUP;
    }

    if (py_policy) {
        validate_policy_apply(&err, py_policy, &apply_policy);
    }
    if (err.code != AEROSPIKE_OK) {
        goto CLEANUP;
    }

	// Convert python policy object to as_policy_apply
	pyobject_to_policy_apply(&err, py_policy, &apply_policy, &apply_policy_p);
	if ( err.code != AEROSPIKE_OK ) {
		goto CLEANUP;
	}

    aerospike_lstack_set_capacity(self->client->as, &err, apply_policy_p, &self->key,
            &self->lstack, capacity);

CLEANUP:

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		return NULL;
	}

	return PyLong_FromLong(0);
}

PyObject * AerospikeLStack_Size(AerospikeLStack * self, PyObject * args, PyObject * kwds)
{
    uint32_t size = 0;
    PyObject* py_policy = NULL;
    as_policy_apply apply_policy;
    as_policy_apply* apply_policy_p = NULL;

    as_error err;
    as_error_init(&err);
    
	static char * kwlist[] = {"policy", NULL};

	// Python Function Argument Parsing
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "|O:size", kwlist, 
			&py_policy) == false ) {
		return NULL;
	}

    if (!self || !self->client->as) {
        as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
        goto CLEANUP;
    }

    if (py_policy) {
        validate_policy_apply(&err, py_policy, &apply_policy);
    }
    if (err.code != AEROSPIKE_OK) {
        goto CLEANUP;
    }

	// Convert python policy object to as_policy_apply
	pyobject_to_policy_apply(&err, py_policy, &apply_policy, &apply_policy_p);
	if ( err.code != AEROSPIKE_OK ) {
		goto CLEANUP;
	}

    aerospike_lstack_size(self->client->as, &err, apply_policy_p, &self->key,
            &self->lstack, &size);

CLEANUP:

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		return NULL;
	}

	return PyLong_FromLong(size);
}

PyObject * AerospikeLStack_Config(AerospikeLStack * self, PyObject * args, PyObject * kwds)
{
    /*
     * To be implemented
     * CSDK API is not present.
     */
	return PyLong_FromLong(0);
}
