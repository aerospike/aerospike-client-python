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
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/as_key.h>
#include <aerospike/as_error.h>
#include <aerospike/as_record.h>
#include <aerospike/as_operations.h>
#include <aerospike/as_map_operations.h>
#include <aerospike/aerospike_info.h>
#include "client.h"
#include "conversions.h"
#include "exceptions.h"
#include "policy.h"

#include <aerospike/as_double.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_geojson.h>
#include <aerospike/as_nil.h>

// Struct for Python User-Data for the Callback
typedef struct {
	as_error error;
	PyObject *py_results;
    PyObject *main_mod;
	AerospikeClient *client;
} LocalData;

static bool
batch_read_operate_cb(const as_batch_read* results, uint32_t n, void* udata)
{
	// Extract callback user-data
	LocalData *data = (LocalData *)udata;
	as_error *error = &data->error;
	PyObject *py_err = NULL;
	PyObject *py_arglist = NULL;
	as_batch_read* r = NULL;
	PyObject *py_rec;
	PyObject *py_exception;

	// Lock Python State
	PyGILState_STATE gstate;
	gstate = PyGILState_Ensure();

	error_to_pyobject(error, &py_err);

	PyList_Append(data->py_results, py_err);

	for (uint32_t i = 0; i < n; i++) {
		r = (as_batch_read*) &results[i];

		error->code = r->result;

		if (error->code == AEROSPIKE_OK) {
			record_to_resultpyobject(data->client, error, 
									&r->record,
									&py_rec);
		}

		error_to_pyobject(error, &py_err);

        // TODO this might need to come last
		Py_INCREF(Py_None);
		if (error->code == AEROSPIKE_OK) {
			py_exception = Py_None;
		} 
        else {
			py_rec = Py_None;
			py_exception = raise_exception(error);
		}

		// py_arglist = PyTuple_New(3);
		// PyTuple_SetItem(py_arglist, 0, py_rec); //1-record tuple (key-tuple, meta, bin)
		// PyTuple_SetItem(py_arglist, 1, py_err); //2-error tuple
		// PyTuple_SetItem(py_arglist, 2, py_exception); //3-exception

        PyObject *batch_record = NULL;

        batch_record = PyObject_CallMethod(data->main_mod, "BatchRecord", "");
        if ( !batch_record) {
            as_error_update(error, AEROSPIKE_ERR_CLIENT,
                            "Unable to instance BatchRecord");
        }

        PyObject_SetAttrString(batch_record, "record", py_rec);

		PyList_Append(data->py_results, batch_record);
	}

	PyGILState_Release(gstate);

	return true;
}

/**
 *******************************************************************************************************
 * This function invokes csdk's API's.
 *
 * @param self                  AerospikeClient object
 * @param err                   The as_error to be populated by the function
 *                              with the encountered error if any.
 * @param py_keys                   The list containing keys.
 * @param py_ops               The list containing op, bin and value.
 * @param py_policy      		Python dict used to populate the operate_policy or map_policy. TODO correct these
 * @param py_policy      		Python dict used to populate the operate_policy or map_policy.
 *******************************************************************************************************
 */
static PyObject *AerospikeClient_Batch_Operate_Invoke(AerospikeClient *self,
												as_error *err, 
												PyObject *py_keys,
												PyObject *py_ops,
												PyObject *py_policy_batch,
                                                PyObject *py_policy_batch_write)
{
	long operation;
	long return_type = -1;

	as_policy_batch policy_batch;
	as_policy_batch *policy_batch_p = NULL;

	as_policy_batch_write policy_batch_write;
	as_policy_batch_write *policy_batch_write_p = NULL;

	PyObject *py_results = NULL;
	as_batch batch;
	
	as_batch_init(&batch, 0);

	// For expressions conversion.
	as_exp exp_list;
	as_exp *exp_list_p = NULL;

	// For converting predexp.
	as_predexp_list predexp_list;
	as_predexp_list *predexp_list_p = NULL;

	as_vector *unicodeStrVector = as_vector_create(sizeof(char *), 128);

	as_static_pool static_pool;
	memset(&static_pool, 0, sizeof(static_pool));

	as_operations ops;

	Py_ssize_t ops_size = PyList_Size(py_ops);
	as_operations_inita(&ops, ops_size);

	for (int i = 0; i < ops_size; i++) {
		PyObject *py_val = PyList_GetItem(py_ops, i);

        if ( !PyDict_Check(py_val)) {
			as_error_update(err, AEROSPIKE_ERR_PARAM,
							"op should be an aerospike operation dictionary");
			goto CLEANUP;
        }

        if (add_op(self, err, py_val, unicodeStrVector, &static_pool, &ops,
                    &operation, &return_type) != AEROSPIKE_OK) {
            goto CLEANUP;
        }
	}

	Py_ssize_t keys_size = PyList_Size(py_keys);
	as_batch_init(&batch, keys_size);

	for (int i = 0; i < keys_size; i++) {
		PyObject *py_key = PyList_GetItem(py_keys, i);

		if ( !PyTuple_Check(py_key)) {
			as_error_update(err, AEROSPIKE_ERR_PARAM,
							"key should be an aerospike key tuple");
			goto CLEANUP;
		}

		pyobject_to_key(err, py_key, as_batch_keyat(&batch, i));
		if (err->code != AEROSPIKE_OK) {
			as_error_update(err, AEROSPIKE_ERR_PARAM,
							"failed to convert key at index: %d", i);
			goto CLEANUP;
		}
	}

	if (py_policy_batch) {
		if (pyobject_to_policy_batch(self, err, py_policy_batch, &policy_batch, &policy_batch_p,
								&self->as->config.policies.batch, &predexp_list,
								&predexp_list_p, &exp_list, &exp_list_p) != AEROSPIKE_OK) {
			goto CLEANUP;
		}
	}

	if (py_policy_batch_write) {
		if (pyobject_to_batch_write_policy(self, err, py_policy_batch_write, &policy_batch_write,
                                &policy_batch_write_p, &exp_list, &exp_list_p) != AEROSPIKE_OK) {
			goto CLEANUP;
		}
	}

    // import batch_records helper
    PyObject *br_module = NULL;
    PyObject* main_mod = NULL;
    PyObject* br_instance = NULL;
    PyObject *sys_modules = PyImport_GetModuleDict();

    if (PyMapping_HasKeyString(sys_modules, "batch_records")) {
        br_module = PyMapping_GetItemString(sys_modules, "batch_records");
    }
    else {
        br_module = PyImport_ImportModule("aerospike_helpers.batch_records");
    }

    if ( !br_module) {
        as_error_update(err, AEROSPIKE_ERR_CLIENT,
                        "Unable to load batch_records module");
        goto CLEANUP;
    }

    br_instance = PyObject_CallMethod(br_module, "BatchRecords", "");
    if ( !br_instance) {
        as_error_update(err, AEROSPIKE_ERR_CLIENT,
                        "Unable to instance BatchRecords");
        goto CLEANUP;
    }

	// Create and initialize callback user-data
	LocalData data;
	data.client = self;
	py_results = PyList_New(0);
	data.py_results = py_results;

	as_error_init(&data.error);

	Py_BEGIN_ALLOW_THREADS

	aerospike_batch_operate(self->as, &data.error, 
							policy_batch_p, policy_batch_write_p,
                            &batch, &ops,
							batch_read_operate_cb, &data);

	Py_END_ALLOW_THREADS

	as_error_copy(err, &data.error);

	if (err->code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

CLEANUP:
    // don't need below loop?
	for (unsigned int i = 0; i < unicodeStrVector->size; i++) {
		free(as_vector_get_ptr(unicodeStrVector, i));
	}

	if (exp_list_p) {
		as_exp_destroy(exp_list_p);
	}

	if (predexp_list_p) {
		as_predexp_list_destroy(&predexp_list);
	}

	as_vector_destroy(unicodeStrVector);

	as_operations_destroy(&ops);

	as_batch_destroy(&batch);

	if (err->code != AEROSPIKE_OK) {
		PyObject *py_err = NULL;
		error_to_pyobject(err, &py_err);
		PyObject *exception_type = raise_exception(err);
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);

		if (py_results) {
			Py_DECREF(py_results);
		}
		return NULL;
	}

	return py_results;
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
PyObject *AerospikeClient_Batch_Operate(AerospikeClient *self, PyObject *args,
								   PyObject *kwds)
{
	as_error err;
	PyObject *py_policy_batch = NULL;
	PyObject *py_policy_batch_write = NULL;
	PyObject *py_keys = NULL;
	PyObject *py_ops = NULL;
	PyObject *py_results = NULL;

	as_error_init(&err);

	// Python Function Keyword Arguments
	static char *kwlist[] = {"keys", "ops", "policy_batch", "policy_batch_write", NULL};
	if (PyArg_ParseTupleAndKeywords(args, kwds, "OO|OO:batch_Operate", kwlist,
									&py_keys,
                                    &py_ops,
									&py_policy_batch,
                                    &py_policy_batch_write
                                    ) == false) {
		return NULL;
	}

    // required arg so don't need to check for NULL
    if ( !PyList_Check(py_ops)) {
			as_error_update(&err, AEROSPIKE_ERR_PARAM,
							"ops should be a list of op dictionaries");
			goto CLEANUP;
    }

    // required arg so don't need to check for NULL
    if ( !PyList_Check(py_keys)) {
			as_error_update(&err, AEROSPIKE_ERR_PARAM,
							"keys should be a list of aerospike key tuples");
			goto CLEANUP;
    }

	py_results = AerospikeClient_Batch_Operate_Invoke(self, &err, 
											py_keys, py_ops, py_policy_batch,
                                            py_policy_batch_write);

CLEANUP:

	if (err.code != AEROSPIKE_OK) {
		PyObject *py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyObject *exception_type = raise_exception(&err);
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return NULL;
	}

	return py_results;
}
