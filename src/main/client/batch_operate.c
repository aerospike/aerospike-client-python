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

as_status as_batch_result_to_BatchRecord(AerospikeClient *self, as_error *err, as_batch_result *bres, PyObject *py_batch_record) {
    as_status *result_code = &(bres->result);
    const as_key *requested_key = bres->key;
    as_record *result_rec = &(bres->record);
    bool in_doubt = bres->in_doubt;

    if (PyErr_Occurred()) {
        printf("conv 1\n");
        PyErr_Print();
    }

    PyObject *py_res = PyLong_FromLong((long)*result_code);
    PyObject_SetAttrString(py_batch_record, FIELD_NAME_BATCH_RESULT, py_res);
    Py_DECREF(py_res);

    if (PyErr_Occurred()) {
        printf("conv 2\n");
        PyErr_Print();
    }

    PyObject *py_in_doubt = PyBool_FromLong((long)in_doubt);
    PyObject_SetAttrString(py_batch_record, FIELD_NAME_BATCH_INDOUBT, py_in_doubt);
    Py_DECREF(py_in_doubt);

    if (PyErr_Occurred()) {
        printf("conv 3\n");
        PyErr_Print();
    }

    if (*result_code == AEROSPIKE_OK) {
        PyObject *rec = NULL;
        record_to_pyobject(self, err, result_rec, NULL, &rec);

        if (PyErr_Occurred()) {
            printf("conv 4\n");
            PyErr_Print();
        }

        PyObject_SetAttrString(py_batch_record, FIELD_NAME_BATCH_RECORD, rec);
        Py_DECREF(rec);

        if (PyErr_Occurred()) {
            printf("conv 5\n");
            PyErr_Print();
        }

    }

    printf("br_instance.batch_records ref count in as_batch_result_to_BatchRecord: %d\n", py_batch_record->ob_refcnt);
    return err->code;
}

// Struct for Python User-Data for the Callback
typedef struct {
	as_error error;
	PyObject *py_results;
    PyObject *batch_records_module;
    PyObject *func_name;
	AerospikeClient *client;
} LocalData;

static bool
batch_operate_cb(const as_batch_result* results, uint32_t n, void* udata)
{
	// Extract callback user-data
    printf("cb 1, n: %d\n", n);
	LocalData *data = (LocalData *)udata;
	as_error *err = &data->error;
    PyObject *py_key = NULL;
    PyObject *py_batch_record = NULL;
    bool success = true;

    printf("cb 2\n");
	// Lock Python State
	PyGILState_STATE gstate;
	gstate = PyGILState_Ensure();

    if (PyErr_Occurred()) {
        PyErr_Print();
    }

    printf("cb 5\n");
	for (uint32_t i = 0; i < n; i++) {
        printf("cb 5.1\n");

        if (PyErr_Occurred()) {
            PyErr_Print();
        }

        as_batch_read* res = NULL;
		res = (as_batch_read*) &results[i];

        // NOTE these conversions shouldn't go wrong but if they do, return
        if (key_to_pyobject(err, res->key, &py_key) != AEROSPIKE_OK) {
            printf("cb key error\n");
            as_error_update(err, AEROSPIKE_ERR_CLIENT,
                            "unable to convert res->key at results index: %d", i);
            break;
            success = false;
        }

        if (PyErr_Occurred()) {
            PyErr_Print();
        }

        py_batch_record = PyObject_CallMethodObjArgs(data->batch_records_module, data->func_name, py_key, NULL);
        if (py_batch_record == NULL) {
            as_error_update(err, AEROSPIKE_ERR_CLIENT,
                            "Unable to instance BatchRecord at results index: %d", i);
            success = false;
            Py_DECREF(py_key);
            break;
        }
        Py_DECREF(py_key);
        printf("cb 5.2\n");

        if (PyErr_Occurred()) {
            PyErr_Print();
        }

        as_batch_result_to_BatchRecord(data->client, err, res, py_batch_record);
        if (err->code != AEROSPIKE_OK) {
            success = false;
            break;
        }

        printf("cb 5.6\n");

        if (PyErr_Occurred()) {
            PyErr_Print();
        }

		PyList_Append(data->py_results, py_batch_record);

        printf("cb 5.7\n");
	}

    //printf("py_funcname ref count: %d\n", py_funcname->ob_refcnt);
    Py_XDECREF(py_batch_record);

	PyGILState_Release(gstate);
    printf("cb returning\n");
	return success;
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

    printf("1\n");

	as_policy_batch policy_batch;
	as_policy_batch *policy_batch_p = NULL;

	as_policy_batch_write policy_batch_write;
	as_policy_batch_write *policy_batch_write_p = NULL;

	as_batch batch;
	as_batch_init(&batch, 0);

	// For expressions conversion.
	as_exp batch_exp_list;
	as_exp *batch_exp_list_p = NULL;

	as_exp batch_write_exp_list;
	as_exp *batch_write_exp_list_p = NULL;

	// For converting predexp.
	as_predexp_list predexp_list;
	as_predexp_list *predexp_list_p = NULL;

	as_vector *unicodeStrVector = as_vector_create(sizeof(char *), 128);

	as_static_pool static_pool;
	memset(&static_pool, 0, sizeof(static_pool));

	as_operations ops;

	Py_ssize_t ops_size = PyList_Size(py_ops);
	as_operations_inita(&ops, ops_size);

   PyObject* br_instance = NULL;

    printf("2\n");

	for (int i = 0; i < ops_size; i++) {
        printf("2.5\n");
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

    printf("3\n");

	Py_ssize_t keys_size = PyList_Size(py_keys);
	as_batch_init(&batch, keys_size);

    printf("4\n");

	for (int i = 0; i < keys_size; i++) {
        printf("4.5\n");
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

    printf("5\n");

	if (py_policy_batch) {
        printf("5.5\n");
		if (pyobject_to_policy_batch(self, err, py_policy_batch, &policy_batch, &policy_batch_p,
								&self->as->config.policies.batch, &predexp_list,
								&predexp_list_p, &batch_exp_list, &batch_exp_list) != AEROSPIKE_OK) {
			goto CLEANUP;
		}
	}

    printf("6\n");

    // TODO both can't use same expressions vars
    // TODO check that the polices are dicts
	if (py_policy_batch_write) {
        printf("6.5\n");
		if (pyobject_to_batch_write_policy(self, err, py_policy_batch_write, &policy_batch_write,
                                &policy_batch_write_p, &batch_write_exp_list, &batch_write_exp_list) != AEROSPIKE_OK) {
			goto CLEANUP;
		}
	}

    printf("7\n");

    // import batch_records helper
    PyObject *br_module = NULL;
    PyObject *sys_modules = PyImport_GetModuleDict();

    printf("8\n");
    if (PyMapping_HasKeyString(sys_modules, "aerospike_helpers")) {
        printf("8.2\n");
        br_module = PyMapping_GetItemString(sys_modules, "aerospike_helpers.batch.records");
    }
    else {
        printf("8.3\n");
        br_module = PyImport_ImportModule("aerospike_helpers.batch.records");
    }

    printf("9\n");

    if ( !br_module) {
        as_error_update(err, AEROSPIKE_ERR_CLIENT,
                        "Unable to load batch_records module");
        goto CLEANUP;
    }

    printf("10\n");


    PyObject *obj_name = PyUnicode_FromString("BatchRecords");
    PyObject *res_list = PyList_New(0);
    br_instance = PyObject_CallMethodObjArgs(br_module, obj_name, res_list, NULL);
    if ( !br_instance) {
        as_error_update(err, AEROSPIKE_ERR_CLIENT,
                        "Unable to instance BatchRecords");
        Py_DECREF(br_module);
        Py_DECREF(obj_name);
        Py_DECREF(res_list);
        goto CLEANUP;
    }
    Py_DECREF(br_module);
    Py_DECREF(obj_name);
    Py_DECREF(res_list);

    printf("11\n");

	// Create and initialize callback user-data
	LocalData data;
	data.client = self;
    data.func_name = PyUnicode_FromString("BatchRecord");
	data.py_results = PyObject_GetAttrString(br_instance, "batch_records");
    printf("br_instance.batch_records ref1 count: %d\n", data.py_results->ob_refcnt);
    printf("br_instance.batch_records size: %d\n", PyList_Size(data.py_results));
    data.batch_records_module = br_module;

    printf("12\n");

	as_error_init(&data.error);

    printf("13\n");

	Py_BEGIN_ALLOW_THREADS

	aerospike_batch_operate(self->as, &data.error, 
							policy_batch_p, policy_batch_write_p,
                            &batch, &ops,
							batch_operate_cb, &data);

	Py_END_ALLOW_THREADS

    printf("14\n");
    printf("py_funcname ref count: %d\n", data.func_name->ob_refcnt);
    printf("br_module ref count: %d\n", br_module->ob_refcnt);
    printf("obj_name ref count: %d\n", obj_name->ob_refcnt);
    printf("sys_modules ref count: %d\n", sys_modules->ob_refcnt);
    printf("res_list ref count: %d\n", res_list->ob_refcnt);

    Py_DECREF(data.py_results);
    Py_DECREF(data.func_name);

	as_error_copy(err, &data.error);

    printf("15\n");

	if (err->code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

CLEANUP:
    // don't need below loop?
    printf("In CLEANUP\n");

    if (PyErr_Occurred()) {
        PyErr_Print();
    }

    // printf("br_instance ref count: %d\n", br_instance->ob_refcnt);
    // printf("br_instance.batch_records ref3 count: %d\n", data.py_results->ob_refcnt);
    // printf("br_instance.batch_records size: %d\n", PyList_Size(data.py_results));

	for (unsigned int i = 0; i < unicodeStrVector->size; i++) {
		free(as_vector_get_ptr(unicodeStrVector, i));
	}

    printf("16\n");

	if (batch_exp_list_p) {
		as_exp_destroy(batch_exp_list_p);
	}

	if (batch_write_exp_list_p) {
		as_exp_destroy(batch_write_exp_list_p);
	}

    printf("17\n");

	if (predexp_list_p) {
		as_predexp_list_destroy(&predexp_list);
	}

    printf("18\n");

	as_vector_destroy(unicodeStrVector);

    printf("19\n");

	as_operations_destroy(&ops);

    printf("20\n");

	as_batch_destroy(&batch);

    printf("21\n");

	if (err->code != AEROSPIKE_OK) {
		PyObject *py_err = NULL;
		error_to_pyobject(err, &py_err);
		PyObject *exception_type = raise_exception(err);
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);

		return NULL;
	}

    printf("returning\n");

	return br_instance;
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
			goto ERROR;
    }

    // required arg so don't need to check for NULL
    if ( !PyList_Check(py_keys)) {
			as_error_update(&err, AEROSPIKE_ERR_PARAM,
							"keys should be a list of aerospike key tuples");
			goto ERROR;
    }

	py_results = AerospikeClient_Batch_Operate_Invoke(self, &err, 
											py_keys, py_ops, py_policy_batch,
                                            py_policy_batch_write);

    return py_results;

ERROR:

	if (err.code != AEROSPIKE_OK) {
		PyObject *py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyObject *exception_type = raise_exception(&err);
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
	}

    return NULL;
}
