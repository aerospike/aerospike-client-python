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
#include <aerospike/aerospike_batch.h>
#include <aerospike/as_error.h>
#include <aerospike/as_exp.h>
#include <aerospike/as_vector.h>
#include <aerospike/as_geojson.h>
#include <aerospike/as_msgpack_ext.h>

#include "client.h"
#include "conversions.h"
#include "serializer.h"
#include "exceptions.h"
#include "policy.h"
#include "cdt_operation_utils.h"
#include "geo.h"
#include "cdt_types.h"

// TODO define these as exposed aerospike constants so C and Python can both use them
#define FIELD_NAME_BATCH_RECORDS "batch_records"
#define FIELD_NAME_BATCH_TYPE "_type"
#define FIELD_NAME_BATCH_KEY "key"
#define FIELD_NAME_BATCH_OPS "ops"
#define FIELD_NAME_BATCH_RESULT "result"

/*
* convert_batchrecords
* Converts a Python BatchRecords object into a C client as_batch_records struct
*/
static PyObject convert_exp_list(AerospikeClient *self, PyObject *py_obj, as_error *err)
{
	if (!py_obj) {
		as_error_update(err, AEROSPIKE_ERR_CLIENT,
							   "py_obj value is null");
        goto CLEANUP;
	}

    // setup for op conversion
    as_vector *unicodeStrVector = as_vector_create(sizeof(char *), 128);
	as_static_pool static_pool;
	memset(&static_pool, 0, sizeof(static_pool));

    PyObject *py_batch_records = PyObject_GetAttrString(py_obj, FIELD_NAME_BATCH_RECORDS);

    Py_ssize_t py_batch_records_size = PyList_Size(py_batch_records);
    as_batch_records batch_records;
    as_batch_records_inita(batch_records, py_batch_records_size);

    for (Py_ssize_t i = 0; i < batch_records; i++) {
        PyObject *py_batch_record = PyList_GetItem(py_batch_records, i);

        // extract as_batch_base_record fields
        // all batch_records classes should have these
        PyObject *py_key = PyObject_GetAttrString(py_batch_record, FIELD_NAME_BATCH_KEY);
        PyObject *py_batch_type = PyObject_GetAttrString(py_batch_record, FIELD_NAME_BATCH_TYPE);
        PyObject *py_ops_list = PyObject_GetAttrString(py_batch_record, FIELD_NAME_BATCH_OPS);

        as_key key;
        if (pyobject_to_key(err, py_key, &key) != AEROSPIKE_OK) {
            // error
            goto CLEANUP;
        }
        
        Py_ssize_t py_ops_size = PyList_Size(py_ops_list);

        long operation;
        long return_type = -1;
        as_operations *ops = NULL;
        as_operations_inita(ops, py_ops_size);
        for (Py_ssize_t i = 0; i < py_ops_size; i++) {
            PyObject *py_op = PyList_GetItem(py_ops_list, i);

            if (add_op(self, &err, py_op, unicodeStrVector,
                        static_pool, ops, &operation,
                        &return_type) != AEROSPIKE_OK) {
                    // error
                    goto CLEANUP;
                }
        }

        switch (batch_type)
        {
        case AS_BATCH_READ:
            as_error_update(err, AEROSPIKE_ERR_CLIENT, "NOT YET IMPLEMENTED");
            break;
        
        case AS_BATCH_WRITE:;
            as_batch_write_record* wr;
            wr = as_batch_write_reserve(&batch_records);
            memcpy(&wr->key, &key, size_of(as_key));
            wr->ops = &ops;
            break;
        
        case AS_BATCH_APPLY:
            as_error_update(err, AEROSPIKE_ERR_CLIENT, "NOT YET IMPLEMENTED");
            break;
        
        case AS_BATCH_REMOVE:
            as_error_update(err, AEROSPIKE_ERR_CLIENT, "NOT YET IMPLEMENTED");
            break;
        

        default:
            break;
        }
    }

    // TODO get result and populate batch record result
    Py_BEGIN_ALLOW_THREADS

    if (aerospike_batch_operate(self, err, NULL, &batch_records) != AEROSPIKE_OK) {
        // error
        goto CLEANUP;
    }

    Py_END_ALLOW_THREADS

    // populate results
    as_vector* res_list = &batch_records.list;
    for (Py_ssize_t i = 0; i < batch_records; i++) {
        PyObject *py_batch_record = PyList_GetItem(py_batch_records, i);

        as_batch_record *batch_record = as_vector_get(res_list, i);

        PyObject *py_res = PyLong_FromLong((long)batch_record->result);
        PyObject_SetAttrString(py_batch_record, FIELD_NAME_BATCH_RESULT, py_res);

        if (batch_record->result == AEROSPIKE_OK) {
            int py_record_tuple_size = 3; // TODO define this
            PyObject *rec = PyTuple_New(py_record_tuple_size);
            record_to_pyobject(self, err, batch_record->record, batch_record->key, &rec);
        }
    }

CLEANUP:
    // TODO cleanup
	if (err->code != AEROSPIKE_OK) {
		PyObject *py_err = NULL;
		error_to_pyobject(err, &py_err);
		PyObject *exception_type = raise_exception(err);
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return NULL;
	}

	return py_batch_records;
}

/**
 ******************************************************************************************************
 * Returns data about a particular node in the database depending upon the request string.
 *
 * @param self                  AerospikeClient object.
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function.
 * @param kwds                  Dictionary of keywords.
 *
 * Returns information about a host.
 ********************************************************************************************************/
PyObject *AerospikeClient_InfoSingleNode(AerospikeClient *self, PyObject *args,
										 PyObject *kwds)
{
	PyObject *py_host = NULL;
	PyObject *py_policy = NULL;
	PyObject *py_command = NULL;

	as_error err;
	as_error_init(&err);

	static char *kwlist[] = {"command", "host", "policy", NULL};

	if (PyArg_ParseTupleAndKeywords(args, kwds, "OO|O:info_single_node", kwlist,
									&py_command, &py_host,
									&py_policy) == false) {
		return NULL;
	}

	return AerospikeClient_InfoSingleNode_Invoke(&err, self, py_command,
												 py_host, py_policy);
}