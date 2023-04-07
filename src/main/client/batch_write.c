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

#define GET_BATCH_POLICY_FROM_PYOBJECT(__policy, __policy_type,                \
                                       __conversion_func, __batch_type)        \
    {                                                                          \
        PyObject *py___policy =                                                \
            PyObject_GetAttrString(py_batch_record, FIELD_NAME_BATCH_POLICY);  \
        if (py___policy != Py_None) {                                          \
            as_exp *expr = NULL;                                               \
            as_exp *expr_p = expr;                                             \
            if (py___policy != NULL) {                                         \
                __policy = (__policy_type *)malloc(sizeof(__policy_type));     \
                garb->policy_to_free = __policy;                               \
                if (__conversion_func(self, err, py___policy, __policy,        \
                                      &__policy, expr,                         \
                                      &expr_p) != AEROSPIKE_OK) {              \
                    as_error_update(                                           \
                        err, AEROSPIKE_ERR_PARAM,                              \
                        "batch_type: %s, failed to convert policy",            \
                        __batch_type);                                         \
                    Py_DECREF(py___policy);                                    \
                    goto CLEANUP0;                                             \
                }                                                              \
                garb->expressions_to_free = expr_p;                            \
            }                                                                  \
            else {                                                             \
                as_error_update(err, AEROSPIKE_ERR_PARAM,                      \
                                "batch_type: %s, policy must be a dict",       \
                                __batch_type);                                 \
                Py_DECREF(py___policy);                                        \
                goto CLEANUP0;                                                 \
            }                                                                  \
        }                                                                      \
        Py_DECREF(py___policy);                                                \
    }

// TODO replace this with type checking the batch_records
// and cleaning up at the end, no struct needed that way
typedef struct garbage_s {

    as_operations *ops_to_free;

    /**
     * NOTE this isn't used for anything but freeing the polices
     * so using void for all should be fine.
     */
    void *policy_to_free;

    as_exp *expressions_to_free;

    as_list *udf_args_to_free;

} garbage;

void garbage_destroy(garbage *garb)
{
    as_exp *expr = garb->expressions_to_free;
    if (expr != NULL) {
        as_exp_destroy(expr);
    }

    void *pol = garb->policy_to_free;
    if (pol != NULL) {
        free(pol);
    }

    as_operations *ops = garb->ops_to_free;
    if (ops != NULL) {
        as_operations_destroy(ops);
    }

    as_list *args_l = garb->udf_args_to_free;
    if (args_l != NULL) {
        as_list_destroy(args_l);
    }
}

/*
* AerospikeClient_BatchWriteInvoke
* Converts Python BatchRecords objects into a C client as_batch_records struct.
* Then calls aerospike_batch_records.
*/
static PyObject *AerospikeClient_BatchWriteInvoke(AerospikeClient *self,
                                                  as_error *err,
                                                  PyObject *py_policy,
                                                  PyObject *py_obj)
{
    Py_ssize_t py_batch_records_size = 0;
    as_batch_records batch_records;
    as_batch_records *batch_records_p = NULL;

    as_policy_batch batch_policy;
    as_policy_batch *batch_policy_p = NULL;
    as_exp exp_list;
    as_exp *exp_list_p = NULL;

    PyObject *py_batch_type = NULL;
    PyObject *py_key = NULL;
    PyObject *py_batch_records = NULL;
    PyObject *py_meta = NULL, *py_ops_list = NULL;

    // setup for op conversion
    as_vector *unicodeStrVector = as_vector_create(sizeof(char *), 128);
    as_static_pool static_pool;
    memset(&static_pool, 0, sizeof(static_pool));

    as_vector garbage_list;
    as_vector *garbage_list_p = NULL;

    if (!self || !self->as) {
        as_error_update(err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
        goto CLEANUP4;
    }

    if (!self->is_conn_16) {
        as_error_update(err, AEROSPIKE_ERR_CLUSTER,
                        "No connection to aerospike cluster");
        goto CLEANUP4;
    }

    if (py_obj == NULL) {
        as_error_update(err, AEROSPIKE_ERR_PARAM, "py_obj value is null");
        goto CLEANUP4;
    }

    if (py_policy != NULL) {
        if (pyobject_to_policy_batch(self, err, py_policy, &batch_policy,
                                     &batch_policy_p,
                                     &self->as->config.policies.batch,
                                     &exp_list, &exp_list_p) != AEROSPIKE_OK) {
            goto CLEANUP4;
        }
    }

    // TODO check that py_object is an instance of class

    py_batch_records = PyObject_GetAttrString(py_obj, FIELD_NAME_BATCH_RECORDS);
    if (py_batch_records == NULL || !PyList_Check(py_batch_records)) {
        as_error_update(err, AEROSPIKE_ERR_PARAM,
                        "%s must be a list of BatchRecord",
                        FIELD_NAME_BATCH_RECORDS);
        goto CLEANUP4;
    }

    py_batch_records_size = PyList_Size(py_batch_records);
    as_batch_records_init(&batch_records, py_batch_records_size);
    batch_records_p = &batch_records;

    as_vector_init(&garbage_list, sizeof(garbage), py_batch_records_size);
    for (Py_ssize_t i = 0; i < py_batch_records_size; i++) {
        garbage garb_to_free = {0};
        as_vector_set(&garbage_list, i, (void *)&garb_to_free);
    }
    garbage_list_p = &garbage_list;

    for (Py_ssize_t i = 0; i < py_batch_records_size; i++) {
        garbage *garb = as_vector_get(&garbage_list, i);
        PyObject *py_batch_record = PyList_GetItem(py_batch_records, i);
        // TODO check that this is an instance/subclass on BatchRecord
        if (py_batch_record == NULL) {
            as_error_update(
                err, AEROSPIKE_ERR_PARAM,
                "py_batch_record is NULL, %s must be a list of BatchRecord",
                FIELD_NAME_BATCH_RECORDS);
            goto CLEANUP4;
        }

        // extract as_batch_base_record fields
        // all batch_records classes should have these
        py_key = PyObject_GetAttrString(py_batch_record, FIELD_NAME_BATCH_KEY);
        if (py_key == NULL || !PyTuple_Check(py_key)) {
            as_error_update(err, AEROSPIKE_ERR_PARAM,
                            "py_key is NULL or not a tuple, %s must be a "
                            "aerospike key tuple",
                            FIELD_NAME_BATCH_KEY);
            goto CLEANUP3;
        }

        py_batch_type =
            PyObject_GetAttrString(py_batch_record, FIELD_NAME_BATCH_TYPE);
        if (py_batch_type == NULL ||
            !PyLong_Check(
                py_batch_type)) { // TODO figure away around this being an enum
            as_error_update(err, AEROSPIKE_ERR_PARAM,
                            "py_batch_type is NULL or not an int, %s must be "
                            "an int from batch_records._Types",
                            FIELD_NAME_BATCH_TYPE);
            goto CLEANUP2;
        }

        // Not checking for overflow here because type is private in python
        // so we shouldn't get anything unexpected.
        uint8_t batch_type = 0;
        batch_type = PyLong_AsLong(py_batch_type);
        if (PyErr_Occurred() && PyErr_ExceptionMatches(PyExc_OverflowError)) {
            as_error_update(err, AEROSPIKE_ERR_PARAM,
                            "py_batch_type aka %s is too large for C long",
                            FIELD_NAME_BATCH_TYPE);
            goto CLEANUP1;
        }

        py_ops_list =
            PyObject_GetAttrString(py_batch_record, FIELD_NAME_BATCH_OPS);
        if (py_ops_list == NULL || !PyList_Check(py_ops_list) ||
            !PyList_Size(py_ops_list)) {

            // batch Read can have None ops if it is using read_all_bins
            if ((batch_type == BATCH_TYPE_READ && py_ops_list != Py_None) ||
                batch_type == BATCH_TYPE_WRITE) {
                as_error_update(err, AEROSPIKE_ERR_PARAM,
                                "py_ops_list is NULL or not a list, %s must be "
                                "a list of aerospike operation dicts",
                                FIELD_NAME_BATCH_OPS);

                goto CLEANUP1;
            }

            // the batch record object had no ops attribute but some don't, so this is ok.
            if (PyErr_Occurred() &&
                PyErr_ExceptionMatches(PyExc_AttributeError)) {
                PyErr_Clear();
            }
        }

        py_meta = NULL;
        if (batch_type == AS_BATCH_READ || batch_type == AS_BATCH_WRITE) {
            py_meta =
                PyObject_GetAttrString(py_batch_record, FIELD_NAME_BATCH_META);
        }

        Py_ssize_t py_ops_size = 0;
        if (py_ops_list != NULL && py_ops_list != Py_None) {
            py_ops_size = PyList_Size(py_ops_list);
        }

        long operation = 0;
        long return_type = -1;

        as_operations *ops = NULL;
        if ((batch_type == AS_BATCH_READ || batch_type == AS_BATCH_WRITE) &&
            (py_ops_size || (py_meta != NULL && py_meta != Py_None))) {

            ops = as_operations_new(py_ops_size);
            garb->ops_to_free = ops;

            if (py_meta) {
                if (check_and_set_meta(py_meta, ops, err) != AEROSPIKE_OK) {
                    goto CLEANUP0;
                }
            }

            for (Py_ssize_t i = 0; i < py_ops_size; i++) {

                PyObject *py_op = PyList_GetItem(py_ops_list, i);
                if (py_op == NULL || !PyDict_Check(py_op)) {
                    as_error_update(
                        err, AEROSPIKE_ERR_PARAM,
                        "py_op is NULL or not a dict, %s must be a dict \
                                    produced by an aerospike operation helper",
                        FIELD_NAME_BATCH_OPS);
                    goto CLEANUP0;
                }

                if (add_op(self, err, py_op, unicodeStrVector, &static_pool,
                           ops, &operation, &return_type) != AEROSPIKE_OK) {
                    goto CLEANUP0;
                }
            }
        }
        switch (batch_type) {
        case AS_BATCH_READ:;

            as_policy_batch_read *r_policy = NULL;
            GET_BATCH_POLICY_FROM_PYOBJECT(r_policy, as_policy_batch_read,
                                           pyobject_to_batch_read_policy,
                                           "Read")

            PyObject *py_read_all_bins =
                PyObject_GetAttrString(py_batch_record, "read_all_bins");
            // Not checking for NULL since batch Read should always have read_all_bins
            bool read_all_bins = PyObject_IsTrue(py_read_all_bins);
            Py_DECREF(py_read_all_bins);

            as_batch_read_record *rr;
            rr = as_batch_read_reserve(&batch_records);

            if (pyobject_to_key(err, py_key, &rr->key) != AEROSPIKE_OK) {
                goto CLEANUP0;
            }

            rr->ops = ops;
            rr->read_all_bins = read_all_bins;
            rr->policy = r_policy;

            break;

        case AS_BATCH_WRITE:;

            as_policy_batch_write *w_policy = NULL;
            GET_BATCH_POLICY_FROM_PYOBJECT(w_policy, as_policy_batch_write,
                                           pyobject_to_batch_write_policy,
                                           "Write")

            as_batch_write_record *wr;
            wr = as_batch_write_reserve(&batch_records);

            if (pyobject_to_key(err, py_key, &wr->key) != AEROSPIKE_OK) {
                goto CLEANUP0;
            }

            wr->ops = ops;
            wr->policy = w_policy;

            break;

        case AS_BATCH_APPLY:;

            as_policy_batch_apply *a_policy = NULL;
            GET_BATCH_POLICY_FROM_PYOBJECT(a_policy, as_policy_batch_apply,
                                           pyobject_to_batch_apply_policy,
                                           "Apply")

            PyObject *py_mod = PyObject_GetAttrString(py_batch_record,
                                                      FIELD_NAME_BATCH_MODULE);
            if (py_mod == NULL || !PyUnicode_Check(py_mod)) {
                as_error_update(err, AEROSPIKE_ERR_PARAM, "%s must be a string",
                                FIELD_NAME_BATCH_MODULE);
                Py_XDECREF(py_mod);
                goto CLEANUP0;
            }
            Py_DECREF(py_mod);
            const char *mod = PyUnicode_AsUTF8(py_mod);

            PyObject *py_func = PyObject_GetAttrString(
                py_batch_record, FIELD_NAME_BATCH_FUNCTION);
            if (py_func == NULL || !PyUnicode_Check(py_func)) {
                as_error_update(err, AEROSPIKE_ERR_PARAM, "%s must be a string",
                                FIELD_NAME_BATCH_FUNCTION);
                Py_XDECREF(py_func);
                goto CLEANUP0;
            }
            Py_DECREF(py_func);
            const char *func = PyUnicode_AsUTF8(py_func);

            PyObject *py_args =
                PyObject_GetAttrString(py_batch_record, FIELD_NAME_BATCH_ARGS);
            if (py_args == NULL || !PyList_Check(py_args)) {
                as_error_update(err, AEROSPIKE_ERR_PARAM,
                                "%s must be a list of arguments for the UDF",
                                FIELD_NAME_BATCH_ARGS);
                Py_XDECREF(py_args);
                goto CLEANUP0;
            }

            as_list *arglist = NULL;
            pyobject_to_list(self, err, py_args, &arglist, &static_pool,
                             SERIALIZER_PYTHON);
            if (err->code != AEROSPIKE_OK) {
                Py_DECREF(py_args);
                goto CLEANUP0;
            }
            Py_DECREF(py_args);
            garb->udf_args_to_free = arglist;

            as_batch_apply_record *ar;
            ar = as_batch_apply_reserve(&batch_records);

            if (pyobject_to_key(err, py_key, &ar->key) != AEROSPIKE_OK) {
                goto CLEANUP0;
            }

            ar->module = mod;
            ar->function = func;
            ar->arglist = arglist;
            ar->policy = a_policy;

            break;

        case AS_BATCH_REMOVE:;

            as_policy_batch_remove *re_policy = NULL;
            GET_BATCH_POLICY_FROM_PYOBJECT(re_policy, as_policy_batch_remove,
                                           pyobject_to_batch_remove_policy,
                                           "Remove")

            as_batch_remove_record *rer;
            rer = as_batch_remove_reserve(&batch_records);

            if (pyobject_to_key(err, py_key, &rer->key) != AEROSPIKE_OK) {
                goto CLEANUP0;
            }

            rer->policy = re_policy;

            break;

        default:
            as_error_update(err, AEROSPIKE_ERR_PARAM, "batch_type unkown: %d",
                            batch_type);
            goto CLEANUP0;
            break;
        }
        Py_DECREF(py_key);
        Py_DECREF(py_batch_type);
        Py_XDECREF(py_ops_list);
    }

    Py_BEGIN_ALLOW_THREADS

    aerospike_batch_write(self->as, err, batch_policy_p, &batch_records);

    Py_END_ALLOW_THREADS

    PyObject *py_bw_res = PyLong_FromLong((long)err->code);
    if (PyObject_HasAttrString(py_obj, FIELD_NAME_BATCH_RESULT)) {
        PyObject_DelAttrString(py_obj, FIELD_NAME_BATCH_RESULT);
    }
    PyObject_SetAttrString(py_obj, FIELD_NAME_BATCH_RESULT, py_bw_res);
    Py_DECREF(py_bw_res);

    as_error_reset(err);

    // populate results
    as_vector *res_list = &batch_records.list;

    for (Py_ssize_t i = 0; i < py_batch_records_size; i++) {
        PyObject *py_batch_record = PyList_GetItem(py_batch_records, i);

        as_batch_base_record *batch_record = as_vector_get(res_list, i);

        as_status *result_code = &(batch_record->result);
        as_key *requested_key = &(batch_record->key);
        as_record *result_rec = &(batch_record->record);
        bool in_doubt = batch_record->in_doubt;

        PyObject *py_res = PyLong_FromLong((long)*result_code);
        if (PyObject_HasAttrString(py_batch_record, FIELD_NAME_BATCH_RESULT)) {
            PyObject_DelAttrString(py_batch_record, FIELD_NAME_BATCH_RESULT);
        }
        PyObject_SetAttrString(py_batch_record, FIELD_NAME_BATCH_RESULT,
                               py_res);
        Py_DECREF(py_res);

        PyObject *py_in_doubt = PyBool_FromLong((long)in_doubt);
        PyObject_SetAttrString(py_batch_record, FIELD_NAME_BATCH_INDOUBT,
                               py_in_doubt);
        Py_DECREF(py_in_doubt);

        if (*result_code == AEROSPIKE_OK) {
            PyObject *rec = NULL;

            if (PyObject_HasAttrString(py_batch_record,
                                       FIELD_NAME_BATCH_RECORD)) {
                PyObject_DelAttrString(py_batch_record,
                                       FIELD_NAME_BATCH_RECORD);
            }

            if (result_rec) {
                record_to_pyobject(self, err, result_rec, requested_key, &rec);
                PyObject_SetAttrString(py_batch_record, FIELD_NAME_BATCH_RECORD,
                                       rec);
                Py_DECREF(rec);
            }
            else {
                Py_INCREF(Py_None);
                PyObject_SetAttrString(py_batch_record, FIELD_NAME_BATCH_RECORD,
                                       Py_None);
            }
        }
    }

    goto CLEANUP3;

CLEANUP0:
    Py_XDECREF(py_meta);
    Py_XDECREF(py_ops_list);
CLEANUP1:
    Py_XDECREF(py_batch_type);
CLEANUP2:
    Py_XDECREF(py_key);
CLEANUP3:
    Py_XDECREF(py_batch_records);
CLEANUP4:
    if (garbage_list_p != NULL) {
        for (int i = 0; i < py_batch_records_size; i++) {
            garbage *garb_to_free = as_vector_get(&garbage_list, i);
            garbage_destroy(garb_to_free);
        }
        as_vector_destroy(garbage_list_p);
    }

    if (batch_records_p != NULL) {
        as_batch_records_destroy(&batch_records);
    }

    for (unsigned int i = 0; i < unicodeStrVector->size; i++) {
        free(as_vector_get_ptr(unicodeStrVector, i));
    }

    as_vector_destroy(unicodeStrVector);

    if (exp_list_p != NULL) {
        as_exp_destroy(exp_list_p);
    }

    if (err->code != AEROSPIKE_OK) {
        raise_exception(err);
        return NULL;
    }

    Py_IncRef(py_obj);
    return py_obj;
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
PyObject *AerospikeClient_BatchWrite(AerospikeClient *self, PyObject *args,
                                     PyObject *kwds)
{
    PyObject *py_policy = NULL;
    PyObject *py_batch_recs = NULL;

    as_error err;
    as_error_init(&err);

    static char *kwlist[] = {"batch_records", "policy_batch", NULL};

    if (PyArg_ParseTupleAndKeywords(args, kwds, "O|O:batch_write", kwlist,
                                    &py_batch_recs, &py_policy) == false) {
        return NULL;
    }

    return AerospikeClient_BatchWriteInvoke(self, &err, py_policy,
                                            py_batch_recs);
}
