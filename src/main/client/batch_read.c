#include <Python.h>
#include <aerospike/as_error.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_exp.h>
#include <aerospike/aerospike_batch.h>
#include <aerospike/as_log_macros.h>

#include "types.h"
#include "policy.h"
#include "conversions.h"
#include "exceptions.h"

// Struct for Python User-Data for the Callback
typedef struct {
    PyObject *py_results;
    PyObject *batch_records_module;
    PyObject *func_name;
    AerospikeClient *client;
    bool checking_if_records_exist;
} LocalData;

static bool batch_read_cb(const as_batch_result *results, uint32_t n,
                          void *udata)
{
    // Extract callback user-data
    LocalData *data = (LocalData *)udata;
    as_error err;
    as_error_init(&err);
    PyObject *py_key = NULL;
    PyObject *py_batch_record = NULL;
    bool success = true;

    // Lock Python State
    PyGILState_STATE gstate;
    gstate = PyGILState_Ensure();

    for (uint32_t i = 0; i < n; i++) {

        as_batch_read *res = NULL;
        res = (as_batch_read *)&results[i];

        // NOTE these conversions shouldn't go wrong but if they do, return
        if (key_to_pyobject(&err, res->key, &py_key) != AEROSPIKE_OK) {
            as_log_error("unable to convert res->key at results index: %d", i);
            success = false;
            break;
        }

        // Create BatchRecord instance
        py_batch_record = PyObject_CallMethodObjArgs(
            data->batch_records_module, data->func_name, py_key, NULL);
        if (py_batch_record == NULL) {
            as_log_error("unable to instance BatchRecord at results index: %d",
                         i);
            success = false;
            Py_DECREF(py_key);
            break;
        }
        Py_DECREF(py_key);

        // Initialize BatchRecord instance
        as_batch_result_to_BatchRecord(data->client, &err, res, py_batch_record,
                                       data->checking_if_records_exist);
        if (err.code != AEROSPIKE_OK) {
            as_log_error(
                "as_batch_result_to_BatchRecord failed at results index: %d",
                i);
            success = false;
            Py_DECREF(py_batch_record);
            break;
        }

        PyList_Append(data->py_results, py_batch_record);
        Py_DECREF(py_batch_record);
    }

    PyGILState_Release(gstate);
    return success;
}

PyObject *AerospikeClient_BatchRead(AerospikeClient *self, PyObject *args,
                                    PyObject *kwds)
{
    PyObject *py_keys = NULL;
    PyObject *py_bins = NULL;
    PyObject *py_policy_batch = NULL;
    static char *kwlist[] = {"keys", "bins", "policy", NULL};
    if (PyArg_ParseTupleAndKeywords(args, kwds, "O|OO:batch_read", kwlist,
                                    &py_keys, &py_bins,
                                    &py_policy_batch) == false) {
        return NULL;
    }

    as_error err;
    as_error_init(&err);

    PyObject *br_instance = NULL;

    // required arg so don't need to check for NULL
    if (!PyList_Check(py_keys)) {
        as_error_update(&err, AEROSPIKE_ERR_PARAM,
                        "keys should be a list of aerospike key tuples");
        goto CLEANUP1;
    }

    as_vector tmp_keys;
    Py_ssize_t keys_size = PyList_Size(py_keys);
    as_vector_init(&tmp_keys, sizeof(as_key), keys_size);
    as_vector *tmp_keys_p = &tmp_keys;

    if (!self || !self->as) {
        as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
        goto CLEANUP2;
    }

    if (!self->is_conn_16) {
        as_error_update(&err, AEROSPIKE_ERR_CLUSTER,
                        "No connection to aerospike cluster");
        goto CLEANUP2;
    }

    uint64_t processed_key_count = 0;
    for (int i = 0; i < keys_size; i++) {
        PyObject *py_key = PyList_GetItem(py_keys, i);
        as_key *tmp_key = (as_key *)as_vector_get(&tmp_keys, i);

        Py_INCREF(py_key);
        if (!PyTuple_Check(py_key)) {
            as_error_update(&err, AEROSPIKE_ERR_PARAM,
                            "key should be an aerospike key tuple");
            Py_DECREF(py_key);
            goto CLEANUP2;
        }

        pyobject_to_key(&err, py_key, tmp_key);
        if (err.code != AEROSPIKE_OK) {
            as_error_update(&err, AEROSPIKE_ERR_PARAM,
                            "failed to convert key at index: %d", i);
            Py_DECREF(py_key);
            goto CLEANUP2;
        }

        Py_DECREF(py_key);
        processed_key_count++;
    }

    as_batch batch;
    as_batch_init(&batch, processed_key_count);
    memcpy(batch.keys.entries, tmp_keys.list,
           sizeof(as_key) * processed_key_count);

    as_policy_batch policy_batch;
    as_policy_batch *policy_batch_p = NULL;

    // For expressions conversion.
    as_exp batch_exp_list;
    as_exp *batch_exp_list_p = NULL;

    if (py_policy_batch) {
        if (pyobject_to_policy_batch(
                self, &err, py_policy_batch, &policy_batch, &policy_batch_p,
                &self->as->config.policies.batch, &batch_exp_list,
                &batch_exp_list_p) != AEROSPIKE_OK) {
            goto CLEANUP3;
        }
    }

    // import batch_records helper
    PyObject *br_module = NULL;
    PyObject *sys_modules = PyImport_GetModuleDict();

    Py_INCREF(sys_modules);
    if (PyMapping_HasKeyString(sys_modules,
                               "aerospike_helpers.batch.records")) {
        br_module = PyMapping_GetItemString(sys_modules,
                                            "aerospike_helpers.batch.records");
    }
    else {
        br_module = PyImport_ImportModule("aerospike_helpers.batch.records");
    }
    Py_DECREF(sys_modules);

    if (!br_module) {
        as_error_update(&err, AEROSPIKE_ERR_CLIENT,
                        "Unable to load batch_records module");
        goto CLEANUP3;
    }

    PyObject *obj_name = PyUnicode_FromString("BatchRecords");
    PyObject *res_list = PyList_New(0);
    br_instance =
        PyObject_CallMethodObjArgs(br_module, obj_name, res_list, NULL);

    Py_DECREF(obj_name);
    Py_DECREF(res_list);

    if (!br_instance) {
        as_error_update(&err, AEROSPIKE_ERR_CLIENT,
                        "Unable to instance BatchRecords");
        goto CLEANUP4;
    }

    // Create and initialize callback user-data
    LocalData data;
    // Used to decode record bins
    data.client = self;
    // Used to append BatchRecord instances to the BatchRecords object in this function
    data.py_results = PyObject_GetAttrString(br_instance, "batch_records");
    // Used to create a new BatchRecord instance in the callback function
    data.batch_records_module = br_module;
    data.func_name = PyUnicode_FromString("BatchRecord");
    data.checking_if_records_exist = false;

    Py_ssize_t bin_count = 0;
    const char **filter_bins = NULL;

    // Parse list of bins
    if (py_bins != NULL) {
        if (!PyList_Check(py_bins)) {
            as_error_update(&err, AEROSPIKE_ERR_PARAM,
                            "Bins argument should be a list.");
            goto CLEANUP4;
        }

        bin_count = PyList_Size(py_bins);
        if (bin_count == 0) {
            data.checking_if_records_exist = true;
        }
        else {
            filter_bins = (const char **)malloc(sizeof(char *) * bin_count);

            for (Py_ssize_t i = 0; i < bin_count; i++) {
                PyObject *py_bin = PyList_GetItem(py_bins, i);
                if (PyUnicode_Check(py_bin)) {
                    filter_bins[i] = PyUnicode_AsUTF8(py_bin);
                }
                else {
                    as_error_update(
                        &err, AEROSPIKE_ERR_PARAM,
                        "Bin name should be a string or unicode string.");
                    goto CLEANUP5;
                }
            }
        }
    }

    Py_BEGIN_ALLOW_THREADS

    if (py_bins == NULL) {
        aerospike_batch_get(self->as, &err, policy_batch_p, &batch,
                            batch_read_cb, &data);
    }
    else if (bin_count == 0) {
        aerospike_batch_exists(self->as, &err, policy_batch_p, &batch,
                               batch_read_cb, &data);
    }
    else {
        aerospike_batch_get_bins(self->as, &err, policy_batch_p, &batch,
                                 filter_bins, bin_count, batch_read_cb, &data);
    }

    Py_END_ALLOW_THREADS

    PyObject *py_br_res = PyLong_FromLong((long)err.code);
    PyObject_SetAttrString(br_instance, FIELD_NAME_BATCH_RESULT, py_br_res);
    Py_DECREF(py_br_res);

    as_error_reset(&err);

CLEANUP5:

    free(filter_bins);

CLEANUP4:

    Py_DECREF(br_module);

    Py_DECREF(data.py_results);
    Py_DECREF(data.func_name);

CLEANUP3:

    as_batch_destroy(&batch);

    if (batch_exp_list_p) {
        as_exp_destroy(batch_exp_list_p);
    }

CLEANUP2:

    if (tmp_keys_p) {
        as_vector_destroy(tmp_keys_p);
    }

CLEANUP1:

    if (err.code != AEROSPIKE_OK) {
        raise_exception(&err);
        return NULL;
    }

    return br_instance;
}
