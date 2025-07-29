#include <Python.h>
#define NPY_NO_DEPRECATED_API NPY_1_7_API_VERSION
#include <numpy/arrayobject.h>
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
    PyObject *numpy_array;
    AerospikeClient *client;
    int current_row;
    int num_bins;
    char **bin_names;
} LocalData;

static bool batch_read_cb(const as_batch_result *results, uint32_t n,
                          void *udata)
{
    LocalData *data = (LocalData *)udata;
    as_error err;
    as_error_init(&err);
    bool success = true;

    // Lock Python GIL
    PyGILState_STATE gstate = PyGILState_Ensure();

    for (uint32_t i = 0; i < n; i++) {
        const as_batch_result *result = &results[i];
        
        if (result->result == AEROSPIKE_OK && result->record.bins.size > 0) {
            // Set each field individually using Python API
            for (int j = 0; j < data->num_bins; j++) {
                PyObject *bin_value = NULL;
                
                // Get bin value from aerospike record
                as_bin_value *bin_val = as_record_get(&result->record, data->bin_names[j]);
                if (bin_val) {
                    as_val *val = (as_val*)bin_val;
                    if (val_to_pyobject(data->client, &err, val, &bin_value) != AEROSPIKE_OK) {
                        // If conversion fails, use appropriate default based on field type
                        bin_value = Py_None;
                        Py_INCREF(bin_value);
                    }
                } else {
                    // If bin doesn't exist, use appropriate default based on field type
                    bin_value = Py_None;
                    Py_INCREF(bin_value);
                }
                
                if (bin_value) {
                    // Create index tuple (row_index,)
                    PyObject *row_idx = PyLong_FromLong(data->current_row);
                    if (row_idx) {
                        // Get field name for this column
                        PyObject *field_name = PyUnicode_FromString(data->bin_names[j]);
                        if (field_name) {
                            // Get the field array: numpy_array[field_name]
                            PyObject *field_array = PyObject_GetItem(data->numpy_array, field_name);
                            if (field_array) {
                                // Set the value: field_array[row_index] = bin_value
                                if (bin_value != Py_None) {
                                    if (PyObject_SetItem(field_array, row_idx, bin_value) < 0) {
                                        // Clear the error and continue
                                        PyErr_Clear();
                                    }
                                }
                                Py_DECREF(field_array);
                            } else {
                                PyErr_Clear();
                            }
                            Py_DECREF(field_name);
                        }
                        Py_DECREF(row_idx);
                    }
                    Py_DECREF(bin_value);
                }
            }
        }
        
        data->current_row++;
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
    
    // bins is now required
    if (PyArg_ParseTupleAndKeywords(args, kwds, "OO|O:batch_read", kwlist,
                                    &py_keys, &py_bins, &py_policy_batch) == false) {
        return NULL;
    }

    as_error err;
    as_error_init(&err);

    // Validate required arguments
    if (!PyList_Check(py_keys)) {
        as_error_update(&err, AEROSPIKE_ERR_PARAM,
                        "keys should be a list of aerospike key tuples");
        goto CLEANUP1;
    }

    if (!PyList_Check(py_bins)) {
        as_error_update(&err, AEROSPIKE_ERR_PARAM,
                        "bins should be a list of (name, dtype) tuples");
        goto CLEANUP1;
    }

    Py_ssize_t keys_size = PyList_Size(py_keys);
    Py_ssize_t bins_size = PyList_Size(py_bins);

    if (bins_size == 0) {
        as_error_update(&err, AEROSPIKE_ERR_PARAM, "bins cannot be empty");
        goto CLEANUP1;
    }

    // Create bin_names array for aerospike call
    char **bin_names = (char**)malloc(bins_size * sizeof(char*));
    if (!bin_names) {
        as_error_update(&err, AEROSPIKE_ERR_CLIENT, "Failed to allocate bin_names");
        goto CLEANUP1;
    }

    // Parse bins and create structured dtype using numpy Python API
    PyObject *numpy_module = PyImport_ImportModule("numpy");
    if (!numpy_module) {
        as_error_update(&err, AEROSPIKE_ERR_CLIENT, "Failed to import numpy");
        for (Py_ssize_t i = 0; i < bins_size; i++) {
            free(bin_names[i]);
        }
        free(bin_names);
        goto CLEANUP1;
    }

    PyObject *dtype_func = PyObject_GetAttrString(numpy_module, "dtype");
    if (!dtype_func) {
        as_error_update(&err, AEROSPIKE_ERR_CLIENT, "Failed to get numpy.dtype");
        Py_DECREF(numpy_module);
        for (Py_ssize_t i = 0; i < bins_size; i++) {
            free(bin_names[i]);
        }
        free(bin_names);
        goto CLEANUP1;
    }

    // Create dtype list for structured array
    PyObject *dtype_list = PyList_New(bins_size);
    if (!dtype_list) {
        as_error_update(&err, AEROSPIKE_ERR_CLIENT, "Failed to create dtype list");
        Py_DECREF(dtype_func);
        Py_DECREF(numpy_module);
        for (Py_ssize_t i = 0; i < bins_size; i++) {
            free(bin_names[i]);
        }
        free(bin_names);
        goto CLEANUP1;
    }

    for (Py_ssize_t i = 0; i < bins_size; i++) {
        PyObject *bin_spec = PyList_GetItem(py_bins, i);
        if (!PyTuple_Check(bin_spec) || PyTuple_Size(bin_spec) != 2) {
            as_error_update(&err, AEROSPIKE_ERR_PARAM,
                            "Each bin must be a (name, dtype) tuple");
            Py_DECREF(dtype_list);
            Py_DECREF(dtype_func);
            Py_DECREF(numpy_module);
            for (Py_ssize_t j = 0; j < i; j++) {
                free(bin_names[j]);
            }
            free(bin_names);
            goto CLEANUP1;
        }

        PyObject *bin_name = PyTuple_GetItem(bin_spec, 0);
        PyObject *bin_dtype = PyTuple_GetItem(bin_spec, 1);
        
        if (!PyUnicode_Check(bin_name) || !PyUnicode_Check(bin_dtype)) {
            as_error_update(&err, AEROSPIKE_ERR_PARAM,
                            "Bin name and dtype must be strings");
            Py_DECREF(dtype_list);
            Py_DECREF(dtype_func);
            Py_DECREF(numpy_module);
            for (Py_ssize_t j = 0; j < i; j++) {
                free(bin_names[j]);
            }
            free(bin_names);
            goto CLEANUP1;
        }

        // Store bin name for aerospike call
        const char *bin_name_str = PyUnicode_AsUTF8(bin_name);
        bin_names[i] = strdup(bin_name_str);
        
        // Add to dtype list
        Py_INCREF(bin_spec);
        PyList_SetItem(dtype_list, i, bin_spec);
    }

    // Create numpy dtype from the list using Python API
    PyObject *numpy_dtype = PyObject_CallFunction(dtype_func, "O", dtype_list);
    if (!numpy_dtype) {
        as_error_update(&err, AEROSPIKE_ERR_CLIENT, "Failed to create numpy dtype");
        Py_DECREF(dtype_list);
        Py_DECREF(dtype_func);
        Py_DECREF(numpy_module);
        for (Py_ssize_t i = 0; i < bins_size; i++) {
            free(bin_names[i]);
        }
        free(bin_names);
        goto CLEANUP1;
    }

    // Create zeros array with the structured dtype
    PyObject *zeros_func = PyObject_GetAttrString(numpy_module, "zeros");
    if (!zeros_func) {
        as_error_update(&err, AEROSPIKE_ERR_CLIENT, "Failed to get numpy.zeros");
        Py_DECREF(numpy_dtype);
        Py_DECREF(dtype_list);
        Py_DECREF(dtype_func);
        Py_DECREF(numpy_module);
        for (Py_ssize_t i = 0; i < bins_size; i++) {
            free(bin_names[i]);
        }
        free(bin_names);
        goto CLEANUP1;
    }

    PyObject *numpy_array = PyObject_CallFunction(zeros_func, "iO", (int)keys_size, numpy_dtype);
    if (!numpy_array) {
        as_error_update(&err, AEROSPIKE_ERR_CLIENT, "Failed to create numpy array");
        Py_DECREF(zeros_func);
        Py_DECREF(numpy_dtype);
        Py_DECREF(dtype_list);
        Py_DECREF(dtype_func);
        Py_DECREF(numpy_module);
        for (Py_ssize_t i = 0; i < bins_size; i++) {
            free(bin_names[i]);
        }
        free(bin_names);
        goto CLEANUP1;
    }

    // Clean up creation objects
    Py_DECREF(zeros_func);
    Py_DECREF(numpy_dtype);
    Py_DECREF(dtype_list);
    Py_DECREF(dtype_func);
    Py_DECREF(numpy_module);

    // Convert keys to as_batch
    as_batch batch;
    if (as_batch_init(&batch, keys_size) == false) {
        as_error_update(&err, AEROSPIKE_ERR_CLIENT, "Failed to initialize batch");
        Py_DECREF(numpy_array);
        for (Py_ssize_t i = 0; i < bins_size; i++) {
            free(bin_names[i]);
        }
        free(bin_names);
        goto CLEANUP1;
    }
    
    for (Py_ssize_t i = 0; i < keys_size; i++) {
        PyObject *py_key = PyList_GetItem(py_keys, i);
        if (pyobject_to_key(&err, py_key, &batch.keys.entries[i]) != AEROSPIKE_OK) {
            as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid key at index %d", (int)i);
            Py_DECREF(numpy_array);
            for (Py_ssize_t j = 0; j < bins_size; j++) {
                free(bin_names[j]);
            }
            free(bin_names);
            as_batch_destroy(&batch);
            goto CLEANUP1;
        }
    }

    // Set up policy
    as_policy_batch *policy_batch_p = NULL;
    as_policy_batch policy_batch;
    as_exp batch_exp_list;
    as_exp *batch_exp_list_p = NULL;
    
    if (py_policy_batch && py_policy_batch != Py_None) {
        if (pyobject_to_policy_batch(self, &err, py_policy_batch, &policy_batch,
                                     &policy_batch_p, &self->as->config.policies.batch,
                                     &batch_exp_list, &batch_exp_list_p) != AEROSPIKE_OK) {
            Py_DECREF(numpy_array);
            for (Py_ssize_t i = 0; i < bins_size; i++) {
                free(bin_names[i]);
            }
            free(bin_names);
            as_batch_destroy(&batch);
            goto CLEANUP1;
        }
    }

    // Set up callback data
    LocalData data;
    data.numpy_array = numpy_array;
    data.client = self;
    data.current_row = 0;
    data.num_bins = bins_size;
    data.bin_names = bin_names;

    // Call aerospike batch_get_bins
    Py_BEGIN_ALLOW_THREADS

    aerospike_batch_get_bins(self->as, &err, policy_batch_p, &batch,
                             (const char **)bin_names, bins_size, batch_read_cb, &data);

    Py_END_ALLOW_THREADS

    if (err.code != AEROSPIKE_OK) {
        Py_DECREF(numpy_array);
        for (Py_ssize_t i = 0; i < bins_size; i++) {
            free(bin_names[i]);
        }
        free(bin_names);
        as_batch_destroy(&batch);
        if (batch_exp_list_p) {
            as_exp_destroy(batch_exp_list_p);
        }
        goto CLEANUP1;
    }

    // Clean up and return numpy array
    for (Py_ssize_t i = 0; i < bins_size; i++) {
        free(bin_names[i]);
    }
    free(bin_names);
    as_batch_destroy(&batch);
    if (batch_exp_list_p) {
        as_exp_destroy(batch_exp_list_p);
    }

    return numpy_array;

CLEANUP1:
    if (err.code != AEROSPIKE_OK) {
        raise_exception(&err);
    }
    return NULL;
}
