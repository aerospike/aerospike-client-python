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

// Cached numpy objects for performance
static PyObject *numpy_module = NULL;
static PyObject *numpy_dtype_func = NULL;
static PyObject *numpy_zeros_func = NULL;

// Struct for Python User-Data for the Callback
typedef struct {
    PyObject *numpy_array;
    PyObject **field_arrays; // Pre-cached field arrays
    AerospikeClient *client;
    int current_row;
    int num_bins;
    char **bin_names;
} LocalData;

// Helper function to initialize numpy functions (called once)
static int init_numpy_functions()
{
    if (numpy_module == NULL) {
        numpy_module = PyImport_ImportModule("numpy");
        if (!numpy_module)
            return -1;

        numpy_dtype_func = PyObject_GetAttrString(numpy_module, "dtype");
        if (!numpy_dtype_func)
            return -1;

        numpy_zeros_func = PyObject_GetAttrString(numpy_module, "zeros");
        if (!numpy_zeros_func)
            return -1;
    }
    return 0;
}

// Helper function for cleanup
static void cleanup_bin_names(char **bin_names, Py_ssize_t bins_size)
{
    if (bin_names) {
        for (Py_ssize_t i = 0; i < bins_size; i++) {
            if (bin_names[i]) {
                free(bin_names[i]);
            }
        }
        free(bin_names);
    }
}

// Optimized callback with pre-cached field arrays
static bool batch_read_cb(const as_batch_result *results, uint32_t n,
                          void *udata)
{
    LocalData *data = (LocalData *)udata;
    as_error err;
    as_error_init(&err);
    bool success = true;

    // Lock Python GIL once for entire batch
    PyGILState_STATE gstate = PyGILState_Ensure();

    // Process all results in batch
    for (uint32_t i = 0; i < n; i++) {
        const as_batch_result *result = &results[i];

        if (result->result == AEROSPIKE_OK && result->record.bins.size > 0) {
            // Process all bins for this record
            for (int j = 0; j < data->num_bins; j++) {
                as_bin_value *bin_val =
                    as_record_get(&result->record, data->bin_names[j]);
                PyObject *bin_value = NULL;

                if (bin_val) {
                    as_val *val = (as_val *)bin_val;
                    if (val_to_pyobject(data->client, &err, val, &bin_value) !=
                        AEROSPIKE_OK) {
                        bin_value = Py_None;
                        Py_INCREF(bin_value);
                    }
                }
                else {
                    bin_value = Py_None;
                    Py_INCREF(bin_value);
                }

                if (bin_value && bin_value != Py_None) {
                    // Use pre-cached field array and single index object
                    PyObject *row_idx = PyLong_FromLong(data->current_row);
                    if (row_idx) {
                        if (PyObject_SetItem(data->field_arrays[j], row_idx,
                                             bin_value) < 0) {
                            PyErr_Clear(); // Clear error and continue
                        }
                        Py_DECREF(row_idx);
                    }
                }
                Py_XDECREF(bin_value);
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
                                    &py_keys, &py_bins,
                                    &py_policy_batch) == false) {
        return NULL;
    }

    as_error err;
    as_error_init(&err);

    // Initialize numpy functions once
    if (init_numpy_functions() < 0) {
        as_error_update(&err, AEROSPIKE_ERR_CLIENT,
                        "Failed to initialize numpy functions");
        goto CLEANUP1;
    }

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

    // Allocate bin_names array
    char **bin_names = (char **)calloc(bins_size, sizeof(char *));
    if (!bin_names) {
        as_error_update(&err, AEROSPIKE_ERR_CLIENT,
                        "Failed to allocate bin_names");
        goto CLEANUP1;
    }

    // Create dtype list for structured array
    PyObject *dtype_list = PyList_New(bins_size);
    if (!dtype_list) {
        as_error_update(&err, AEROSPIKE_ERR_CLIENT,
                        "Failed to create dtype list");
        cleanup_bin_names(bin_names, bins_size);
        goto CLEANUP1;
    }

    // Process bins list once
    for (Py_ssize_t i = 0; i < bins_size; i++) {
        PyObject *bin_spec = PyList_GetItem(py_bins, i);
        if (!PyTuple_Check(bin_spec) || PyTuple_Size(bin_spec) != 2) {
            as_error_update(&err, AEROSPIKE_ERR_PARAM,
                            "Each bin must be a (name, dtype) tuple");
            Py_DECREF(dtype_list);
            cleanup_bin_names(bin_names, i); // Only cleanup allocated ones
            goto CLEANUP1;
        }

        PyObject *bin_name = PyTuple_GetItem(bin_spec, 0);
        PyObject *bin_dtype = PyTuple_GetItem(bin_spec, 1);

        if (!PyUnicode_Check(bin_name) || !PyUnicode_Check(bin_dtype)) {
            as_error_update(&err, AEROSPIKE_ERR_PARAM,
                            "Bin name and dtype must be strings");
            Py_DECREF(dtype_list);
            cleanup_bin_names(bin_names, i);
            goto CLEANUP1;
        }

        // Store bin name for aerospike call
        const char *bin_name_str = PyUnicode_AsUTF8(bin_name);
        bin_names[i] = strdup(bin_name_str);

        // Add to dtype list
        Py_INCREF(bin_spec);
        PyList_SetItem(dtype_list, i, bin_spec);
    }

    // Create numpy dtype using cached function
    PyObject *numpy_dtype =
        PyObject_CallFunction(numpy_dtype_func, "O", dtype_list);
    if (!numpy_dtype) {
        as_error_update(&err, AEROSPIKE_ERR_CLIENT,
                        "Failed to create numpy dtype");
        Py_DECREF(dtype_list);
        cleanup_bin_names(bin_names, bins_size);
        goto CLEANUP1;
    }

    // Create zeros array using cached function
    PyObject *numpy_array = PyObject_CallFunction(numpy_zeros_func, "iO",
                                                  (int)keys_size, numpy_dtype);
    if (!numpy_array) {
        as_error_update(&err, AEROSPIKE_ERR_CLIENT,
                        "Failed to create numpy array");
        Py_DECREF(numpy_dtype);
        Py_DECREF(dtype_list);
        cleanup_bin_names(bin_names, bins_size);
        goto CLEANUP1;
    }

    // Pre-cache field arrays for performance
    PyObject **field_arrays =
        (PyObject **)malloc(bins_size * sizeof(PyObject *));
    if (!field_arrays) {
        as_error_update(&err, AEROSPIKE_ERR_CLIENT,
                        "Failed to allocate field arrays");
        Py_DECREF(numpy_array);
        Py_DECREF(numpy_dtype);
        Py_DECREF(dtype_list);
        cleanup_bin_names(bin_names, bins_size);
        goto CLEANUP1;
    }

    // Cache field arrays once
    for (Py_ssize_t i = 0; i < bins_size; i++) {
        PyObject *field_name = PyUnicode_FromString(bin_names[i]);
        if (field_name) {
            field_arrays[i] = PyObject_GetItem(numpy_array, field_name);
            Py_DECREF(field_name);
        }
        else {
            field_arrays[i] = NULL;
        }
    }

    // Clean up creation objects
    Py_DECREF(numpy_dtype);
    Py_DECREF(dtype_list);

    // Convert keys to as_batch
    as_batch batch;
    if (as_batch_init(&batch, keys_size) == false) {
        as_error_update(&err, AEROSPIKE_ERR_CLIENT,
                        "Failed to initialize batch");
        Py_DECREF(numpy_array);
        // Cleanup field arrays
        for (Py_ssize_t i = 0; i < bins_size; i++) {
            Py_XDECREF(field_arrays[i]);
        }
        free(field_arrays);
        cleanup_bin_names(bin_names, bins_size);
        goto CLEANUP1;
    }

    for (Py_ssize_t i = 0; i < keys_size; i++) {
        PyObject *py_key = PyList_GetItem(py_keys, i);
        if (pyobject_to_key(&err, py_key, &batch.keys.entries[i]) !=
            AEROSPIKE_OK) {
            as_error_update(&err, AEROSPIKE_ERR_PARAM,
                            "Invalid key at index %d", (int)i);
            Py_DECREF(numpy_array);
            // Cleanup field arrays
            for (Py_ssize_t j = 0; j < bins_size; j++) {
                Py_XDECREF(field_arrays[j]);
            }
            free(field_arrays);
            cleanup_bin_names(bin_names, bins_size);
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
        if (pyobject_to_policy_batch(
                self, &err, py_policy_batch, &policy_batch, &policy_batch_p,
                &self->as->config.policies.batch, &batch_exp_list,
                &batch_exp_list_p) != AEROSPIKE_OK) {
            Py_DECREF(numpy_array);
            // Cleanup field arrays
            for (Py_ssize_t i = 0; i < bins_size; i++) {
                Py_XDECREF(field_arrays[i]);
            }
            free(field_arrays);
            cleanup_bin_names(bin_names, bins_size);
            as_batch_destroy(&batch);
            goto CLEANUP1;
        }
    }

    // Set up optimized callback data
    LocalData data;
    data.numpy_array = numpy_array;
    data.field_arrays = field_arrays;
    data.client = self;
    data.current_row = 0;
    data.num_bins = bins_size;
    data.bin_names = bin_names;

    // Call aerospike batch_get_bins
    Py_BEGIN_ALLOW_THREADS

    aerospike_batch_get_bins(self->as, &err, policy_batch_p, &batch,
                             (const char **)bin_names, bins_size, batch_read_cb,
                             &data);

    Py_END_ALLOW_THREADS

    // Final cleanup
    for (Py_ssize_t i = 0; i < bins_size; i++) {
        Py_XDECREF(field_arrays[i]);
    }
    free(field_arrays);
    cleanup_bin_names(bin_names, bins_size);
    as_batch_destroy(&batch);
    if (batch_exp_list_p) {
        as_exp_destroy(batch_exp_list_p);
    }

    if (err.code != AEROSPIKE_OK) {
        Py_DECREF(numpy_array);
        goto CLEANUP1;
    }

    return numpy_array;

CLEANUP1:
    if (err.code != AEROSPIKE_OK) {
        raise_exception(&err);
    }
    return NULL;
}
