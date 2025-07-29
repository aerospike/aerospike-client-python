#include <Python.h>
#define NPY_NO_DEPRECATED_API NPY_1_7_API_VERSION
#include <numpy/arrayobject.h>
#include <aerospike/as_error.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_exp.h>
#include <aerospike/aerospike_batch.h>
#include <aerospike/as_log_macros.h>
#include <time.h>
#include <sys/time.h>

#include "types.h"
#include "policy.h"
#include "conversions.h"
#include "exceptions.h"

// Debug flag for latency measurement - can be controlled via environment variable
#ifndef ENABLE_LATENCY_DEBUG
    #define ENABLE_LATENCY_DEBUG 1
#endif

// Cached numpy objects for performance
static PyObject *numpy_module = NULL;
static PyObject *numpy_dtype_func = NULL;
static PyObject *numpy_zeros_func = NULL;

// Struct for Python User-Data for the Callback - Optimized with caching
typedef struct {
    PyObject *numpy_array;
    PyObject **field_arrays; // Pre-cached field arrays
    PyObject **row_indices;  // Pre-cached row index objects
    AerospikeClient *client;
    uint32_t current_row; // Use uint32_t for better alignment
    uint32_t num_bins;    // Use uint32_t for better alignment
    uint32_t max_rows;    // Pre-calculated for bounds checking
    char **bin_names;
    bool has_error; // Single error flag for early termination

    // Latency debugging fields
    struct timespec start_callback_time;
    double total_callback_time_ms;
    uint32_t callback_count;
} LocalData;

// Utility function to get current time in microseconds
static inline double get_time_microseconds()
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec * 1000000.0 + (double)ts.tv_nsec / 1000.0;
}

// Utility function to calculate time difference in milliseconds
static inline double time_diff_ms(double start_us, double end_us)
{
    return (end_us - start_us) / 1000.0;
}

// Helper function to initialize numpy functions (called once)
static inline int init_numpy_functions()
{
    if (__builtin_expect(numpy_module == NULL, 0)) {
        numpy_module = PyImport_ImportModule("numpy");
        if (__builtin_expect(!numpy_module, 0))
            return -1;

        numpy_dtype_func = PyObject_GetAttrString(numpy_module, "dtype");
        if (__builtin_expect(!numpy_dtype_func, 0))
            return -1;

        numpy_zeros_func = PyObject_GetAttrString(numpy_module, "zeros");
        if (__builtin_expect(!numpy_zeros_func, 0))
            return -1;
    }
    return 0;
}

// Helper function for cleanup
static void cleanup_bin_names(char **bin_names, Py_ssize_t bins_size)
{
    if (__builtin_expect(bin_names != NULL, 1)) {
        for (Py_ssize_t i = 0; i < bins_size; i++) {
            if (bin_names[i]) {
                free(bin_names[i]);
            }
        }
        free(bin_names);
    }
}

// Helper function to cleanup row indices
static void cleanup_row_indices(PyObject **row_indices, uint32_t max_rows)
{
    if (__builtin_expect(row_indices != NULL, 1)) {
        for (uint32_t i = 0; i < max_rows; i++) {
            Py_XDECREF(row_indices[i]);
        }
        free(row_indices);
    }
}

// Highly optimized callback with maximum caching and minimal overhead
static bool batch_read_cb(const as_batch_result *results, uint32_t n,
                          void *udata)
{
    LocalData *data = (LocalData *)udata;

#if ENABLE_LATENCY_DEBUG
    double callback_start_time = get_time_microseconds();
#endif

    // Early termination if previous error occurred
    if (__builtin_expect(data->has_error, 0)) {
        return false;
    }

    // Lock Python GIL once for entire batch
    PyGILState_STATE gstate = PyGILState_Ensure();

    // Hot path: Process all results with optimized loop
    const as_batch_result *result = results;
    for (uint32_t i = 0; i < n; i++, result++) {

        // Bounds checking
        if (__builtin_expect(data->current_row >= data->max_rows, 0)) {
            data->has_error = true;
            break;
        }

        // Fast path: Check if we have valid data
        if (__builtin_expect(result->result == AEROSPIKE_OK &&
                                 result->record.bins.size > 0,
                             1)) {

            // Pre-fetch row index (already cached)
            PyObject *row_idx = data->row_indices[data->current_row];

            // Process all bins for this record with optimized inner loop
            for (uint32_t j = 0; j < data->num_bins; j++) {
                as_bin_value *bin_val =
                    as_record_get(&result->record, data->bin_names[j]);

                if (__builtin_expect(bin_val != NULL, 1)) {
                    // Hot path: Direct conversion without error checking overhead
                    PyObject *bin_value = NULL;
                    as_error err;

                    if (__builtin_expect(val_to_pyobject(data->client, &err,
                                                         (as_val *)bin_val,
                                                         &bin_value) ==
                                             AEROSPIKE_OK,
                                         1)) {
                        // Ultra-fast path: Direct assignment with cached objects
                        if (__builtin_expect(
                                PyObject_SetItem(data->field_arrays[j], row_idx,
                                                 bin_value) == 0,
                                1)) {
                            // Success - no action needed
                        }
                        else {
                            // Clear error and continue (non-blocking)
                            PyErr_Clear();
                        }
                        Py_DECREF(bin_value);
                    }
                    // Note: Skip None assignment for missing bins to avoid unnecessary overhead
                }
            }
        }

        data->current_row++;
    }

    PyGILState_Release(gstate);

#if ENABLE_LATENCY_DEBUG
    double callback_end_time = get_time_microseconds();
    double callback_duration_ms =
        time_diff_ms(callback_start_time, callback_end_time);
    data->total_callback_time_ms += callback_duration_ms;
    data->callback_count++;
#endif

    return !data->has_error;
}

PyObject *AerospikeClient_BatchRead(AerospikeClient *self, PyObject *args,
                                    PyObject *kwds)
{
#if ENABLE_LATENCY_DEBUG
    double function_start_time = get_time_microseconds();
    double preprocessing_start_time = function_start_time;
#endif

    PyObject *py_keys = NULL;
    PyObject *py_bins = NULL;
    PyObject *py_policy_batch = NULL;
    static char *kwlist[] = {"keys", "bins", "policy", NULL};

    // bins is now required
    if (__builtin_expect(PyArg_ParseTupleAndKeywords(
                             args, kwds, "OO|O:batch_read", kwlist, &py_keys,
                             &py_bins, &py_policy_batch) == false,
                         0)) {
        return NULL;
    }

    as_error err;
    as_error_init(&err);

    // Initialize numpy functions once
    if (__builtin_expect(init_numpy_functions() < 0, 0)) {
        as_error_update(&err, AEROSPIKE_ERR_CLIENT,
                        "Failed to initialize numpy functions");
        goto CLEANUP1;
    }

    // Fast validation with branch prediction hints
    if (__builtin_expect(!PyList_Check(py_keys), 0)) {
        as_error_update(&err, AEROSPIKE_ERR_PARAM,
                        "keys should be a list of aerospike key tuples");
        goto CLEANUP1;
    }

    if (__builtin_expect(!PyList_Check(py_bins), 0)) {
        as_error_update(&err, AEROSPIKE_ERR_PARAM,
                        "bins should be a list of (name, dtype) tuples");
        goto CLEANUP1;
    }

    Py_ssize_t keys_size = PyList_Size(py_keys);
    Py_ssize_t bins_size = PyList_Size(py_bins);

    if (__builtin_expect(bins_size == 0, 0)) {
        as_error_update(&err, AEROSPIKE_ERR_PARAM, "bins cannot be empty");
        goto CLEANUP1;
    }

    // Optimized memory allocation - use calloc for zero-initialization
    char **bin_names = (char **)calloc(bins_size, sizeof(char *));
    if (__builtin_expect(!bin_names, 0)) {
        as_error_update(&err, AEROSPIKE_ERR_CLIENT,
                        "Failed to allocate bin_names");
        goto CLEANUP1;
    }

    // Create dtype list for structured array
    PyObject *dtype_list = PyList_New(bins_size);
    if (__builtin_expect(!dtype_list, 0)) {
        as_error_update(&err, AEROSPIKE_ERR_CLIENT,
                        "Failed to create dtype list");
        cleanup_bin_names(bin_names, bins_size);
        goto CLEANUP1;
    }

    // Process bins list once with optimized loop
    for (Py_ssize_t i = 0; i < bins_size; i++) {
        PyObject *bin_spec = PyList_GetItem(py_bins, i);
        if (__builtin_expect(
                !PyTuple_Check(bin_spec) || PyTuple_Size(bin_spec) != 2, 0)) {
            as_error_update(&err, AEROSPIKE_ERR_PARAM,
                            "Each bin must be a (name, dtype) tuple");
            Py_DECREF(dtype_list);
            cleanup_bin_names(bin_names, i);
            goto CLEANUP1;
        }

        PyObject *bin_name = PyTuple_GetItem(bin_spec, 0);
        PyObject *bin_dtype = PyTuple_GetItem(bin_spec, 1);

        if (__builtin_expect(
                !PyUnicode_Check(bin_name) || !PyUnicode_Check(bin_dtype), 0)) {
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
    if (__builtin_expect(!numpy_dtype, 0)) {
        as_error_update(&err, AEROSPIKE_ERR_CLIENT,
                        "Failed to create numpy dtype");
        Py_DECREF(dtype_list);
        cleanup_bin_names(bin_names, bins_size);
        goto CLEANUP1;
    }

    // Create zeros array using cached function
    PyObject *numpy_array = PyObject_CallFunction(numpy_zeros_func, "iO",
                                                  (int)keys_size, numpy_dtype);
    if (__builtin_expect(!numpy_array, 0)) {
        as_error_update(&err, AEROSPIKE_ERR_CLIENT,
                        "Failed to create numpy array");
        Py_DECREF(numpy_dtype);
        Py_DECREF(dtype_list);
        cleanup_bin_names(bin_names, bins_size);
        goto CLEANUP1;
    }

    // Pre-cache field arrays for performance - use calloc for safety
    PyObject **field_arrays =
        (PyObject **)calloc(bins_size, sizeof(PyObject *));
    if (__builtin_expect(!field_arrays, 0)) {
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
        if (__builtin_expect(field_name != NULL, 1)) {
            field_arrays[i] = PyObject_GetItem(numpy_array, field_name);
            Py_DECREF(field_name);
        }
        else {
            field_arrays[i] = NULL;
        }
    }

    // OPTIMIZATION: Pre-cache row indices to eliminate PyLong_FromLong() calls
    PyObject **row_indices = (PyObject **)calloc(keys_size, sizeof(PyObject *));
    if (__builtin_expect(!row_indices, 0)) {
        as_error_update(&err, AEROSPIKE_ERR_CLIENT,
                        "Failed to allocate row indices");
        Py_DECREF(numpy_array);
        Py_DECREF(numpy_dtype);
        Py_DECREF(dtype_list);
        for (Py_ssize_t i = 0; i < bins_size; i++) {
            Py_XDECREF(field_arrays[i]);
        }
        free(field_arrays);
        cleanup_bin_names(bin_names, bins_size);
        goto CLEANUP1;
    }

    // Pre-create all row index objects for maximum performance
    for (Py_ssize_t i = 0; i < keys_size; i++) {
        row_indices[i] = PyLong_FromLong(i);
        if (__builtin_expect(!row_indices[i], 0)) {
            as_error_update(&err, AEROSPIKE_ERR_CLIENT,
                            "Failed to create row index");
            Py_DECREF(numpy_array);
            Py_DECREF(numpy_dtype);
            Py_DECREF(dtype_list);
            for (Py_ssize_t j = 0; j < bins_size; j++) {
                Py_XDECREF(field_arrays[j]);
            }
            free(field_arrays);
            cleanup_row_indices(row_indices, i);
            cleanup_bin_names(bin_names, bins_size);
            goto CLEANUP1;
        }
    }

    // Clean up creation objects early
    Py_DECREF(numpy_dtype);
    Py_DECREF(dtype_list);

    // Convert keys to as_batch
    as_batch batch;
    if (__builtin_expect(as_batch_init(&batch, keys_size) == false, 0)) {
        as_error_update(&err, AEROSPIKE_ERR_CLIENT,
                        "Failed to initialize batch");
        Py_DECREF(numpy_array);
        for (Py_ssize_t i = 0; i < bins_size; i++) {
            Py_XDECREF(field_arrays[i]);
        }
        free(field_arrays);
        cleanup_row_indices(row_indices, keys_size);
        cleanup_bin_names(bin_names, bins_size);
        goto CLEANUP1;
    }

    for (Py_ssize_t i = 0; i < keys_size; i++) {
        PyObject *py_key = PyList_GetItem(py_keys, i);
        if (__builtin_expect(
                pyobject_to_key(&err, py_key, &batch.keys.entries[i]) !=
                    AEROSPIKE_OK,
                0)) {
            as_error_update(&err, AEROSPIKE_ERR_PARAM,
                            "Invalid key at index %d", (int)i);
            Py_DECREF(numpy_array);
            for (Py_ssize_t j = 0; j < bins_size; j++) {
                Py_XDECREF(field_arrays[j]);
            }
            free(field_arrays);
            cleanup_row_indices(row_indices, keys_size);
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
        if (__builtin_expect(
                pyobject_to_policy_batch(
                    self, &err, py_policy_batch, &policy_batch, &policy_batch_p,
                    &self->as->config.policies.batch, &batch_exp_list,
                    &batch_exp_list_p) != AEROSPIKE_OK,
                0)) {
            Py_DECREF(numpy_array);
            for (Py_ssize_t i = 0; i < bins_size; i++) {
                Py_XDECREF(field_arrays[i]);
            }
            free(field_arrays);
            cleanup_row_indices(row_indices, keys_size);
            cleanup_bin_names(bin_names, bins_size);
            as_batch_destroy(&batch);
            goto CLEANUP1;
        }
    }

    // Set up highly optimized callback data
    LocalData data;
    data.numpy_array = numpy_array;
    data.field_arrays = field_arrays;
    data.row_indices = row_indices;
    data.client = self;
    data.current_row = 0;
    data.num_bins = (uint32_t)bins_size;
    data.max_rows = (uint32_t)keys_size;
    data.bin_names = bin_names;
    data.has_error = false;

    // Initialize latency debugging fields
    data.total_callback_time_ms = 0.0;
    data.callback_count = 0;

#if ENABLE_LATENCY_DEBUG
    double preprocessing_end_time = get_time_microseconds();
    double preprocessing_duration_ms =
        time_diff_ms(preprocessing_start_time, preprocessing_end_time);
    double network_start_time = preprocessing_end_time;
#endif

    // Call aerospike batch_get_bins
    Py_BEGIN_ALLOW_THREADS

    aerospike_batch_get_bins(self->as, &err, policy_batch_p, &batch,
                             (const char **)bin_names, bins_size, batch_read_cb,
                             &data);

    Py_END_ALLOW_THREADS

#if ENABLE_LATENCY_DEBUG
    double network_end_time = get_time_microseconds();
    double network_duration_ms =
        time_diff_ms(network_start_time, network_end_time);
#endif

    // Final cleanup with optimized order
    for (Py_ssize_t i = 0; i < bins_size; i++) {
        Py_XDECREF(field_arrays[i]);
    }
    free(field_arrays);
    cleanup_row_indices(row_indices, keys_size);
    cleanup_bin_names(bin_names, bins_size);
    as_batch_destroy(&batch);
    if (batch_exp_list_p) {
        as_exp_destroy(batch_exp_list_p);
    }

    if (__builtin_expect(err.code != AEROSPIKE_OK || data.has_error, 0)) {
        Py_DECREF(numpy_array);
        goto CLEANUP1;
    }

#if ENABLE_LATENCY_DEBUG
    double function_end_time = get_time_microseconds();
    double total_function_time_ms =
        time_diff_ms(function_start_time, function_end_time);

    // Print detailed latency breakdown
    printf("\n=== batch_read Latency Debug Report ===\n");
    printf("Keys processed: %zu\n", keys_size);
    printf("Bins per key: %zu\n", bins_size);
    printf("Callback calls: %u\n", data.callback_count);
    printf("\n--- Timing Breakdown ---\n");
    printf("1. Preprocessing latency: %.3f ms\n", preprocessing_duration_ms);
    printf("2. Network latency (aerospike_batch_get_bins): %.3f ms\n",
           network_duration_ms);
    printf("3. NumPy array composition latency: %.3f ms (avg: %.3f ms per "
           "callback)\n",
           data.total_callback_time_ms,
           data.callback_count > 0
               ? data.total_callback_time_ms / data.callback_count
               : 0.0);
    printf("4. Total function latency: %.3f ms\n", total_function_time_ms);
    printf("\n--- Performance Metrics ---\n");
    printf("Preprocessing overhead: %.1f%%\n",
           (preprocessing_duration_ms / total_function_time_ms) * 100.0);
    printf("Network overhead: %.1f%%\n",
           (network_duration_ms / total_function_time_ms) * 100.0);
    printf("NumPy composition overhead: %.1f%%\n",
           (data.total_callback_time_ms / total_function_time_ms) * 100.0);
    printf("Records per second: %.0f\n",
           (keys_size * 1000.0) / total_function_time_ms);
    printf("========================================\n\n");
#endif

    return numpy_array;

CLEANUP1:
    if (err.code != AEROSPIKE_OK) {
        raise_exception(&err);
    }
    return NULL;
}
