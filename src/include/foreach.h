#include <Python.h>
#include "client.h"

// Struct for Python User-Data for the Callback
typedef struct {
    PyObject *py_obj;
    AerospikeClient *client;
    as_vector thread_errors;
    pthread_mutex_t thread_errors_mutex;
    bool partition_query;
} LocalData;
