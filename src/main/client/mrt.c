#include <Python.h>

#include <aerospike/aerospike_txn.h>
#include "exceptions.h"
#include "types.h"
#include "client.h"

PyObject *AerospikeClient_Commit(AerospikeClient *self, PyObject *args,
                                 PyObject *kwds)
{
    AerospikeTransaction *py_transaction = NULL;
    int get_status = 0;

    static char *kwlist[] = {"transaction", "get_commit_status", NULL};

    if (PyArg_ParseTupleAndKeywords(
            args, kwds, "O!|p:commit", kwlist, &AerospikeTransaction_Type,
            (PyObject **)(&py_transaction), &get_status) == false) {
        return NULL;
    }

    as_error err;
    as_error_init(&err);

    as_commit_status status;
    as_commit_status *status_ref;
    if (get_status) {
        status_ref = &status;
    }
    else {
        status_ref = NULL;
    }

    Py_BEGIN_ALLOW_THREADS
    aerospike_commit(self->as, &err, py_transaction->txn, status_ref);
    Py_END_ALLOW_THREADS

    if (err.code != AEROSPIKE_OK) {
        raise_exception(&err);
        return NULL;
    }

    if (get_status) {
        PyObject *py_status = PyLong_FromUnsignedLong((unsigned long)status);
        if (py_status == NULL) {
            return NULL;
        }
        return py_status;
    }
    else {
        Py_RETURN_NONE;
    }
}

PyObject *AerospikeClient_Abort(AerospikeClient *self, PyObject *args,
                                PyObject *kwds)
{
    AerospikeTransaction *py_transaction = NULL;

    static char *kwlist[] = {"transaction", "get_abort_status", NULL};
    int get_status = 0;

    if (PyArg_ParseTupleAndKeywords(
            args, kwds, "O!p:abort", kwlist, &AerospikeTransaction_Type,
            (PyObject **)(&py_transaction), &get_status) == false) {
        return NULL;
    }

    as_error err;
    as_error_init(&err);

    as_abort_status status;
    as_abort_status *status_ref;
    if (get_status) {
        status_ref = &status;
    }
    else {
        status_ref = NULL;
    }

    Py_BEGIN_ALLOW_THREADS
    aerospike_abort(self->as, &err, py_transaction->txn, status_ref);
    Py_END_ALLOW_THREADS

    if (err.code != AEROSPIKE_OK) {
        raise_exception(&err);
        return NULL;
    }

    if (get_status) {
        PyObject *py_status = PyLong_FromUnsignedLong((unsigned long)status);
        if (py_status == NULL) {
            return NULL;
        }
        return py_status;
    }
    else {
        Py_RETURN_NONE;
    }
}
