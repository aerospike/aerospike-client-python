#include <Python.h>

#include <aerospike/aerospike_txn.h>
#include "exceptions.h"
#include "types.h"
#include "client.h"

PyObject *AerospikeClient_Commit(AerospikeClient *self, PyObject *args,
                                 PyObject *kwds)
{
    AerospikeTransaction *py_transaction = NULL;

    static char *kwlist[] = {"transaction", NULL};

    if (PyArg_ParseTupleAndKeywords(args, kwds, "O!:commit", kwlist,
                                    &AerospikeTransaction_Type,
                                    (PyObject **)(&py_transaction)) == false) {
        return NULL;
    }

    as_error err;
    as_error_init(&err);

    // Set a default value in case the C client does not initialize the status.
    as_commit_status status = AS_COMMIT_OK;

    Py_BEGIN_ALLOW_THREADS
    aerospike_commit(self->as, &err, py_transaction->txn, &status);
    Py_END_ALLOW_THREADS

    PyObject *py_status = PyLong_FromUnsignedLong((unsigned long)status);
    if (py_status == NULL) {
        return NULL;
    }

    if (err.code != AEROSPIKE_OK) {
        raise_exception_with_mrt_status(&err, py_status, NULL);
        return NULL;
    }

    return py_status;
}

PyObject *AerospikeClient_Abort(AerospikeClient *self, PyObject *args,
                                PyObject *kwds)
{
    AerospikeTransaction *py_transaction = NULL;

    static char *kwlist[] = {"transaction", NULL};

    if (PyArg_ParseTupleAndKeywords(args, kwds, "O!|p:abort", kwlist,
                                    &AerospikeTransaction_Type,
                                    (PyObject **)(&py_transaction)) == false) {
        return NULL;
    }

    as_error err;
    as_error_init(&err);

    // Set a default value in case the C client does not initialize the status.
    as_abort_status status = AS_ABORT_OK;

    Py_BEGIN_ALLOW_THREADS
    aerospike_abort(self->as, &err, py_transaction->txn, &status);
    Py_END_ALLOW_THREADS

    PyObject *py_status = PyLong_FromUnsignedLong((unsigned long)status);
    if (py_status == NULL) {
        return NULL;
    }

    if (err.code != AEROSPIKE_OK) {
        raise_exception_with_mrt_status(&err, NULL, py_status);
        return NULL;
    }

    return py_status;
}
