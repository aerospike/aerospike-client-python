#include <Python.h>

#include <aerospike/aerospike_txn.h>
#include "exceptions.h"
#include "types.h"

PyObject *AerospikeClient_Commit(AerospikeClient *self, PyObject *args,
                                 PyObject *kwds)
{
    AerospikeTransaction *py_transaction = NULL;

    static char *kwlist[] = {"transaction", NULL};

    if (PyArg_ParseTupleAndKeywords(args, kwds, "O!:commit", kwlist,
                                    AerospikeTransaction_Type,
                                    (PyObject **)(&py_transaction)) == false) {
        return NULL;
    }

    as_error err;
    as_error_init(&err);

    Py_BEGIN_ALLOW_THREADS
    aerospike_commit(self->as, &err, py_transaction->txn);
    Py_END_ALLOW_THREADS

    if (err.code != AEROSPIKE_OK) {
        raise_exception(&err);
        return NULL;
    }

    Py_RETURN_NONE;
}

PyObject *AerospikeClient_Abort(AerospikeClient *self, PyObject *args,
                                PyObject *kwds)
{
    AerospikeTransaction *py_transaction = NULL;

    static char *kwlist[] = {"transaction", NULL};

    if (PyArg_ParseTupleAndKeywords(args, kwds, "O!:abort", kwlist,
                                    AerospikeTransaction_Type,
                                    (PyObject **)(&py_transaction)) == false) {
        return NULL;
    }

    as_error err;
    as_error_init(&err);

    Py_BEGIN_ALLOW_THREADS
    aerospike_abort(self->as, &err, py_transaction->txn);
    Py_END_ALLOW_THREADS

    if (err.code != AEROSPIKE_OK) {
        raise_exception(&err);
        return NULL;
    }

    Py_RETURN_NONE;
}
