#include <Python.h>

#include <aerospike/aerospike_txn.h>
#include "exceptions.h"
#include "types.h"
#include "client.h"

static PyObject *call_c_client_mrt_method(
    AerospikeClient *self, PyObject *args, PyObject *kwds,
    as_status (*c_client_api_call)(aerospike *as, as_error *err, as_txn *txn))
{
    AerospikeTransaction *py_transaction = NULL;

    static char *kwlist[] = {"transaction", NULL};

    const char *format;
    if (c_client_api_call == aerospike_commit) {
        format = "O!:commit";
    }
    else {
        format = "O!:abort";
    }
    if (PyArg_ParseTupleAndKeywords(args, kwds, format, kwlist,
                                    &AerospikeTransaction_Type,
                                    (PyObject **)(&py_transaction)) == false) {
        return NULL;
    }

    as_error err;
    as_error_init(&err);

    Py_BEGIN_ALLOW_THREADS
    c_client_api_call(self->as, &err, py_transaction->txn);
    Py_END_ALLOW_THREADS

    if (err.code != AEROSPIKE_OK) {
        raise_exception(&err);
        return NULL;
    }

    Py_RETURN_NONE;
}

PyObject *AerospikeClient_Commit(AerospikeClient *self, PyObject *args,
                                 PyObject *kwds)
{
    return call_c_client_mrt_method(self, args, kwds, aerospike_commit);
}

PyObject *AerospikeClient_Abort(AerospikeClient *self, PyObject *args,
                                PyObject *kwds)
{
    return call_c_client_mrt_method(self, args, kwds, aerospike_abort);
}
