#include <Python.h>

#include "types.h"

static void AerospikeTransaction_dealloc(AerospikeTransaction *self)
{
    // Transaction object can be created but not initialized, so need to check
    if (self->txn != NULL) {
        as_txn_destroy(self->txn);
    }
    Py_TYPE(self)->tp_free((PyObject *)self);
}

static PyObject *AerospikeTransaction_new(PyTypeObject *type, PyObject *args,
                                          PyObject *kwds)
{
    AerospikeTransaction *self =
        (AerospikeTransaction *)type->tp_alloc(type, 0);
    if (self == NULL) {
        return NULL;
    }
    return (PyObject *)self;
}

// Error indicator must always be checked after this call
// Constructor parameter name needed for constructing error message
static uint32_t convert_pyobject_to_uint32_t(PyObject *pyobject,
                                             const char *param_name_of_pyobj)
{
    if (!PyLong_Check(pyobject)) {
        PyErr_Format(PyExc_TypeError, "%s must be an integer",
                     param_name_of_pyobj);
        goto error;
    }
    unsigned long long_value = PyLong_AsUnsignedLong(pyobject);
    if (PyErr_Occurred()) {
        goto error;
    }

    if (long_value > UINT32_MAX) {
        PyErr_Format(PyExc_ValueError,
                     "%s is too large for an unsigned 32-bit integer",
                     param_name_of_pyobj);
        goto error;
    }

    uint32_t value = (uint32_t)long_value;
    return value;

error:
    return 0;
}

// We don't initialize in __new__ because it's not documented how to raise
// exceptions in __new__
// We can raise an exception and fail out in __init__ though
static int AerospikeTransaction_init(AerospikeTransaction *self, PyObject *args,
                                     PyObject *kwds)
{
    static char *kwlist[] = {"reads_capacity", "writes_capacity", NULL};
    // We could use unsigned longs directly in the format string
    // But then we can't tell if they were set or not by the user
    // So we just use PyObjects for the optional args instead
    PyObject *py_reads_capacity = NULL;
    PyObject *py_writes_capacity = NULL;

    if (PyArg_ParseTupleAndKeywords(args, kwds, "|OO", kwlist,
                                    &py_reads_capacity,
                                    &py_writes_capacity) == false) {
        goto error;
    }

    as_txn *txn;
    uint32_t reads_capacity, writes_capacity;
    if (py_reads_capacity) {
        reads_capacity =
            convert_pyobject_to_uint32_t(py_reads_capacity, kwlist[0]);
        if (PyErr_Occurred()) {
            goto error;
        }
    }
    else {
        reads_capacity = AS_TXN_READ_CAPACITY_DEFAULT;
    }

    if (py_writes_capacity) {
        writes_capacity =
            convert_pyobject_to_uint32_t(py_writes_capacity, kwlist[1]);
        if (PyErr_Occurred()) {
            goto error;
        }
    }
    else {
        writes_capacity = AS_TXN_WRITE_CAPACITY_DEFAULT;
    }

    txn = as_txn_create_capacity(reads_capacity, writes_capacity);

    // If this transaction object was already initialized before, reinitialize it
    if (self->txn) {
        as_txn_destroy(self->txn);
    }
    self->txn = txn;

    return 0;
error:
    return -1;
}

static PyObject *AerospikeTransaction_get_in_doubt(AerospikeTransaction *self,
                                                   void *closure)
{
    PyObject *py_in_doubt = PyBool_FromLong(self->txn->in_doubt);
    if (py_in_doubt == NULL) {
        return NULL;
    }
    return py_in_doubt;
}

static PyObject *AerospikeTransaction_get_state(AerospikeTransaction *self,
                                                void *closure)
{
    PyObject *py_state = PyLong_FromLong((long)self->txn->state);
    if (py_state == NULL) {
        return NULL;
    }
    return py_state;
}

static PyObject *AerospikeTransaction_get_timeout(AerospikeTransaction *self,
                                                  void *closure)
{
    PyObject *py_timeout =
        PyLong_FromUnsignedLong((unsigned long)self->txn->timeout);
    if (py_timeout == NULL) {
        return NULL;
    }
    return py_timeout;
}

static int AerospikeTransaction_set_timeout(AerospikeTransaction *self,
                                            PyObject *py_value, void *closure)
{
    uint32_t timeout = convert_pyobject_to_uint32_t(py_value, "timeout");
    if (PyErr_Occurred()) {
        return -1;
    }

    self->txn->timeout = timeout;
    return 0;
}

static PyObject *AerospikeTransaction_get_id(AerospikeTransaction *self,
                                             void *closure)
{
    PyObject *py_id =
        PyLong_FromUnsignedLongLong((unsigned long long)self->txn->id);
    if (py_id == NULL) {
        return NULL;
    }
    return py_id;
}

static PyGetSetDef AerospikeTransaction_getsetters[] = {
    {.name = "timeout",
     .get = (getter)AerospikeTransaction_get_timeout,
     .set = (setter)AerospikeTransaction_set_timeout},
    {.name = "in_doubt", .get = (getter)AerospikeTransaction_get_in_doubt},
    {.name = "state", .get = (getter)AerospikeTransaction_get_state},
    {.name = "id", .get = (getter)AerospikeTransaction_get_id},
    {NULL} /* Sentinel */
};

PyTypeObject AerospikeTransaction_Type = {
    .ob_base = PyVarObject_HEAD_INIT(NULL, 0).tp_name =
        FULLY_QUALIFIED_TYPE_NAME("Transaction"),
    .tp_basicsize = sizeof(AerospikeTransaction),
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_new = AerospikeTransaction_new,
    .tp_init = (initproc)AerospikeTransaction_init,
    .tp_dealloc = (destructor)AerospikeTransaction_dealloc,
    .tp_getset = AerospikeTransaction_getsetters};

PyTypeObject *AerospikeTransaction_Ready()
{
    return PyType_Ready(&AerospikeTransaction_Type) == 0
               ? &AerospikeTransaction_Type
               : NULL;
}
