#include "types.h"
#include "config_provider.h"
#include "conversions.h"

static PyObject *AerospikeCompiledExpression_new(PyTypeObject *type,
                                                 PyObject *args, PyObject *kwds)
{
    AerospikeCompiledExpression *self =
        (AerospikeCompiledExpression *)type->tp_alloc(type, 0);
    if (self == NULL) {
        // TODO: db check
        return NULL;
    }

    self->exp

        error : Py_TYPE(self)
                    ->tp_free((PyObject *)self);
    return NULL;
}

static void
AerospikeCompiledExpression_dealloc(AerospikeCompiledExpression *self)
{
    if (self->exp) {
        as_exp_destroy(self->exp);
    }
    Py_TYPE(self)->tp_free((PyObject *)self);
}

PyTypeObject AerospikeCompiledExpression_Type = {
    .ob_base = PyVarObject_HEAD_INIT(NULL, 0).tp_name =
        FULLY_QUALIFIED_TYPE_NAME("CompiledExpression"),
    .tp_basicsize = sizeof(AerospikeCompiledExpression),
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_new = NULL,
    .tp_dealloc = (destructor)AerospikeCompiledExpression_dealloc,
};

// PyTypeObject *AerospikeConfigProvider_Ready()
// {
//     return PyType_Ready(&AerospikeConfigProvider_Type) == 0
//                ? &AerospikeConfigProvider_Type
//                : NULL;
// }
