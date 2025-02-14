#include <Python.h>

#include "types.h"
#include "fixed_key_dict.h"

// TODO: eventually we need to move this to a per-module state
PyObject *py_set_of_valid_keys = NULL;

static PyObject *FixedKeyDict___setitem__(PyObject *self, PyObject *py_key,
                                          PyObject *py_value)
{
    if (py_key == NULL) {
        // Cannot validate a NULL key
        return NULL;
    }

    int retval = PySet_Contains(py_set_of_valid_keys, py_key);
    if (retval == 0) {
        PyErr_Format(PyExc_KeyError,
                     "%R is an invalid key for client config dictionary",
                     py_key);
        return NULL;
    }
    else if (retval == -1) {
        return NULL;
    }

    // super()
    PyObject *py_base_type = self->ob_type->tp_base;
    PyObject *py_retval = PyObject_CallMethod(
        self->ob_type->tp_base, "__setitem__", "OO", py_key, py_value);
    if (py_retval == NULL) {
        return NULL;
    }

    return py_retval;
}

static PyMappingMethods FixedKeyDict_Type_AsMapping = {
    .mp_subscript = FixedKeyDict___setitem__};

PyTypeObject FixedKeyDict_Type = {
    .ob_base = PyVarObject_HEAD_INIT(NULL, 0).tp_name =
        FULLY_QUALIFIED_TYPE_NAME("FixedKeyDict"),
    .tp_basicsize = sizeof(FixedKeyDict),
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_as_mapping = &FixedKeyDict_Type_AsMapping};

PyTypeObject *AerospikeFixedKeyDict_Ready()
{
    // TODO: use frozen set with fixed size
    py_set_of_valid_keys = PySet_New(NULL);
    if (py_set_of_valid_keys == NULL) {
        return NULL;
    }

    // TODO: centralize this
    const char *valid_keys[] = {""};

    for (unsigned long i = 0; i < sizeof(valid_keys) / sizeof(valid_keys[0]);
         i++) {
        PyObject *py_key = PyUnicode_FromString(valid_keys[i]);
        if (py_key == NULL) {
            Py_DECREF(py_set_of_valid_keys);
            return NULL;
        }

        int result = PySet_Add(py_set_of_valid_keys, py_key);
        Py_DECREF(py_key);
        if (result == -1) {
            Py_DECREF(py_set_of_valid_keys);
            return NULL;
        }
    }

    return PyType_Ready(&FixedKeyDict_Type) == 0 ? &FixedKeyDict_Type : NULL;
}
