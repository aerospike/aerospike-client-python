#include <Python.h>
#include <structmember.h>
#include <stdbool.h>

#include <aerospike/aerospike.h>
#include <aerospike/as_config.h>
#include <aerospike/as_error.h>
#include <aerospike/as_key.h>
#include <aerospike/as_policy.h>

#include "client.h"
#include "key.h"

/*******************************************************************************
 * PYTHON TYPE METHODS
 ******************************************************************************/

static PyMethodDef AerospikeKeyType_Methods[] = {
    {"apply",	(PyCFunction) AerospikeKey_Apply,	METH_VARARGS | METH_KEYWORDS, "Apply a UDF on a record."},
    {"exists",	(PyCFunction) AerospikeKey_Exists,	METH_VARARGS | METH_KEYWORDS, "Check existence of the record."},
    {"get",		(PyCFunction) AerospikeKey_Get,		METH_VARARGS | METH_KEYWORDS, "Get all bins of the record."},
    {"put",		(PyCFunction) AerospikeKey_Put,		METH_VARARGS | METH_KEYWORDS, "Update a record."},
    {"remove",	(PyCFunction) AerospikeKey_Remove,	METH_VARARGS | METH_KEYWORDS, "Remove a record."},
    // {"select",	(PyCFunction) AerospikeKey_Select,	METH_VARARGS | METH_KEYWORDS, "Select specific bins of the record."},
	{NULL}
};

/*******************************************************************************
 * PYTHON TYPE HOOKS
 ******************************************************************************/

static PyObject * AerospikeKeyType_New(PyTypeObject * type, PyObject * args, PyObject * kwds)
{
	AerospikeKey * self = NULL;

    self = (AerospikeKey *) type->tp_alloc(type, 0);

    if ( self == NULL ) {
		return NULL;
    }
	
	return (PyObject *) self;
}

static int AerospikeKeyType_Init(AerospikeKey * self, PyObject * args, PyObject * kwds)
{
	PyObject * py_client = PyTuple_GetItem(args, 0);
	PyObject * py_args = PyTuple_GetItem(args, 1);
	// PyObject * py_namespace = NULL;
	// PyObject * py_set = NULL;
	// PyObject * py_key = NULL;
	// PyObject * py_digest = NULL;
	
	// static char * kwlist[] = {"namespace", "set", "key", "digest", NULL};
	
	// if ( PyArg_ParseTupleAndKeywords(py_args, kwds, "O|OOO:key", kwlist, 
	// 	&py_namespace, &py_set, &py_key, &py_digest) == false ) {
	// 	return 0;
	// }

	// char * n = NULL;
	// char * s = NULL;

	// if ( PyString_Check(py_namespace) ) {
	// 	n = PyString_AsString(py_namespace);
	// }

	// if ( PyString_Check(py_set) ) {
	// 	s = PyString_AsString(py_set);
	// }

	// if ( PyInt_Check(py_key) ) {
	// 	int64_t k = (int64_t) PyInt_AsLong(py_key);
	// 	as_key_init_int64(&self->key, n, s, k);
	// }
	// else if ( PyLong_Check(py_key) ) {
	// 	int64_t k = (int64_t) PyLong_AsLongLong(py_key);
	// 	as_key_init_int64(&self->key, n, s, k);
	// }
	// else if ( PyString_Check(py_key) ) {
	// 	char * k = PyString_AsString(py_key);
	// 	as_key_init_strp(&self->key, n, s, k, false);
	// }

	// PyObject * py_keytuple = PyTuple_New(4);
	// PyTuple_SetItem(py_keytuple, 0, py_namespace);
	// PyTuple_SetItem(py_keytuple, 0, py_set);
	// PyTuple_SetItem(py_keytuple, 0, py_key);
	// PyTuple_SetItem(py_keytuple, 0, py_digest);
	
	Py_INCREF(py_client);
	Py_INCREF(py_args);

	self->client = (AerospikeClient *) py_client;
	self->key = py_args;

    return 0;
}

static void AerospikeKeyType_Dealloc(AerospikeKey * self)
{
	// as_key_destroy(&self->key);
    self->ob_type->tp_free((PyObject *) self);
}

/*******************************************************************************
 * PYTHON TYPE DESCRIPTOR
 ******************************************************************************/

static PyTypeObject AerospikeKeyType = {
	PyObject_HEAD_INIT(NULL)

    .ob_size			= 0,
    .tp_name			= "aerospike.key",
    .tp_basicsize		= sizeof(AerospikeKey),
    .tp_itemsize		= 0,
    .tp_dealloc			= (destructor) AerospikeKeyType_Dealloc,
    .tp_print			= 0,
    .tp_getattr			= 0,
    .tp_setattr			= 0,
    .tp_compare			= 0,
    .tp_repr			= 0,
    .tp_as_number		= 0,
    .tp_as_sequence		= 0,
    .tp_as_mapping		= 0,
    .tp_hash			= 0,
    .tp_call			= 0,
    .tp_str				= 0,
    .tp_getattro		= 0,
    .tp_setattro		= 0,
    .tp_as_buffer		= 0,
    .tp_flags			= Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
    .tp_doc				= "aerospike.key doc",
    .tp_traverse		= 0,
    .tp_clear			= 0,
    .tp_richcompare		= 0,
    .tp_weaklistoffset	= 0,
    .tp_iter			= 0,
    .tp_iternext		= 0,
    .tp_methods			= AerospikeKeyType_Methods,
    .tp_members			= 0,
    .tp_getset			= 0,
    .tp_base			= 0,
    .tp_dict			= 0,
    .tp_descr_get		= 0,
    .tp_descr_set		= 0,
    .tp_dictoffset		= 0,
    .tp_init			= (initproc) AerospikeKeyType_Init,
    .tp_alloc			= 0,
    .tp_new				= AerospikeKeyType_New
};

/*******************************************************************************
 * PUBLIC FUNCTIONS
 ******************************************************************************/

bool AerospikeKey_Ready()
{
	return PyType_Ready(&AerospikeKeyType) < 0;
}

PyObject * AerospikeKey_Create(PyObject * self, PyObject * args, PyObject * kwds)
{
    PyObject * key = AerospikeKeyType.tp_new(&AerospikeKeyType, args, kwds);
    AerospikeKeyType.tp_init(key, args, kwds);
	return key;
}