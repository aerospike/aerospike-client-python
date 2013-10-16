#include <Python.h>
#include <structmember.h>
#include <stdbool.h>

#include <aerospike/aerospike.h>
#include <aerospike/as_config.h>
#include <aerospike/as_error.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_scan.h>

#include "client.h"
#include "scan.h"

/*******************************************************************************
 * PYTHON TYPE METHODS
 ******************************************************************************/

static PyMethodDef AerospikeScanType_Methods[] = {
    {"foreach",	(PyCFunction) AerospikeScan_Foreach,	METH_VARARGS | METH_KEYWORDS, "Iterate over each result and call the callback function."},
    {"results",	(PyCFunction) AerospikeScan_Results,	METH_VARARGS | METH_KEYWORDS, "Get a record."},
	{NULL}
};

/*******************************************************************************
 * PYTHON TYPE HOOKS
 ******************************************************************************/

static PyObject * AerospikeScanType_New(PyTypeObject * type, PyObject * args, PyObject * kwds)
{
	AerospikeScan * self = NULL;

    self = (AerospikeScan *) type->tp_alloc(type, 0);

    if ( self == NULL ) {
    	return NULL;
    }

	return (PyObject *) self;
}

static int AerospikeScanType_Init(AerospikeScan * self, PyObject * args, PyObject * kwds)
{
	PyObject * py_client = PyTuple_GetItem(args, 0);
	PyObject * py_args = PyTuple_GetItem(args, 1);
	PyObject * py_namespace = NULL;
	PyObject * py_set = NULL;
	
	static char * kwlist[] = {"namespace", "set", NULL};

	if ( PyArg_ParseTupleAndKeywords(py_args, kwds, "O|O:key", kwlist, 
		&py_namespace, &py_set) == false ) {
		return 0;
	}
		
	char * namespace = NULL;
	char * set = NULL;

	if ( PyString_Check(py_namespace) ) {
		namespace = PyString_AsString(py_namespace);
	}

	if ( PyString_Check(py_set) ) {
		set = PyString_AsString(py_set);
	}

	Py_INCREF(py_client);

	self->client = (AerospikeClient *) py_client;
	as_scan_init(&self->scan, namespace, set);

    return 0;
}

static void AerospikeScanType_Dealloc(PyObject * self)
{
    self->ob_type->tp_free((PyObject *) self);
}

/*******************************************************************************
 * PYTHON TYPE DESCRIPTOR
 ******************************************************************************/

static PyTypeObject AerospikeScanType = {
	PyObject_HEAD_INIT(NULL)

    .ob_size			= 0,
    .tp_name			= "aerospike.scan",
    .tp_basicsize		= sizeof(AerospikeScan),
    .tp_itemsize		= 0,
    .tp_dealloc			= (destructor) AerospikeScanType_Dealloc,
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
    .tp_doc				= "aerospike.scan doc",
    .tp_traverse		= 0,
    .tp_clear			= 0,
    .tp_richcompare		= 0,
    .tp_weaklistoffset	= 0,
    .tp_iter			= 0,
    .tp_iternext		= 0,
    .tp_methods			= AerospikeScanType_Methods,
    .tp_members			= 0,
    .tp_getset			= 0,
    .tp_base			= 0,
    .tp_dict			= 0,
    .tp_descr_get		= 0,
    .tp_descr_set		= 0,
    .tp_dictoffset		= 0,
    .tp_init			= (initproc) AerospikeScanType_Init,
    .tp_alloc			= 0,
    .tp_new				= AerospikeScanType_New
};

/*******************************************************************************
 * PUBLIC FUNCTIONS
 ******************************************************************************/

bool AerospikeScan_Ready()
{
	return PyType_Ready(&AerospikeScanType) < 0;
}

PyObject * AerospikeScan_Create(PyObject * self, PyObject * args, PyObject * kwds)
{
    PyObject * scan = AerospikeScanType.tp_new(&AerospikeScanType, args, kwds);
    AerospikeScanType.tp_init(scan, args, kwds);
	return scan;
}