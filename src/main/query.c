#include <Python.h>
#include <structmember.h>
#include <stdbool.h>

#include <aerospike/aerospike.h>
#include <aerospike/as_config.h>
#include <aerospike/as_error.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_query.h>

#include "client.h"
#include "query.h"

/*******************************************************************************
 * PYTHON TYPE METHODS
 ******************************************************************************/

static PyMethodDef AerospikeQueryType_Methods[] = {
    {"apply",	(PyCFunction) AerospikeQuery_Apply,		METH_VARARGS | METH_KEYWORDS, "Apply a UDF on the results of the query."},
    {"foreach",	(PyCFunction) AerospikeQuery_Foreach,	METH_VARARGS | METH_KEYWORDS, "Iterate over each result and call the callback function."},
    {"results",	(PyCFunction) AerospikeQuery_Results,	METH_VARARGS | METH_KEYWORDS, "Get a record."},
    {"select",	(PyCFunction) AerospikeQuery_Select,	METH_VARARGS | METH_KEYWORDS, "Add bins to select in the query."},
    {"where",	(PyCFunction) AerospikeQuery_Where,		METH_VARARGS | METH_KEYWORDS, "Add a where predicate to the query."},
	{NULL}
};

/*******************************************************************************
 * PYTHON TYPE HOOKS
 ******************************************************************************/

static PyObject * AerospikeQueryType_New(PyTypeObject * type, PyObject * args, PyObject * kwds)
{
	AerospikeQuery * self = NULL;

    self = (AerospikeQuery *) type->tp_alloc(type, 0);

    if ( self == NULL ) {
    	return NULL;
    }

	return (PyObject *) self;
}

static int AerospikeQueryType_Init(AerospikeQuery * self, PyObject * args, PyObject * kwds)
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
	as_query_init(&self->query, namespace, set);

    return 0;
}

static void AerospikeQueryType_Dealloc(PyObject * self)
{
    self->ob_type->tp_free((PyObject *) self);
}

/*******************************************************************************
 * PYTHON TYPE DESCRIPTOR
 ******************************************************************************/

static PyTypeObject AerospikeQueryType = {
	PyObject_HEAD_INIT(NULL)

    .ob_size			= 0,
    .tp_name			= "aerospike.query",
    .tp_basicsize		= sizeof(AerospikeQuery),
    .tp_itemsize		= 0,
    .tp_dealloc			= (destructor) AerospikeQueryType_Dealloc,
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
    .tp_doc				= "aerospike.query doc",
    .tp_traverse		= 0,
    .tp_clear			= 0,
    .tp_richcompare		= 0,
    .tp_weaklistoffset	= 0,
    .tp_iter			= 0,
    .tp_iternext		= 0,
    .tp_methods			= AerospikeQueryType_Methods,
    .tp_members			= 0,
    .tp_getset			= 0,
    .tp_base			= 0,
    .tp_dict			= 0,
    .tp_descr_get		= 0,
    .tp_descr_set		= 0,
    .tp_dictoffset		= 0,
    .tp_init			= (initproc) AerospikeQueryType_Init,
    .tp_alloc			= 0,
    .tp_new				= AerospikeQueryType_New
};

/*******************************************************************************
 * PUBLIC FUNCTIONS
 ******************************************************************************/

bool AerospikeQuery_Ready()
{
	return PyType_Ready(&AerospikeQueryType) < 0;
}

PyObject * AerospikeQuery_Create(PyObject * self, PyObject * args, PyObject * kwds)
{
    PyObject * query = AerospikeQueryType.tp_new(&AerospikeQueryType, args, kwds);
    AerospikeQueryType.tp_init(query, args, kwds);
	return query;
}