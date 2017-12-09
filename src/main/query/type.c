/*******************************************************************************
 * Copyright 2013-2017 Aerospike, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

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
#include "conversions.h"
#include "exceptions.h"

/*******************************************************************************
 * PYTHON DOC METHODS
 ******************************************************************************/

PyDoc_STRVAR(apply_doc,
"apply(module, function[, arguments])\n\
\n\
Aggregate the results() using a stream UDF. \
If no predicate is attached to the Query the stream UDF will aggregate over all the records in the specified set.");

PyDoc_STRVAR(foreach_doc,
"foreach(callback[, policy])\n\
\n\
Invoke the callback function for each of the records streaming back from the query.");

PyDoc_STRVAR(results_doc,
"results([policy]) -> list of (key, meta, bins)\n\
\n\
Buffer the records resulting from the query, and return them as a list of records.");

PyDoc_STRVAR(select_doc,
"select(bin1[, bin2[, bin3..]])\n\
\n\
Set a filter on the record bins resulting from results() or foreach(). \
If a selected bin does not exist in a record it will not appear in the bins portion of that record tuple.");

PyDoc_STRVAR(where_doc,
"where(predicate)\n\
\n\
Set a where predicate for the query, without which the query will behave similar to aerospike.Scan. \
The predicate is produced by one of the aerospike.predicates methods equals() and between().");

/*******************************************************************************
 * PYTHON TYPE METHODS
 ******************************************************************************/

static PyMethodDef AerospikeQuery_Type_Methods[] = {

	{"apply",	(PyCFunction) AerospikeQuery_Apply,		METH_VARARGS | METH_KEYWORDS,
				apply_doc},

	{"foreach",	(PyCFunction) AerospikeQuery_Foreach,	METH_VARARGS | METH_KEYWORDS,
				foreach_doc},

	{"results",	(PyCFunction) AerospikeQuery_Results,	METH_VARARGS | METH_KEYWORDS,
				results_doc},

	{"select",	(PyCFunction) AerospikeQuery_Select,	METH_VARARGS | METH_KEYWORDS,
				select_doc},

	{"where",	(PyCFunction) AerospikeQuery_Where,		METH_VARARGS,
				where_doc},

	{NULL}
};

/*******************************************************************************
 * PYTHON TYPE HOOKS
 ******************************************************************************/

static PyObject * AerospikeQuery_Type_New(PyTypeObject * type, PyObject * args, PyObject * kwds)
{
	AerospikeQuery * self = NULL;

	self = (AerospikeQuery *) type->tp_alloc(type, 0);

	return (PyObject *) self;
}

static int AerospikeQuery_Type_Init(AerospikeQuery * self, PyObject * args, PyObject * kwds)
{
	PyObject * py_namespace = NULL;
	PyObject * py_set = NULL;
	PyObject * py_ustr_set = NULL;

	as_error err;
	as_error_init(&err);

	static char * kwlist[] = {"namespace", "set", NULL};

	if (PyArg_ParseTupleAndKeywords(args, kwds, "O|O:key", kwlist,
		&py_namespace, &py_set) == false) {
		as_query_destroy(&self->query);
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "query() expects atleast 1 parameter");
		goto CLEANUP;
	}

	char * namespace = NULL;
	char * set = NULL;

	if (PyString_Check(py_namespace)) {
		namespace = PyString_AsString(py_namespace);
	} else {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Namespace should be a string");
		goto CLEANUP;
	}

	if (py_set) {
		if (PyString_Check(py_set)) {
			set = PyString_AsString(py_set);
		} else if (PyUnicode_Check(py_set)) {
			py_ustr_set = PyUnicode_AsUTF8String(py_set);
			set = PyBytes_AsString(py_ustr_set);
		} else if ( py_set != Py_None ) {
			as_error_update(&err, AEROSPIKE_ERR_PARAM, "Set should be string, unicode or None");
			goto CLEANUP;
		}
	}

	as_query_init(&self->query, namespace, set);

CLEANUP:
	if (py_ustr_set) {
		Py_DECREF(py_ustr_set);
	}

	if (err.code != AEROSPIKE_OK) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyObject *exception_type = raise_exception(&err);
		PyErr_SetObject(exception_type, py_err);
		Py_XDECREF(py_err);
		return -1;
	}

	return 0;
}

static void AerospikeQuery_Type_Dealloc(AerospikeQuery * self)
{
	int i;
	for (i=0; i < self->u_objs.size; i++) {
		Py_XDECREF(self->u_objs.ob[i]);
	}

	for (i = 0; i < self->query.where.size; i++) {
		as_predicate * p = &self->query.where.entries[i];
		if (p) {
			if (p->dtype == AS_INDEX_STRING) {
				free(p->value.string);
			}
		}
	}

	as_query_destroy(&self->query);
	Py_TYPE(self)->tp_free((PyObject *) self);
}

/*******************************************************************************
 * PYTHON TYPE DESCRIPTOR
 ******************************************************************************/
static PyTypeObject AerospikeQuery_Type = {
	PyVarObject_HEAD_INIT(NULL, 0)
	"aerospike.Query",                  // tp_name
	sizeof(AerospikeQuery),             // tp_basicsize
	0,                                  // tp_itemsize
	(destructor) AerospikeQuery_Type_Dealloc,
	                                    // tp_dealloc
	0,                                  // tp_print
	0,                                  // tp_getattr
	0,                                  // tp_setattr
	0,                                  // tp_compare
	0,                                  // tp_repr
	0,                                  // tp_as_number
	0,                                  // tp_as_sequence
	0,                                  // tp_as_mapping
	0,                                  // tp_hash
	0,                                  // tp_call
	0,                                  // tp_str
	0,                                  // tp_getattro
	0,                                  // tp_setattro
	0,                                  // tp_as_buffer
	Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
	                                    // tp_flags
	"The Query class assists in populating the parameters of a query\n"
	"operation. To create a new instance of the Query class, call the\n"
	"query() method on an instance of a Client class.\n",
	                                    // tp_doc
	0,                                  // tp_traverse
	0,                                  // tp_clear
	0,                                  // tp_richcompare
	0,                                  // tp_weaklistoffset
	0,                                  // tp_iter
	0,                                  // tp_iternext
	AerospikeQuery_Type_Methods,        // tp_methods
	0,                                  // tp_members
	0,                                  // tp_getset
	0,                                  // tp_base
	0,                                  // tp_dict
	0,                                  // tp_descr_get
	0,                                  // tp_descr_set
	0,                                  // tp_dictoffset
	(initproc) AerospikeQuery_Type_Init,
	                                    // tp_init
	0,                                  // tp_alloc
	AerospikeQuery_Type_New,     // tp_new
	0,                                  // tp_free
	0,                                  // tp_is_gc
	0                                   // tp_bases
};


/*******************************************************************************
 * PUBLIC FUNCTIONS
 ******************************************************************************/

PyTypeObject * AerospikeQuery_Ready()
{
	return PyType_Ready(&AerospikeQuery_Type) == 0 ? &AerospikeQuery_Type : NULL;
}

AerospikeQuery * AerospikeQuery_New(AerospikeClient * client, PyObject * args, PyObject * kwds)
{
	AerospikeQuery * self = (AerospikeQuery *) AerospikeQuery_Type.tp_new(&AerospikeQuery_Type, args, kwds);
	self->client = client;

	if (AerospikeQuery_Type.tp_init((PyObject *) self, args, kwds) == 0) {
		Py_INCREF(client);
		return self;
	} else {
		return NULL;
	}
}

PyObject * StoreUnicodePyObject(AerospikeQuery * self, PyObject *obj)
{
	if (self->u_objs.size < MAX_UNICODE_OBJECTS) {
		self->u_objs.ob[self->u_objs.size++] = obj;
	}
	return obj;
}
