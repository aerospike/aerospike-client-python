/*******************************************************************************
 * Copyright 2013-2021 Aerospike, Inc.
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
#include <aerospike/as_scan.h>

#include "client.h"
#include "scan.h"
#include "conversions.h"
#include "exceptions.h"
#include "macros.h"

/*******************************************************************************
 * PYTHON DOC METHODS
 ******************************************************************************/

PyDoc_STRVAR(foreach_doc, "foreach(callback[, policy[, options [, nodename]])\n\
\n\
Invoke the callback function for each of the records streaming back from the scan. If provided \
nodename should be the Node ID of a node to limit the scan to.");

PyDoc_STRVAR(select_doc, "select(bin1[, bin2[, bin3..]])\n\
\n\
Set a filter on the record bins resulting from results() or foreach(). \
If a selected bin does not exist in a record it will not appear in the bins portion of that record tuple.");

PyDoc_STRVAR(results_doc,
			 "results([policy [, nodename]) -> list of (key, meta, bins)\n\
\n\
Buffer the records resulting from the scan, and return them as a list of records.If provided \
nodename should be the Node ID of a node to limit the scan to.");

PyDoc_STRVAR(paginate_doc, "paginate()\n\
\n\
Set pagination filter to receive records in bunch (max_records or page_size).");

PyDoc_STRVAR(is_done_doc, "is_done() -> bool\n\
\n\
Gets the status of scan");

/*******************************************************************************
 * PYTHON TYPE METHODS
 ******************************************************************************/

static PyMethodDef AerospikeScan_Type_Methods[] = {

	{"foreach", (PyCFunction)AerospikeScan_Foreach,
	 METH_VARARGS | METH_KEYWORDS, foreach_doc},

	{"select", (PyCFunction)AerospikeScan_Select, METH_VARARGS | METH_KEYWORDS,
	 select_doc},

	{"results", (PyCFunction)AerospikeScan_Results,
	 METH_VARARGS | METH_KEYWORDS, results_doc},

	{"execute_background", (PyCFunction)AerospikeScan_ExecuteBackground,
	 METH_VARARGS | METH_KEYWORDS, results_doc},

	{"apply", (PyCFunction)AerospikeScan_Apply, METH_VARARGS | METH_KEYWORDS,
	 results_doc},

	{"add_ops", (PyCFunction)AerospikeScan_Add_Ops,
	 METH_VARARGS | METH_KEYWORDS, results_doc},

	{"paginate", (PyCFunction)AerospikeScan_Paginate,
	 METH_VARARGS | METH_KEYWORDS, paginate_doc},

	{"is_done", (PyCFunction)AerospikeScan_Is_Done,
	 METH_VARARGS | METH_KEYWORDS, is_done_doc},
	{NULL}};

/*******************************************************************************
 * PYTHON TYPE HOOKS
 ******************************************************************************/

static PyObject *AerospikeScan_Type_New(PyTypeObject *type, PyObject *args,
										PyObject *kwds)
{
	AerospikeScan *self = NULL;

	self = (AerospikeScan *)type->tp_alloc(type, 0);

	if (self) {
		self->client = NULL;
	}

	return (PyObject *)self;
}

static int AerospikeScan_Type_Init(AerospikeScan *self, PyObject *args,
								   PyObject *kwds)
{
	PyObject *py_namespace = NULL;
	PyObject *py_set = NULL;

	static char *kwlist[] = {"namespace", "set", NULL};

	if (PyArg_ParseTupleAndKeywords(args, kwds, "O|O:key", kwlist,
									&py_namespace, &py_set) == false) {
		return -1;
	}

	char *namespace = NULL;
	char *set = NULL;
	PyObject *py_ustr = NULL;

	if (py_namespace && PyString_Check(py_namespace)) {
		namespace = PyString_AsString(py_namespace);
	}
	else {
		return -1;
	}

	if (py_set) {
		if (PyUnicode_Check(py_set)) {
			py_ustr = PyUnicode_AsUTF8String(py_set);
			set = PyBytes_AsString(py_ustr);
		}
		else if (PyString_Check(py_set)) {
			set = PyString_AsString(py_set);
		}
		else if (Py_None == py_set) {
			set = NULL;
		}
	}

	self->unicodeStrVector = NULL;
	self->static_pool = NULL;
	as_scan_init(&self->scan, namespace, set);

	if (py_ustr) {
		Py_DECREF(py_ustr);
	}
	return 0;
}

static void AerospikeScan_Type_Dealloc(AerospikeScan *self)
{
	as_scan_destroy(&self->scan);

	if (self->unicodeStrVector != NULL) {
		for (unsigned int i = 0; i < self->unicodeStrVector->size; ++i) {
			free(as_vector_get_ptr(self->unicodeStrVector, i));
		}

		as_vector_destroy(self->unicodeStrVector);
	}

	Py_CLEAR(self->client);
	Py_TYPE(self)->tp_free((PyObject *)self);
}

/*******************************************************************************
 * PYTHON TYPE DESCRIPTOR
 ******************************************************************************/

static PyTypeObject AerospikeScan_Type = {
	PyVarObject_HEAD_INIT(NULL, 0) "aerospike.Scan", // tp_name
	sizeof(AerospikeScan),							 // tp_basicsize
	0,												 // tp_itemsize
	(destructor)AerospikeScan_Type_Dealloc,
	// tp_dealloc
	0, // tp_print
	0, // tp_getattr
	0, // tp_setattr
	0, // tp_compare
	0, // tp_repr
	0, // tp_as_number
	0, // tp_as_sequence
	0, // tp_as_mapping
	0, // tp_hash
	0, // tp_call
	0, // tp_str
	0, // tp_getattro
	0, // tp_setattro
	0, // tp_as_buffer
	Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
	// tp_flags
	"The Scan class assists in populating the parameters of a scan\n"
	"operation. To create a new instance of the Scan class, call the\n"
	"scan() method on an instance of a Client class.\n",
	// tp_doc
	0,							// tp_traverse
	0,							// tp_clear
	0,							// tp_richcompare
	0,							// tp_weaklistoffset
	0,							// tp_iter
	0,							// tp_iternext
	AerospikeScan_Type_Methods, // tp_methods
	0,							// tp_members
	0,							// tp_getset
	0,							// tp_base
	0,							// tp_dict
	0,							// tp_descr_get
	0,							// tp_descr_set
	0,							// tp_dictoffset
	(initproc)AerospikeScan_Type_Init,
	// tp_init
	0,						// tp_alloc
	AerospikeScan_Type_New, // tp_new
	0,						// tp_free
	0,						// tp_is_gc
	0						// tp_bases
};

/*******************************************************************************
 * PUBLIC FUNCTIONS
 ******************************************************************************/

PyTypeObject *AerospikeScan_Ready()
{
	return PyType_Ready(&AerospikeScan_Type) == 0 ? &AerospikeScan_Type : NULL;
}

AerospikeScan *AerospikeScan_New(AerospikeClient *client, PyObject *args,
								 PyObject *kwds)
{
	AerospikeScan *self = (AerospikeScan *)AerospikeScan_Type.tp_new(
		&AerospikeScan_Type, args, kwds);
	self->client = client;
	Py_INCREF(client);
	if (AerospikeScan_Type.tp_init((PyObject *)self, args, kwds) != -1) {
		return self;
	}
	else {
		Py_XDECREF(self);
		as_error err;
		as_error_init(&err);
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Parameters are incorrect");
		PyObject *py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyObject *exception_type = raise_exception(&err);
		PyErr_SetObject(exception_type, py_err);
		Py_XDECREF(py_err);
		return NULL;
	}
}
