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
#include <aerospike/as_query.h>

#include "client.h"
#include "geo.h"
#include "conversions.h"
#include "exceptions.h"

/*******************************************************************************
 * PYTHON TYPE METHODS
 ******************************************************************************/
static PyMemberDef AerospikeGeospatial_Type_Members[] = {
	{"geo_data", T_OBJECT, offsetof(AerospikeGeospatial, geo_data), 0,
	 "The aerospike.GeoJSON object"},
	{NULL}};
static PyMethodDef AerospikeGeospatial_Type_Methods[] = {

	{"wrap", (PyCFunction)AerospikeGeospatial_Wrap,
	 METH_VARARGS | METH_KEYWORDS,
	 "Sets the geospatial data in the aerospike.GeoJSON object"},

	{"unwrap", (PyCFunction)AerospikeGeospatial_Unwrap,
	 METH_VARARGS | METH_KEYWORDS,
	 "Returns the geospatial data contained in the aerospike.GeoJSON object"},

	{"loads", (PyCFunction)AerospikeGeospatial_Loads,
	 METH_VARARGS | METH_KEYWORDS,
	 "Set the geospatial data from a raw GeoJSON string"},

	{"dumps", (PyCFunction)AerospikeGeospatial_Dumps,
	 METH_VARARGS | METH_KEYWORDS,
	 "Get the geospatial data in form of a GeoJSON string."},

	/*{"where",	(PyCFunction) AerospikeQuery_Where,		METH_VARARGS,
				"Predicate to be applied to the query."},*/

	{NULL}};

/*******************************************************************************
 * PYTHON TYPE HOOKS
 ******************************************************************************/
void store_geodata(AerospikeGeospatial *self, as_error *err,
				   PyObject *py_geodata)
{
	if (PyDict_Check(py_geodata)) {
		if (self->geo_data) {
			Py_DECREF(self->geo_data);
		}
		self->geo_data = py_geodata;
	}
	else {
		as_error_update(
			err, AEROSPIKE_ERR_PARAM,
			"Geospatial data should be a dictionary or raw GeoJSON string");
	}
}

static PyObject *AerospikeGeospatial_Type_New(PyTypeObject *type,
											  PyObject *args, PyObject *kwds)
{
	AerospikeGeospatial *self = NULL;

	self = (AerospikeGeospatial *)type->tp_alloc(type, 0);

	return (PyObject *)self;
}

static int AerospikeGeospatial_Type_Init(AerospikeGeospatial *self,
										 PyObject *args, PyObject *kwds)
{
	PyObject *py_geodata = NULL;
	PyObject *initresult = NULL;

	as_error err;
	as_error_init(&err);

	static char *kwlist[] = {"geo_data", NULL};

	if (PyArg_ParseTupleAndKeywords(args, kwds, "O:GeoJSON", kwlist,
									&py_geodata) == false) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM,
						"GeoJSON() expects exactly 1 parameter");
		goto CLEANUP;
	}

	if (PyString_Check(py_geodata)) {
		initresult = AerospikeGeospatial_DoLoads(py_geodata, &err);
		if (!initresult) {
			as_error_update(&err, AEROSPIKE_ERR_CLIENT,
							"String is not GeoJSON serializable");
			goto CLEANUP;
		}
		store_geodata(self, &err, initresult);
	}
	else {
		store_geodata(self, &err, py_geodata);
	}

CLEANUP:

	if (err.code != AEROSPIKE_OK) {
		PyObject *py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyObject *exception_type = raise_exception(&err);
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return -1;
	}

	Py_INCREF(self->geo_data);
	if (initresult) {
		Py_DECREF(initresult);
	}
	return 0;
}

PyObject *AerospikeGeospatial_Type_Repr(self) AerospikeGeospatial *self;
{
	PyObject *initresult = NULL, *py_return = NULL;
	char *new_repr_str = NULL;
	// Aerospike error object
	as_error err;
	// Initialize error object
	as_error_init(&err);

	if (!self) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid geospatial object");
		goto CLEANUP;
	}

	initresult = AerospikeGeospatial_DoDumps(self->geo_data, &err);
	if (!initresult) {
		as_error_update(&err, AEROSPIKE_ERR_CLIENT,
						"Unable to call get data in str format");
		goto CLEANUP;
	}
	char *initresult_str = PyString_AsString(initresult);
	new_repr_str = (char *)malloc(strlen(initresult_str) + 3);
	memset(new_repr_str, '\0', strlen(initresult_str) + 3);
	snprintf(new_repr_str, strlen(initresult_str) + 3, "\'%s\'",
			 initresult_str);

CLEANUP:

	// If an error occurred, tell Python.
	if (err.code != AEROSPIKE_OK) {
		PyObject *py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyObject *exception_type = raise_exception(&err);
		PyErr_SetObject(exception_type, py_err);
		Py_XDECREF(py_err);
		if (new_repr_str) {
			free(new_repr_str);
		}
		return NULL;
	}

	py_return = PyString_FromString(new_repr_str);
	Py_XDECREF(initresult);
	free(new_repr_str);
	return py_return;
}

PyObject *AerospikeGeospatial_Type_Str(self) AerospikeGeospatial *self;
{
	PyObject *initresult = NULL;
	// Aerospike error object
	as_error err;
	// Initialize error object
	as_error_init(&err);

	if (!self) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid geospatial object");
		goto CLEANUP;
	}

	initresult = AerospikeGeospatial_DoDumps(self->geo_data, &err);
	if (!initresult) {
		as_error_update(&err, AEROSPIKE_ERR_CLIENT,
						"Unable to call get data in str format");
		goto CLEANUP;
	}

CLEANUP:

	// If an error occurred, tell Python.
	if (err.code != AEROSPIKE_OK) {
		PyObject *py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyObject *exception_type = raise_exception(&err);
		PyErr_SetObject(exception_type, py_err);
		Py_XDECREF(py_err);
		return NULL;
	}
	return initresult;
}
static void AerospikeGeospatial_Type_Dealloc(AerospikeGeospatial *self)
{
	if (self->geo_data) {
		Py_DECREF(self->geo_data);
	}
	Py_TYPE(self)->tp_free((PyObject *)self);
}

/*******************************************************************************
 * PYTHON TYPE DESCRIPTOR
 ******************************************************************************/
static PyTypeObject AerospikeGeospatial_Type = {
	PyVarObject_HEAD_INIT(NULL, 0) "aerospike.Geospatial", // tp_name
	sizeof(AerospikeGeospatial),						   // tp_basicsize
	0,													   // tp_itemsize
	(destructor)AerospikeGeospatial_Type_Dealloc,
	// tp_dealloc
	0,							   // tp_print
	0,							   // tp_getattr
	0,							   // tp_setattr
	0,							   // tp_compare
	AerospikeGeospatial_Type_Repr, // tp_repr
	0,							   // tp_as_number
	0,							   // tp_as_sequence
	0,							   // tp_as_mapping
	0,							   // tp_hash
	0,							   // tp_call
	AerospikeGeospatial_Type_Str,  // tp_str
	0,							   // tp_getattro
	0,							   // tp_setattro
	0,							   // tp_as_buffer
	Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
	// tp_flags
	"The GeoJSON class casts geospatial data to and from the server's\n"
	"as_geojson type.\n",
	// tp_doc
	0,								  // tp_traverse
	0,								  // tp_clear
	0,								  // tp_richcompare
	0,								  // tp_weaklistoffset
	0,								  // tp_iter
	0,								  // tp_iternext
	AerospikeGeospatial_Type_Methods, // tp_methods
	AerospikeGeospatial_Type_Members, // tp_members
	0,								  // tp_getset
	0,								  // tp_base
	0,								  // tp_dict
	0,								  // tp_descr_get
	0,								  // tp_descr_set
	0,								  // tp_dictoffset
	(initproc)AerospikeGeospatial_Type_Init,
	// tp_init
	0,							  // tp_alloc
	AerospikeGeospatial_Type_New, // tp_new
	0,							  // tp_free
	0,							  // tp_is_gc
	0							  // tp_bases
};

/*******************************************************************************
 * PUBLIC FUNCTIONS
 ******************************************************************************/

PyTypeObject *AerospikeGeospatial_Ready()
{
	return PyType_Ready(&AerospikeGeospatial_Type) == 0
			   ? &AerospikeGeospatial_Type
			   : NULL;
}

AerospikeGeospatial *Aerospike_Set_Geo_Data(PyObject *parent, PyObject *args,
											PyObject *kwds)
{
	// Python function arguments
	PyObject *py_geodata = NULL;
	// Python function keyword arguments
	static char *kwlist[] = {"geo_data", NULL};
	as_error err;
	as_error_init(&err);

	if (PyArg_ParseTupleAndKeywords(args, kwds, "O:geodata", kwlist,
									&py_geodata) == false) {
		return NULL;
	}

	if (PyDict_Check(py_geodata)) {
		AerospikeGeospatial *self =
			(AerospikeGeospatial *)AerospikeGeospatial_Type.tp_new(
				&AerospikeGeospatial_Type, args, kwds);
		if (AerospikeGeospatial_Type.tp_init((PyObject *)self, args, kwds) ==
			0) {
			return self;
		}
		else {
			return NULL;
		}
	}
	else {
		as_error_update(&err, AEROSPIKE_ERR_PARAM,
						"The geospatial data should be a dictionary");
	}

	if (err.code != AEROSPIKE_OK) {
		PyObject *py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyObject *exception_type = raise_exception(&err);
		PyErr_SetObject(exception_type, py_err);
		Py_XDECREF(py_err);
	}
	return NULL;
}

AerospikeGeospatial *Aerospike_Set_Geo_Json(PyObject *parent, PyObject *args,
											PyObject *kwds)
{
	// Python function arguments
	PyObject *py_geodata = NULL;
	// Python function keyword arguments
	static char *kwlist[] = {"geojson_str", NULL};
	as_error err;
	as_error_init(&err);

	if (PyArg_ParseTupleAndKeywords(args, kwds, "O:geojson", kwlist,
									&py_geodata) == false) {
		return NULL;
	}

	if (PyString_Check(py_geodata)) {
		AerospikeGeospatial *self =
			(AerospikeGeospatial *)AerospikeGeospatial_Type.tp_new(
				&AerospikeGeospatial_Type, args, kwds);
		if (AerospikeGeospatial_Type.tp_init((PyObject *)self, args, kwds) ==
			0) {
			return self;
		}
		else {
			return NULL;
		}
	}
	else {
		as_error_update(&err, AEROSPIKE_ERR_PARAM,
						"The geospatial data should be a GeoJSON string");
	}

	if (err.code != AEROSPIKE_OK) {
		PyObject *py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyObject *exception_type = raise_exception(&err);
		PyErr_SetObject(exception_type, py_err);
		Py_XDECREF(py_err);
	}
	return NULL;
}

PyObject *AerospikeGeospatial_New(as_error *err, PyObject *value)
{
	AerospikeGeospatial *self =
		(AerospikeGeospatial *)AerospikeGeospatial_Type.tp_new(
			&AerospikeGeospatial_Type, Py_None, Py_None);
	store_geodata(self, err, value);
	Py_XINCREF(self->geo_data);
	return (PyObject *)self;
}
