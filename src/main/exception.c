/*******************************************************************************
 * Copyright 2013-2014 Aerospike, Inc.
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

/**
 * from aerospike import predicates as p
 *
 * q = client.query(ns,set).where(p.equals("bin",1))
 */

#include <Python.h>
#include <aerospike/as_query.h>
#include <aerospike/as_error.h>

#include "conversions.h"
#include "exceptions.h"
PyObject *ParamError;
PyObject *ClientError;
PyObject *ServerError;
static PyObject *AerospikeError, *InvalidHostError, *NamespaceNotFound;
static PyObject *RecordError, *RecordKeyMismatch, *RecordNotFound, *BinNameError, *RecordGenerationError, *RecordExistsError, *RecordTooBig, *RecordBusy, *BinNameError, *BinExistsError, *BinNotFound, *BinIncompatibleType;
static PyObject *IndexError, *IndexNotFound, *IndexFoundError, *IndexOOM, *IndexNotReadable, *IndexNameMaxLen, *IndexNameMaxCount;
static PyObject *UDFError, *UDFNotFound, *LuaFileNotFound;
static PyObject *ClusterError, *ClusterChangeError;

PyObject * AerospikeException_New(void)
{
	PyObject * module = Py_InitModule3("aerospike.exception", NULL, "Exception objects");

	PyObject *py_dict = PyDict_New();
	PyDict_SetItemString(py_dict, "code", Py_None);
	PyDict_SetItemString(py_dict, "file", Py_None);
	PyDict_SetItemString(py_dict, "msg", Py_None);
	PyDict_SetItemString(py_dict, "line", Py_None);
	AerospikeError = PyErr_NewException("exception.AerospikeError", NULL, py_dict);
	Py_INCREF(AerospikeError);
	Py_DECREF(py_dict);
	PyModule_AddObject(module, "AerospikeError", AerospikeError);

	ClientError = PyErr_NewException("exception.ClientError", AerospikeError, NULL);
	Py_INCREF(ClientError);
	PyModule_AddObject(module, "ClientError", ClientError);

	ServerError = PyErr_NewException("exception.ServerError", AerospikeError, NULL);
	Py_INCREF(ServerError);
	PyModule_AddObject(module, "ServerError", ServerError);

	ParamError = PyErr_NewException("exception.ParamError", ClientError, NULL);
	Py_INCREF(ParamError);
	PyModule_AddObject(module, "ParamError", ParamError);

	InvalidHostError = PyErr_NewException("exception.InvalidHostError", ClientError, NULL);
	Py_INCREF(InvalidHostError);
	PyModule_AddObject(module, "InvalidHostError", InvalidHostError);

	//Cluster exceptions
	ClusterError = PyErr_NewException("exception.ClusterError", ServerError, NULL);
	Py_INCREF(ClusterError);
	PyModule_AddObject(module, "ClusterError", ClusterError);

	ClusterChangeError = PyErr_NewException("exception.ClusterChangeError", ClusterError, NULL);
	Py_INCREF(ClusterChangeError);
	PyModule_AddObject(module, "ClusterChangeError", ClusterChangeError);

	//Record exceptions
	PyObject *py_record_dict = PyDict_New();
	PyDict_SetItemString(py_record_dict, "key", Py_None);
	PyDict_SetItemString(py_record_dict, "bins", Py_None);

	RecordError = PyErr_NewException("exception.RecordError", ServerError, py_record_dict);
	Py_INCREF(RecordError);
	Py_DECREF(py_record_dict);
	PyModule_AddObject(module, "RecordError", RecordError);

	RecordKeyMismatch = PyErr_NewException("exception.RecordKeyMismatch", RecordError, NULL);
	Py_INCREF(RecordKeyMismatch);
	PyModule_AddObject(module, "RecordKeyMismatch", RecordKeyMismatch);

	RecordNotFound = PyErr_NewException("exception.RecordNotFound", RecordError, NULL);
	Py_INCREF(RecordNotFound);
	PyModule_AddObject(module, "RecordNotFound", RecordNotFound);

	RecordGenerationError = PyErr_NewException("exception.RecordGenerationError", RecordError, NULL);
	Py_INCREF(RecordGenerationError);
	PyModule_AddObject(module, "RecordGenerationError", RecordGenerationError);

	RecordExistsError = PyErr_NewException("exception.RecordExistsError", RecordError, NULL);
	Py_INCREF(RecordExistsError);
	PyModule_AddObject(module, "RecordExistsError", RecordExistsError);

	RecordTooBig = PyErr_NewException("exception.RecordTooBig", RecordError, NULL);
	Py_INCREF(RecordTooBig);
	PyModule_AddObject(module, "RecordTooBig", RecordTooBig);

	RecordBusy = PyErr_NewException("exception.RecordBusy", RecordError, NULL);
	Py_INCREF(RecordBusy);
	PyModule_AddObject(module, "RecordBusy", RecordBusy);

	BinNameError = PyErr_NewException("exception.BinNameError", RecordError, NULL);
	Py_INCREF(BinNameError);
	PyModule_AddObject(module, "BinNameError", BinNameError);

	BinExistsError = PyErr_NewException("exception.BinExistsError", RecordError, NULL);
	Py_INCREF(BinExistsError);
	PyModule_AddObject(module, "BinExistsError", BinExistsError);

	BinNotFound = PyErr_NewException("exception.BinNotFound", RecordError, NULL);
	Py_INCREF(BinNotFound);
	PyModule_AddObject(module, "BinNotFound", BinNotFound);

	BinIncompatibleType = PyErr_NewException("exception.BinIncompatibleType", RecordError, NULL);
	Py_INCREF(BinIncompatibleType);
	PyModule_AddObject(module, "BinIncompatibleType", BinIncompatibleType);

	NamespaceNotFound = PyErr_NewException("exception.NamespaceNotFound", ServerError, NULL);
	Py_INCREF(NamespaceNotFound);
	PyModule_AddObject(module, "NamespaceNotFound", NamespaceNotFound);

	//Index exceptions
	PyObject *py_index_dict = PyDict_New();
	PyDict_SetItemString(py_index_dict, "name", Py_None);
	IndexError = PyErr_NewException("exception.IndexError", ServerError, NULL);
	Py_INCREF(IndexError);
	PyModule_AddObject(module, "IndexError", IndexError);

	IndexNotFound = PyErr_NewException("exception.IndexNotFound", IndexError, NULL);
	Py_INCREF(IndexNotFound);
	PyModule_AddObject(module, "IndexNotFound", IndexNotFound);

	IndexFoundError = PyErr_NewException("exception.IndexFoundError", IndexError, NULL);
	Py_INCREF(IndexFoundError);
	PyModule_AddObject(module, "IndexFoundError", IndexFoundError);

	IndexOOM = PyErr_NewException("exception.IndexOOM", IndexError, NULL);
	Py_INCREF(IndexOOM);
	PyModule_AddObject(module, "IndexOOM", IndexOOM);

	IndexNotReadable = PyErr_NewException("exception.IndexNotReadable", IndexError, NULL);
	Py_INCREF(IndexNotReadable);
	PyModule_AddObject(module, "IndexNotReadable", IndexNotReadable);

	IndexNameMaxLen = PyErr_NewException("exception.IndexNameMaxLen", IndexError, NULL);
	Py_INCREF(IndexNameMaxLen);
	PyModule_AddObject(module, "IndexNameMaxLen", IndexNameMaxLen);

	IndexNameMaxCount = PyErr_NewException("exception.IndexNameMaxCount", IndexError, NULL);
	Py_INCREF(IndexNameMaxCount);
	PyModule_AddObject(module, "IndexNameMaxCount", IndexNameMaxCount);

	//UDF exceptions
	PyObject *py_udf_dict = PyDict_New();
	PyDict_SetItemString(py_udf_dict, "module", Py_None);	
	PyDict_SetItemString(py_udf_dict, "func", Py_None);	

	UDFError = PyErr_NewException("exception.UDFError", ServerError, py_udf_dict);
	Py_INCREF(UDFError);
	Py_DECREF(py_udf_dict);
	PyModule_AddObject(module, "UDFError", UDFError);

	UDFNotFound = PyErr_NewException("exception.UDFNotFound", UDFError, NULL);
	Py_INCREF(UDFNotFound);
	PyModule_AddObject(module, "UDFNotFound", UDFNotFound);

	LuaFileNotFound = PyErr_NewException("exception.LuaFileNotFound", UDFError, NULL);
	Py_INCREF(LuaFileNotFound);
	PyModule_AddObject(module, "LuaFileNotFound", LuaFileNotFound);
	return module;
}

PyObject* raise_exception(as_error *err) {
		PyObject * type_of_exception = NULL;
		switch(err->code) {
			case -1:
				type_of_exception = ClientError;
			break;

			case -2:
				type_of_exception = ParamError;
			break;

			case 20:
				type_of_exception = NamespaceNotFound;
			break;

			//Catch Record Errors
			case 19:
				type_of_exception = RecordKeyMismatch;
			break;

			case 2:
				type_of_exception = RecordNotFound;
			break;

			case 3:
				type_of_exception = RecordGenerationError;
			break;

			case 5:
				type_of_exception = RecordExistsError;
			break;

			case 13:
				type_of_exception = RecordTooBig;
			break;

			case 14:
				type_of_exception = RecordBusy;
			break;

			case 21:
				type_of_exception = BinNameError;
			break;

			case 6:
				type_of_exception = BinExistsError;
			break;

			case 17:
				type_of_exception = BinNotFound;
			break;

			case 12:
				type_of_exception = BinIncompatibleType;
			break;

			//Catch cluster errors
			case 11:
				type_of_exception = ClusterError;
			break;

			case 7:
				type_of_exception = ClusterChangeError;
			break;

			//Catch index exceptions
			case 201:
				type_of_exception = IndexNotFound;
			break;

			case 200:
				type_of_exception = IndexFoundError;
			break;

			case 202:
				type_of_exception = IndexOOM;
			break;

			case 203:
				type_of_exception = IndexNotReadable;
			break;

			case 205:
				type_of_exception = IndexNameMaxLen;
			break;

			case 206:
				type_of_exception = IndexNameMaxCount;
			break;

			//Catch udf exceptions
			case 100:
				type_of_exception = UDFError;
			break;

			case 1301:
				type_of_exception = UDFNotFound;
			break;

			case 1302:
				type_of_exception = LuaFileNotFound;
			break;
		}

		PyObject_SetAttrString(type_of_exception, "code", PyInt_FromLong(err->code));
		PyObject_SetAttrString(type_of_exception, "msg", PyString_FromString(err->message));
		PyObject_SetAttrString(type_of_exception, "file", PyString_FromString(err->file));
		PyObject_SetAttrString(type_of_exception, "line", PyInt_FromLong(err->line));

		return type_of_exception;
}
