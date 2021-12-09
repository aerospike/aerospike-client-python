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

#pragma once

#include <Python.h>
#include <stdbool.h>

#include <aerospike/as_query.h>

#include "types.h"
#include "client.h"

/*******************************************************************************
 * FUNCTIONS
 ******************************************************************************/

PyTypeObject *AerospikeQuery_Ready(void);

AerospikeQuery *AerospikeQuery_New(AerospikeClient *client, PyObject *args,
								   PyObject *kwds);

/*******************************************************************************
 * OPERATIONS
 ******************************************************************************/

/**
 * Add a where predicate to the query.
 *
 * Selecting a single bin:
 *
 *		query.select(bin)
 *
 * Selecting multiple bins:
 *
 *		query.select(bin, bin, bin)
 *
 */
AerospikeQuery *AerospikeQuery_Select(AerospikeQuery *self, PyObject *args,
									  PyObject *kwds);

/**
 * Add a list of write operations to the query.
 *
 */
AerospikeQuery *AerospikeQuery_Add_Ops(AerospikeQuery *self, PyObject *args,
									   PyObject *kwds);

/**
 * Add a where predicate to the query.
 *
 * Selecting a single bin:
 *
 *		query.select(bin)
 *
 * Selecting multiple bins:
 *
 *		query.select(bin, bin, bin)
 *
 */
AerospikeQuery *AerospikeQuery_Where(AerospikeQuery *self, PyObject *args);

/**
 * Apply a list of predicates to the query.
 *
 *		query.predexp(predexps)
 *
 */
AerospikeQuery *AerospikeQuery_Predexp(AerospikeQuery *self, PyObject *args);

/**
 * Apply the specified udf on the results of the query.
 *
 *		query.apply(module, function, arglist)
 *
 */
AerospikeQuery *AerospikeQuery_Apply(AerospikeQuery *self, PyObject *args,
									 PyObject *kwds);

/**
 * Execute the query and call the callback for each result returned.
 *
 *		def each_result(result):
 *			print result
 *
 *		query.foreach(each_result)
 *
 */
PyObject *AerospikeQuery_Foreach(AerospikeQuery *self, PyObject *args,
								 PyObject *kwds);

/**
 * Execute the query and return a generator.
 *
 *		for result in query.results():
 *			print result
 *
 */
PyObject *AerospikeQuery_Results(AerospikeQuery *self, PyObject *args,
								 PyObject *kwds);

/**
 * Execute a UDF in the background. Returns the query id to allow status of the query to be monitored.
 * */

PyObject *AerospikeQuery_ExecuteBackground(AerospikeQuery *self, PyObject *args,
										   PyObject *kwds);

/**
 * Store the Unicode -> UTF8 string converted PyObject into 
 * a pool of PyObjects. So that, they will be decref'ed at later stages
 * without leaving memory trails behind.
 *		StoreUnicodePyObject(self, PyUnicode_AsUTF8String(py_bin));
 *
 */
PyObject *StoreUnicodePyObject(AerospikeQuery *self, PyObject *obj);

int64_t pyobject_to_int64(PyObject *py_obj);

/* Initialize the predexp module */
PyObject *AerospikePredExp_New(void);

as_status RegisterPredExpConstants(PyObject *module);