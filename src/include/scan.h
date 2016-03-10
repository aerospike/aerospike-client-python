/*******************************************************************************
 * Copyright 2013-2016 Aerospike, Inc.
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

#include <aerospike/as_scan.h>

#include "types.h"
#include "client.h"

/*******************************************************************************
 * FUNCTIONS
 ******************************************************************************/

PyTypeObject * AerospikeScan_Ready(void);

AerospikeScan * AerospikeScan_New(AerospikeClient * client, PyObject * args, PyObject * kwds);

/*******************************************************************************
 * OPERATIONS
 ******************************************************************************/

/**
 * Add a where predicate to the query.
 *
 * Selecting a single bin:
 *
 *    query.select(bin)
 *
 * Selecting multiple bins:
 *
 *    query.select(bin, bin, bin)
 *
 */
AerospikeScan * AerospikeScan_Select(AerospikeScan * self, PyObject * args, PyObject * kwds);

/**
 * Apply the specified udf on the results of the query.
 *
 *    query.apply_each(module, function, arglist)
 *
 */
// PyObject * AerospikeScan_ApplyEach(AerospikeScan * self, PyObject * args, PyObject * kwds);

/**
 * Execute the query and call the callback for each result returned.
 *
 *    def each_result(result):
 *      print result
 *
 *    query.foreach(each_result)
 *
 */
PyObject * AerospikeScan_Foreach(AerospikeScan * self, PyObject * args, PyObject * kwds);

/**
 * Execute the query and return a generator
 *
 *    for result in query.results():
 *      print result
 *
 */
PyObject * AerospikeScan_Results(AerospikeScan * self, PyObject * args, PyObject * kwds);
