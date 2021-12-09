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

#include <aerospike/as_scan.h>

#include "types.h"
#include "client.h"

#define CLUSTER_NPARTITIONS	(4096)

/*******************************************************************************
 * FUNCTIONS
 ******************************************************************************/

PyTypeObject *AerospikeScan_Ready(void);

AerospikeScan *AerospikeScan_New(AerospikeClient *client, PyObject *args,
								 PyObject *kwds);

/*******************************************************************************
 * OPERATIONS
 ******************************************************************************/

/**
 * Select which bins will be returned.
 *
 * Selecting a single bin:
 *
 *    scan.select(bin)
 *
 * Selecting multiple bins:
 *
 *    scan.select(bin, bin, bin)
 *
 */
AerospikeScan *AerospikeScan_Select(AerospikeScan *self, PyObject *args,
									PyObject *kwds);

/**
 * Apply the specified udf on the records scanned.
 *
 *    scan.apply_each(module, function, arglist)
 *
 */
// PyObject * AerospikeScan_ApplyEach(AerospikeScan * self, PyObject * args, PyObject * kwds);

/**
 * Execute the scan and call the callback for each result returned.
 *
 *    def each_result(result):
 *      print result
 *
 *    scan.foreach(each_result)
 *
 */
PyObject *AerospikeScan_Foreach(AerospikeScan *self, PyObject *args,
								PyObject *kwds);

/**
 * Execute the scan and return a generator.
 *
 *    for result in scan.results():
 *      print result
 *
 */
PyObject *AerospikeScan_Results(AerospikeScan *self, PyObject *args,
								PyObject *kwds);

/**
 * Execute the scan in the background.
 *
 *    job_id = scan.execute_background()
 *
 */
PyObject *AerospikeScan_ExecuteBackground(AerospikeScan *self, PyObject *args,
										  PyObject *kwds);

/**
 * Apply the specified udf on the results of the scan.
 *
 *    scan.apply(module, function, arglist)
 *
 */
AerospikeScan *AerospikeScan_Apply(AerospikeScan *self, PyObject *args,
								   PyObject *kwds);

/**
 * Add an ops list to the scan.
 *
 */
AerospikeScan *AerospikeScan_Add_Ops(AerospikeScan *self, PyObject *args,
									 PyObject *kwds);

/**
 * Set pagination filter to receive records in bunch (max_records or page_size).
 *
 *    scan.paginate()
 *
 */
PyObject *AerospikeScan_Paginate(AerospikeScan *self, PyObject *args,
								 PyObject *kwds);

/**
 * Gets the status of scan.
 *
 *    scan.is_done()
 *
 */
PyObject *AerospikeScan_Is_Done(AerospikeScan *self, PyObject *args,
								PyObject *kwds);
