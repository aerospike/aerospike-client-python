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
