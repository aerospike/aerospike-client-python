#pragma once

#include <Python.h>
#include <stdbool.h>

#include <aerospike/as_query.h>

#include "client.h"

/*******************************************************************************
 * TYPES
 ******************************************************************************/

typedef struct {
	PyObject_HEAD
	AerospikeClient * client;
	as_query query;
} AerospikeQuery;

/*******************************************************************************
 * FUNCTIONS
 ******************************************************************************/

bool AerospikeQuery_Ready();

PyObject * AerospikeQuery_Create(PyObject * self, PyObject * args, PyObject * kwds);

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
PyObject * AerospikeQuery_Select(AerospikeQuery * self, PyObject * args, PyObject * kwds);

/**
 * Add a where predicate to the query.
 *
 *		query.where(bin, predicate)
 *
 */
PyObject * AerospikeQuery_Where(AerospikeQuery * self, PyObject * args, PyObject * kwds);

/**
 * Apply the specified udf on the results of the query.
 *
 *		query.apply(module, function, arglist)
 *
 */
PyObject * AerospikeQuery_Apply(AerospikeQuery * self, PyObject * args, PyObject * kwds);

/**
 * Execute the query and call the callback for each result returned.
 *
 *		def each_result(result):
 *			print result
 *
 *		query.foreach(each_result)
 *
 */
PyObject * AerospikeQuery_Foreach(AerospikeQuery * self, PyObject * args, PyObject * kwds);

/**
 * Execute the query and return a generator
 *
 *		for result in query.results():
 *			print result
 *
 */
PyObject * AerospikeQuery_Results(AerospikeQuery * self, PyObject * args, PyObject * kwds);
