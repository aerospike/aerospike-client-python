#pragma once

#include <Python.h>
#include <stdbool.h>

#include <aerospike/as_key.h>

#include "client.h"

/*******************************************************************************
 * TYPES
 ******************************************************************************/

typedef struct {
	PyObject_HEAD
	AerospikeClient * client;
	as_key key;
} AerospikeKey;

/*******************************************************************************
 * FUNCTIONS
 ******************************************************************************/

bool AerospikeKey_Ready();

PyObject * AerospikeKey_Create(PyObject * self, PyObject * args, PyObject * kwds);

/*******************************************************************************
 * OPERATIONS
 ******************************************************************************/

/**
 * Performs a `remove` operation. This will remove the record with the 
 * specified key.
 *
 *		client.key(ns,set,key).apply(module, function, arglist)
 *
 */
PyObject * AerospikeKey_Apply(AerospikeKey * self, PyObject * args, PyObject * kwds);

/**
 * Performs a `exists` operation. This will check the existence of the record 
 * with the specified key.
 *
 *		client.key(ns,set,key).exists()
 *
 */
PyObject * AerospikeKey_Exists(AerospikeKey * self, PyObject * args, PyObject * kwds);

/**
 * Performs a `get` operation. This will read a record with the specified key.
 *
 *		client.key(ns,set,key).get()
 *
 */
PyObject * AerospikeKey_Get(AerospikeKey * self, PyObject * args, PyObject * kwds);

/**
 * Performs a `select` operation. This will select specified bins of 
 * the requested record.
 *
 *		client.key(ns,set,key).select("a","b","c")
 */
// PyObject * AerospikeKey_Select(AerospikeKey * self, PyObject * args, PyObject * kwds);

/**
 * Performs a `put` operation. This will select specified bins of the 
 * requested record.
 *
 *		client.key(ns,set,key).put({
 *			"a": 123,
 *			"b": "xyz",
 *			"c": [1,2,3]
 *		})
 *
 */
PyObject * AerospikeKey_Put(AerospikeKey * self, PyObject * args, PyObject * kwds);

/**
 * Performs a `remove` operation. This will remove the record with the 
 * specified key.
 *
 *		client.key(ns,set,key).remove()
 *
 */
PyObject * AerospikeKey_Remove(AerospikeKey * self, PyObject * args, PyObject * kwds);
