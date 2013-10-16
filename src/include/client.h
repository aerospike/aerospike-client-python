#pragma once

#include <Python.h>
#include <stdbool.h>

// #include <aerospike/aerospike.h>
// #include <aerospike/as_config.h>
// #include <aerospike/as_error.h>
// #include <aerospike/as_policy.h>

#define TRACE() printf("%s:%d\n",__FILE__,__LINE__)

/*******************************************************************************
 * TYPES
 ******************************************************************************/

struct aerospike_s;
typedef struct aerospike_s aerospike;

typedef struct {
	PyObject_HEAD
	aerospike * as;
} AerospikeClient;

/*******************************************************************************
 * FUNCTIONS
 ******************************************************************************/

bool AerospikeClient_Ready();

/**
 * Create a new Aerospike client object and connect to the database.
 */
PyObject * AerospikeClient_Create(PyObject * self, PyObject * args, PyObject * kwds);

/*******************************************************************************
 * OPERATIONS
 ******************************************************************************/

/**
 * Close the connect to the database.
 */
PyObject * AerospikeClient_Close(AerospikeClient * self, PyObject * args, PyObject * kwds);

/**
 * This will initialize a key object, which can be used to peform key 
 * operations.
 *
 *		client.key(ns,set,key).put({
 *			"a": 123,
 *			"b": "xyz",
 *			"c": [1,2,3]
 *      })
 *
 *		rec = client.key(ns,set,key).get()
 *
 */
PyObject * AerospikeClient_Key(AerospikeClient * self, PyObject * args, PyObject * kwds);

/**
 * Performs a `scan` operation. This will initialize a scan object, which can 
 * be used to scan records in specified namespace and/or set.
 *
 * A scan can be executed by calling `foreach`, which will call a callback 
 * each result returned:
 *
 *		def each_result(record):
 *			print record
 *
 *		scan = client.scan(ns,set).foreach(each_result)
 *
 * Alternatively, you can use `results()` which is a generator that will yield a
 * result for each iteration:
 *
 *		for record in client.scan(ns,set).results():
 *			print record
 *
 */
PyObject * AerospikeClient_Scan(AerospikeClient * self, PyObject * args, PyObject * kwds);

/**
 * Performs a `query` operation. This will initialize a query object, which 
 * can be used to query records in specified namespace and/or set.
 *
 * A query can be executed by calling `foreach`, which will call a callback 
 * each result returned:
 *
 *		def each_result(result):
 *			print result
 *		
 *		scan = client.query(ns,set).where("a", between(1,100)).foreach(each_result)
 *
 * Alternatively, you can use `results()` which is a generator that will yield a
 * result for each iteration:
 *
 *		for result in client.query(ns,set).where("a", range(1,100)).results():
 *			print result
 *
 */
PyObject * AerospikeClient_Query(AerospikeClient * self, PyObject * args, PyObject * kwds);

/**
 * Performs a `info` operation. This will invoke the info request against each
 * node in the cluster. The return value is a dict where the key is the node 
 * name and the value is a tuple of (Error,Response). If an error occurred on
 * the node, the Error will be an object containing details, otherwise it is
 * None. If the request was successful, then the Response will contain the 
 * string response from the node, otherwise it is None.
 *
 *		for node,(err,res) in client.info('statistics').items():
 *			if err == None:
 *				print "{0} - OK: {1}".format(record,res)
 *			else:
 *				print "{0} - ERR: {1}".format(record,err)
 *
 */
PyObject * AerospikeClient_Info(AerospikeClient * self, PyObject * args, PyObject * kwds);

