#pragma once

#include <Python.h>
#include <stdbool.h>

#include <aerospike/aerospike.h>
#include <aerospike/as_key.h>
#include <aerospike/as_query.h>
#include <aerospike/as_scan.h>


typedef struct {
	PyObject_HEAD
	aerospike * as;
} AerospikeClient;

typedef struct {
	PyObject_HEAD
	AerospikeClient * client;
	PyObject * namespace;
	PyObject * set;
	PyObject * key;
} AerospikeKey;

typedef struct {
	PyObject_HEAD
	AerospikeClient * client;
	as_query query;
} AerospikeQuery;

typedef struct {
  PyObject_HEAD
  AerospikeClient * client;
  as_scan scan;
} AerospikeScan;