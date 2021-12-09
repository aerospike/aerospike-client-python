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

#include <aerospike/aerospike.h>
#include <aerospike/as_key.h>
#include <aerospike/as_query.h>
#include <aerospike/as_scan.h>
#include <aerospike/as_bin.h>
#include <aerospike/as_operations.h>
#include "pool.h"

// Bin names can be of type Unicode in Python
// DB supports 32767 maximum number of bins
#define MAX_UNICODE_OBJECTS 32767
extern int counter;
extern PyObject *py_global_hosts;
extern bool user_shm_key;
extern uint32_t async_support;

typedef struct {
	PyObject_HEAD
} AerospikeNullObject;

typedef struct {
	PyObject_HEAD
} AerospikeCDTWildcardObject;

typedef struct {
	PyObject_HEAD
} AerospikeCDTInfObject;

typedef struct {
	PyObject_HEAD aerospike *as;
	int shm_key;
	int ref_cnt;
} AerospikeGlobalHosts;

typedef struct {
	as_error error;
	PyObject *callback;
} user_serializer_callback;

typedef struct {
	PyObject *ob[MAX_UNICODE_OBJECTS];
	int size;
} UnicodePyObjects;

typedef struct {
	PyObject_HEAD aerospike *as;
	int is_conn_16;
	user_serializer_callback user_serializer_call_info;
	user_serializer_callback user_deserializer_call_info;
	uint8_t is_client_put_serializer;
	uint8_t strict_types;
	bool has_connected;
	bool use_shared_connection;
	uint8_t send_bool_as;
} AerospikeClient;

typedef struct {
	PyObject_HEAD AerospikeClient *client;
	as_query query;
	UnicodePyObjects u_objs;
	as_vector *unicodeStrVector;
	as_static_pool *static_pool;
} AerospikeQuery;

typedef struct {
	PyObject_HEAD AerospikeClient *client;
	as_scan scan;
	as_vector *unicodeStrVector;
	as_static_pool *static_pool;
} AerospikeScan;

typedef struct {
	PyObject_HEAD PyObject *geo_data;
} AerospikeGeospatial;

typedef struct {
	PyDictObject dict;
} AerospikeKeyOrderedDict;