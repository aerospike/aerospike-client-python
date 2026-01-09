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
#include <aerospike/as_txn.h>
#include <aerospike/as_config.h>

#include "pool.h"

#define AEROSPIKE_MODULE_NAME "aerospike"
#define FULLY_QUALIFIED_TYPE_NAME(name) AEROSPIKE_MODULE_NAME "." name

// Bin names can be of type Unicode in Python
// DB supports 32767 maximum number of bins
#define MAX_UNICODE_OBJECTS 32767
extern int counter;
extern PyObject *py_global_hosts;
extern bool user_shm_key;

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
    bool validate_keys;
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

typedef struct {
    PyObject_HEAD
        /* Type-specific fields go here. */
        as_txn *txn;
} AerospikeTransaction;

extern PyTypeObject AerospikeTransaction_Type;

typedef struct {
    PyObject_HEAD char *path;
    uint32_t interval;
} AerospikeConfigProvider;

extern PyTypeObject AerospikeConfigProvider_Type;

// These are defined in aerospike.c
extern PyObject *py_client_config_valid_keys;
extern PyObject *py_client_config_shm_valid_keys;
extern PyObject *py_client_config_lua_valid_keys;
extern PyObject *py_client_config_policies_valid_keys;
extern PyObject *py_client_config_tls_valid_keys;
extern PyObject *py_apply_policy_valid_keys;
extern PyObject *py_admin_policy_valid_keys;
extern PyObject *py_info_policy_valid_keys;
extern PyObject *py_query_policy_valid_keys;
extern PyObject *py_read_policy_valid_keys;
extern PyObject *py_remove_policy_valid_keys;
extern PyObject *py_scan_policy_valid_keys;
extern PyObject *py_write_policy_valid_keys;
extern PyObject *py_operate_policy_valid_keys;
extern PyObject *py_batch_policy_valid_keys;
extern PyObject *py_batch_write_policy_valid_keys;
extern PyObject *py_batch_read_policy_valid_keys;
extern PyObject *py_batch_apply_policy_valid_keys;
extern PyObject *py_batch_remove_policy_valid_keys;
extern PyObject *py_bit_policy_valid_keys;
extern PyObject *py_map_policy_valid_keys;
extern PyObject *py_list_policy_valid_keys;
extern PyObject *py_hll_policy_valid_keys;
// query.apply() takes in one policy parameter that accepts both write and info policy options
extern PyObject *py_info_and_write_policy_valid_keys;
// scan.apply() takes in one policy parameter that accepts both write and info policy options
extern PyObject *py_info_and_scan_policy_valid_keys;
extern PyObject *py_record_metadata_valid_keys;

#define INVALID_DICTIONARY_KEY_ERROR_PART1 "is an invalid"
#define INVALID_DICTIONARY_KEY_ERROR_PART2 "dictionary key"
#define INVALID_DICTIONARY_KEY_ERROR                                           \
    "\"%S\" " INVALID_DICTIONARY_KEY_ERROR_PART1                               \
    " %s " INVALID_DICTIONARY_KEY_ERROR_PART2

// py_set_of_valid_keys contains the valid keys
// Return -1 if we failed to validate dictionary
// Return 0 and set err if dictionary has invalid keys
// Return 1 if dictionary's keys are all valid
//
// adjective is for error reporting only
extern int does_py_dict_contain_valid_keys(as_error *err, PyObject *py_dict,
                                           PyObject *py_set_of_valid_keys,
                                           const char *adjective);
