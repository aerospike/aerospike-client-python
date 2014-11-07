/*******************************************************************************
 * Copyright 2013-2014 Aerospike, Inc.
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

#include <Python.h>
#include <aerospike/as_error.h>
#include <aerospike/as_policy.h>

enum Aerospike_constants {
    OPT_CONNECT_TIMEOUT = 1,
    OPT_READ_TIMEOUT,
    OPT_WRITE_TIMEOUT,
    OPT_POLICY_RETRY,
    OPT_POLICY_EXISTS,
    OPT_POLICY_KEY,
    OPT_POLICY_GEN
};

#define AS_POLICY_RETRY 0x00000010
#define AS_POLICY_EXISTS 0x00000100
#define AS_UDF_TYPE 0x00010000
#define AS_POLICY_KEY_DIGEST 0x10000000
#define AS_POLICY_KEY_GEN 0x100000000

/*
 *******************************************************************************************************
 * Enum for PHP client's optional policy constant values. (POLICY_*)
 *******************************************************************************************************
 */

enum Aerospike_values {
    POLICY_RETRY_NONE      = AS_POLICY_RETRY,
    POLICY_RETRY_ONCE,
    POLICY_EXISTS_IGNORE   = AS_POLICY_EXISTS,
    POLICY_EXISTS_CREATE,
    POLICY_EXISTS_UPDATE,
    POLICY_EXISTS_REPLACE,
    POLICY_EXISTS_CREATE_OR_REPLACE,
    UDF_TYPE_LUA           = AS_UDF_TYPE,
    POLICY_KEY_DIGEST      = AS_POLICY_KEY_DIGEST,
    POLICY_KEY_SEND,
    POLICY_GEN_IGNORE      = AS_POLICY_KEY_GEN,
    POLICY_GEN_EQ,
    POLICY_GEN_GT
};

#define MAX_CONSTANT_STR_SIZE 512

/*
 *******************************************************************************************************
 *Structure to map constant number to constant name string for Aerospike constants.
 *******************************************************************************************************
 */
typedef struct Aerospike_Constants {
    long    constantno;
    char    constant_str[MAX_CONSTANT_STR_SIZE];
}AerospikeConstants;

#define AEROSPIKE_CONSTANTS_ARR_SIZE (sizeof(aerospike_constants)/sizeof(AerospikeConstants))

/*
 *******************************************************************************************************
 * Mapping of constant number to constant name string.
 *******************************************************************************************************
 */
static
AerospikeConstants aerospike_constants[] = {
    { POLICY_RETRY_NONE                 ,   "POLICY_RETRY_NONE" },
    { POLICY_RETRY_ONCE                 ,   "POLICY_RETRY_ONCE" },
    { POLICY_EXISTS_IGNORE              ,   "POLICY_EXISTS_IGNORE" },
    { POLICY_EXISTS_CREATE              ,   "POLICY_EXISTS_CREATE" },
    { POLICY_EXISTS_UPDATE              ,   "POLICY_EXISTS_UPDATE" },
    { POLICY_EXISTS_REPLACE             ,   "POLICY_EXISTS_REPLACE" },
    { POLICY_EXISTS_CREATE_OR_REPLACE   ,   "POLICY_EXISTS_CREATE_OR_REPLACE" },
    { UDF_TYPE_LUA                      ,   "UDF_TYPE_LUA" },
    { POLICY_KEY_DIGEST                 ,   "POLICY_KEY_DIGEST" },
    { POLICY_KEY_SEND                   ,   "POLICY_KEY_SEND" },
    { POLICY_GEN_IGNORE                 ,   "POLICY_GEN_IGNORE" },
    { POLICY_GEN_EQ                     ,   "POLICY_GEN_EQ" },
    { POLICY_GEN_GT                     ,   "POLICY_GEN_GT" },
    { OPT_CONNECT_TIMEOUT               ,   "OPT_CONNECT_TIMEOUT" },
    { OPT_READ_TIMEOUT                  ,   "OPT_READ_TIMEOUT" },
    { OPT_WRITE_TIMEOUT                 ,   "OPT_WRITE_TIMEOUT" },
    { OPT_POLICY_RETRY                  ,   "OPT_POLICY_RETRY" },
    { OPT_POLICY_EXISTS                 ,   "OPT_POLICY_EXISTS" },
    { OPT_POLICY_KEY                    ,   "OPT_POLICY_KEY" },
    { OPT_POLICY_GEN                    ,   "OPT_POLICY_GEN" }
};

as_status pyobject_to_policy_admin(as_error * err, PyObject * py_policy,
									as_policy_admin * policy,
									as_policy_admin ** policy_p);

as_status pyobject_to_policy_apply(as_error * err, PyObject * py_policy,
									as_policy_apply * policy,
									as_policy_apply ** policy_p);

as_status pyobject_to_policy_info(as_error * err, PyObject * py_policy,
									as_policy_info * policy,
									as_policy_info ** policy_p);

as_status pyobject_to_policy_query(as_error * err, PyObject * py_policy,
									as_policy_query * policy,
									as_policy_query ** policy_p);

as_status pyobject_to_policy_read(as_error * err, PyObject * py_policy,
									as_policy_read * policy,
									as_policy_read ** policy_p);

as_status pyobject_to_policy_remove(as_error * err, PyObject * py_policy,
									as_policy_remove * policy,
									as_policy_remove ** policy_p);

as_status pyobject_to_policy_scan(as_error * err, PyObject * py_policy,
									as_policy_scan * policy,
									as_policy_scan ** policy_p);

as_status pyobject_to_policy_write(as_error * err, PyObject * py_policy,
									as_policy_write * policy,
									as_policy_write ** policy_p);

as_status pyobject_to_policy_operate(as_error * err, PyObject * py_policy,
                                    as_policy_operate * policy,
                                    as_policy_operate ** policy_p);

as_status set_policy(as_error *err, PyObject * py_policy, 
        as_policy_operate* operate_policy);

void declare_poliy_constants(PyObject *aerospike);
