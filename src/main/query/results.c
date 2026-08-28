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

#include <Python.h>
#include <pthread.h>
#include <stdbool.h>

#include <aerospike/aerospike_query.h>
#include <aerospike/as_error.h>
#include <aerospike/as_query.h>
#include <aerospike/as_arraylist.h>

#include "client.h"
#include "conversions.h"
#include "exceptions.h"
#include "query.h"
#include "policy.h"

#undef TRACE
#define TRACE()

PyObject *AerospikeQuery_Results(AerospikeQuery *self, PyObject *args,
                                 PyObject *kwds)
{
    PyObject *py_policy = NULL;
    PyObject *py_options = NULL;

    static char *kwlist[] = {"policy", "options", NULL};

    if (PyArg_ParseTupleAndKeywords(args, kwds, "|OO:results", kwlist,
                                    &py_policy, &py_options) == false) {
        return NULL;
    }

    return AerospikeQuery_Foreach_Invoke(self, NULL, py_policy, py_options);
}
