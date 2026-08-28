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

#include <aerospike/aerospike_scan.h>
#include <aerospike/as_error.h>
#include <aerospike/as_scan.h>
#include <aerospike/as_partition.h>

#include "client.h"
#include "conversions.h"
#include "exceptions.h"
#include "policy.h"
#include "scan.h"

#undef TRACE
#define TRACE()

PyObject *AerospikeScan_Results(AerospikeScan *self, PyObject *args,
                                PyObject *kwds)
{
    PyObject *py_policy = NULL;
    PyObject *py_nodename = NULL;

    static char *kwlist[] = {"policy", "nodename", NULL};

    if (PyArg_ParseTupleAndKeywords(args, kwds, "|OO:results", kwlist,
                                    &py_policy, &py_nodename) == false) {
        return NULL;
    }

    return AerospikeScan_Foreach_Invoke(self, NULL, py_policy, NULL,
                                        py_nodename);
}
