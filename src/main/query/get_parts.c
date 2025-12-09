/*******************************************************************************
 * Copyright 2013-2022 Aerospike, Inc.
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

#include "client.h"
#include "exceptions.h"
#include "query.h"
#include "conversions.h"

PyObject *AerospikeQuery_Get_Partitions_status(AerospikeQuery *self)
{
    PyObject *py_parts = NULL;
    const as_partitions_status *all_parts = NULL;
    as_error err;
    as_error_init(&err);

    if (!self || !self->client->as) {
        as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid query object.");
        goto CLEANUP;
    }

    all_parts = self->query.parts_all;

    as_partitions_status_to_pyobject(&err, all_parts, &py_parts);

CLEANUP:
    if (err.code != AEROSPIKE_OK) {
        raise_exception(&err);
        return NULL;
    }

    return py_parts;
}
