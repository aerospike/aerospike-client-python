/*******************************************************************************
 * Copyright 2013-2026 Aerospike, Inc.
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
#include <stdbool.h>

#include <aerospike/as_error.h>
#include <aerospike/as_query.h>

#include "client.h"
#include "exceptions.h"
#include "query.h"

/**
 * Set the ORDER BY clause for a Top-K query (`ORDER BY <bin> LIMIT k`). Must
 * be paired with setting `query.top_k`. Deliberately performs no
 * cross-field validation here - the full rule table (bin-name length, k
 * range, type/flag combinations, incompatible-feature rejection, etc.) is
 * enforced by the vendored C client's as_query_validate_topk() at query
 * execution time (surfaced as aerospike.exception.ParamError), matching
 * this client's existing deferred-validation precedent for where()/select().
 */
AerospikeQuery *AerospikeQuery_OrderBy(AerospikeQuery *self, PyObject *args,
                                       PyObject *kwds)
{
    char *bin_name = NULL;
    unsigned int type = 0;
    unsigned int direction = AS_ORDER_ASCENDING;
    unsigned int flags = AS_QUERY_ORDER_BY_FLAGS_DEFAULT;

    static char *kwlist[] = {"bin", "type", "direction", "flags", NULL};

    as_error err;
    as_error_init(&err);

    if (!self || (self->client && !self->client->as)) {
        as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
        goto CLEANUP;
    }

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "sI|II:order_by", kwlist,
                                     &bin_name, &type, &direction, &flags)) {
        as_error_update(&err, AEROSPIKE_ERR_PARAM,
                        "order_by() expects (bin: str, type: int, "
                        "direction: int = ASCENDING, flags: int = 0)");
        goto CLEANUP;
    }

    as_query_order_by(&self->query, bin_name, (as_query_order_by_type)type,
                      (as_order)direction, (as_query_order_by_flags)flags);

CLEANUP:
    if (err.code != AEROSPIKE_OK) {
        raise_exception(&err);
        return NULL;
    }

    Py_INCREF(self);
    return self;
}
