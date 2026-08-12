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

#include <aerospike/aerospike.h>

#include "client.h"
#include "policy_config.h"

PyObject *AerospikeClient_Get_Policies(AerospikeClient *self, PyObject *args,
                                       PyObject *kwds)
{
    // Read from the live config, not a cached copy, so this reflects any
    // dynamic config updates applied after client construction.
    as_config *config = aerospike_load_config(self->as);
    return get_policies(&config->policies);
}