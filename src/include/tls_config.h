#pragma once
/*******************************************************************************

 * Copyright 2017-2021 Aerospike, Inc.
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

#include <aerospike/as_config.h>

#include "macros.h"

typedef struct {
    char *tls_key;
    char *expected_type;
} as_error_type_info;

as_error_type_info *setup_tls_config(as_config *config, PyObject *tls_config);
