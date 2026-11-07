/*******************************************************************************
 * Copyright 2026 Aerospike, Inc.
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

#include <aerospike/as_config.h>
#include <aerospike/as_error.h>

// Copy a dict of str to str into a new as_addr_map array. option_name names
// the dict in error messages. On failure nothing is allocated and the outputs
// are left unchanged.
as_status addr_map_from_pydict(as_error *err, PyObject *py_map,
                               const char *option_name, as_addr_map **entries,
                               uint32_t *size);

void addr_map_destroy(as_addr_map *entries, uint32_t size);

// Set config->discovery from the client config's "discovery" dict and
// register its seed provider.
as_status set_discovery_config(as_error *err, as_config *config,
                               PyObject *py_discovery, bool validate_keys);

// Set config->translation from the client config's "translation" dict and
// register its address translator. Must run after ip_map is set.
as_status set_translation_config(as_error *err, as_config *config,
                                 PyObject *py_translation, bool validate_keys);
