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

#pragma once

#include <Python.h>

struct int_enum_member {
    const char *name;
    long value;
};

struct int_enum_def {
    const char *name;
    // Ends with a {NULL} member.
    const struct int_enum_member *members;
};

// Creates an enum.IntEnum class for each definition and adds it to py_dict
// under its name. owner_name is the name of the class whose dict py_dict is,
// or NULL for the module's dict; it is needed to pickle the enum members.
// Returns -1 with a Python exception set on failure.
int add_int_enums(PyObject *py_dict, const char *owner_name,
                  const struct int_enum_def *enum_defs, size_t enum_count);
