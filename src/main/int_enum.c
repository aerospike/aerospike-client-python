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

#include "int_enum.h"
#include "types.h"

static int set_int_enum_member(PyObject *py_members,
                               const struct int_enum_member *member)
{
    PyObject *py_value = PyLong_FromLong(member->value);
    if (py_value == NULL) {
        return -1;
    }

    int retval = PyDict_SetItemString(py_members, member->name, py_value);
    Py_DECREF(py_value);
    if (retval == -1) {
        return -1;
    }
    return 0;
}

static PyObject *create_int_enum_members(const struct int_enum_def *enum_def)
{
    PyObject *py_members = PyDict_New();
    if (py_members == NULL) {
        return NULL;
    }

    for (const struct int_enum_member *member = enum_def->members;
         member->name != NULL; member++) {
        int retval = set_int_enum_member(py_members, member);
        if (retval == -1) {
            Py_DECREF(py_members);
            return NULL;
        }
    }
    return py_members;
}

static PyObject *create_int_enum(const struct int_enum_def *enum_def)
{
    PyObject *py_members = create_int_enum_members(enum_def);
    if (py_members == NULL) {
        return NULL;
    }

    PyObject *py_enum_module = PyImport_ImportModule("enum");
    if (py_enum_module == NULL) {
        Py_DECREF(py_members);
        return NULL;
    }

    PyObject *py_int_enum = PyObject_GetAttrString(py_enum_module, "IntEnum");
    Py_DECREF(py_enum_module);
    if (py_int_enum == NULL) {
        Py_DECREF(py_members);
        return NULL;
    }

    PyObject *py_args = Py_BuildValue("(sO)", enum_def->name, py_members);
    Py_DECREF(py_members);
    if (py_args == NULL) {
        Py_DECREF(py_int_enum);
        return NULL;
    }

    PyObject *py_kwargs =
        Py_BuildValue("{s:s,s:s}", "module", AEROSPIKE_MODULE_NAME, "qualname",
                      enum_def->qualname);
    if (py_kwargs == NULL) {
        Py_DECREF(py_args);
        Py_DECREF(py_int_enum);
        return NULL;
    }

    PyObject *py_enum_cls = PyObject_Call(py_int_enum, py_args, py_kwargs);
    Py_DECREF(py_kwargs);
    Py_DECREF(py_args);
    Py_DECREF(py_int_enum);
    return py_enum_cls;
}

int add_int_enums(PyObject *py_dict, const struct int_enum_def *enum_defs,
                  size_t enum_count)
{
    for (size_t i = 0; i < enum_count; i++) {
        const struct int_enum_def *enum_def = &enum_defs[i];
        PyObject *py_enum_cls = create_int_enum(enum_def);
        if (py_enum_cls == NULL) {
            return -1;
        }

        int retval = PyDict_SetItemString(py_dict, enum_def->name, py_enum_cls);
        Py_DECREF(py_enum_cls);
        if (retval == -1) {
            return -1;
        }
    }
    return 0;
}
