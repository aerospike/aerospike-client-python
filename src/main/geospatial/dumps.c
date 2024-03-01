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

#include <aerospike/as_error.h>

PyObject *AerospikeGeospatial_DoDumps(PyObject *geo_data, as_error *err)
{
    PyObject *initresult = NULL;

    PyObject *sysmodules = PyImport_GetModuleDict();
    PyObject *json_module = NULL;
    if (PyMapping_HasKeyString(sysmodules, "json")) {
        json_module = PyMapping_GetItemString(sysmodules, "json");
    }
    else {
        json_module = PyImport_ImportModule("json");
    }

    if (!json_module) {
        /* insert error handling here! and exit this function */
        as_error_update(err, AEROSPIKE_ERR_CLIENT,
                        "Unable to load json module");
    }
    else {
        PyObject *py_funcname = PyUnicode_FromString("dumps");
        initresult = PyObject_CallMethodObjArgs(json_module, py_funcname,
                                                geo_data, NULL);
        Py_DECREF(json_module);
        Py_DECREF(py_funcname);
    }

    return initresult;
}
