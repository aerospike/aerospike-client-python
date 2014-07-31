/*******************************************************************************
 * Copyright 2013-2014 Aerospike, Inc.
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

#include <aerospike/as_admin.h>
#include <aerospike/as_key.h>
#include <aerospike/as_error.h>
#include <aerospike/as_record.h>
#include <aerospike/as_udf.h>

#include "key.h"

as_status as_udf_file_to_pyobject(as_error *err, as_udf_file * entry, PyObject ** py_file);

as_status as_udf_files_to_pyobject(as_error *err, as_udf_files *files, PyObject **py_files);

as_status strArray_to_pyobject(as_error * err, char str_array_ptr[][AS_ROLE_SIZE], PyObject **py_list, int roles_size);

as_status as_user_roles_to_pyobject(as_error *err, as_user_roles *user_roles, PyObject **py_as_user_roles);


as_status as_user_roles_array_to_pyobject(as_error *err, as_user_roles **user_roles, PyObject **py_as_user_roles, int users);

as_status pyobject_to_strArray(as_error * err, PyObject * py_list,  char **arr);

as_status pykey_to_key(as_error * err, AerospikeKey * py_key, as_key * key);

as_status pyobject_to_val(as_error * err, PyObject * py_obj, as_val ** val);

as_status pyobject_to_map(as_error * err, PyObject * py_dict, as_map ** map);

as_status pyobject_to_list(as_error * err, PyObject * py_list, as_list ** list);

as_status pyobject_to_key(as_error * err, PyObject * py_key, as_key * key);

as_status pyobject_to_record(as_error * err, PyObject * py_rec, PyObject * py_meta, as_record * rec);

as_status val_to_pyobject(as_error * err, const as_val * val, PyObject ** py_map);

as_status map_to_pyobject(as_error * err, const as_map * map, PyObject ** py_map);

as_status list_to_pyobject(as_error * err, const as_list * list, PyObject ** py_list);

as_status record_to_pyobject(as_error * err, const as_record * rec, const as_key * key, PyObject ** obj);

as_status key_to_pyobject(as_error * err, const as_key * key, PyObject ** obj);

as_status metadata_to_pyobject(as_error * err, const as_record * rec, PyObject ** obj);

as_status bins_to_pyobject(as_error * err, const as_record * rec, PyObject ** obj);

bool error_to_pyobject(const as_error * err, PyObject ** obj);
