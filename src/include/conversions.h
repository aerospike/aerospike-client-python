#pragma once

#include <Python.h>
#include <stdbool.h>

#include <aerospike/as_key.h>
#include <aerospike/as_error.h>
#include <aerospike/as_record.h>

#include "key.h"

as_status pykey_to_key(as_error * err, AerospikeKey * py_key, as_key * key);


as_status pyobject_to_val(as_error * err, PyObject * py_obj, as_val ** val);

as_status pyobject_to_map(as_error * err, PyObject * py_dict, as_map ** map);

as_status pyobject_to_list(as_error * err, PyObject * py_list, as_list ** list);

as_status pyobject_to_key(as_error * err, PyObject * py_key, as_key * key);

as_status pyobject_to_record(as_error * err, PyObject * py_rec, as_record * rec);



as_status val_to_pyobject(as_error * err, const as_val * val, PyObject ** py_map);

as_status map_to_pyobject(as_error * err, const as_map * map, PyObject ** py_map);

as_status list_to_pyobject(as_error * err, const as_list * list, PyObject ** py_list);

as_status record_to_pyobject(as_error * err, const as_record * rec, PyObject ** obj);

bool error_to_pyobject(const as_error * err, PyObject ** obj);