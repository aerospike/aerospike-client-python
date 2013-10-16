#include <Python.h>
#include <stdbool.h>

#include <aerospike/as_key.h>
#include <aerospike/as_error.h>
#include <aerospike/as_record.h>

#include <aerospike/as_list.h>
#include <aerospike/as_arraylist.h>
#include <aerospike/as_map.h>
#include <aerospike/as_hashmap.h>

#include "key.h"
#include "conversions.h"


as_status pyobject_to_list(as_error * err, PyObject * py_list, as_list ** list)
{
	as_error_reset(err);

	Py_ssize_t size = PyList_Size(py_list);

	if ( *list == NULL ) {
		*list = (as_list *) as_arraylist_new(size, 0);
	}

	for ( int i = 0; i < size; i++ ) {
		 PyObject * py_val = PyList_GetItem(py_list, i);
		 as_val * val = NULL;
		 pyobject_to_val(err, py_val, &val);
		 if ( err->code != AEROSPIKE_OK ) {
		 	break;
		 }
		 as_list_append(*list, val);
	}

	if ( err->code != AEROSPIKE_OK ) {
		as_list_destroy(*list);
	}

	return err->code;
}

as_status pyobject_to_map(as_error * err, PyObject * py_dict, as_map ** map)
{
	as_error_reset(err);

	PyObject * py_key = NULL;
	PyObject * py_val = NULL;
	Py_ssize_t pos = 0;
	Py_ssize_t size = PyDict_Size(py_dict);

	if ( *map == NULL ) {
		*map = (as_map *) as_hashmap_new(size);
	}

	while (PyDict_Next(py_dict, &pos, &py_key, &py_val)) {
		as_val * key = NULL;
		as_val * val = NULL;
		pyobject_to_val(err, py_key, &key);
		if ( err->code != AEROSPIKE_OK ) {
			break;
		}
		pyobject_to_val(err, py_val, &val);
		if ( err->code != AEROSPIKE_OK ) {
			break;
		}
		as_map_set(*map, key, val);
	}

	if ( err->code != AEROSPIKE_OK ) {
		as_map_destroy(*map);
	}	

	return err->code;
}

as_status pyobject_to_val(as_error * err, PyObject * py_obj, as_val ** val)
{
	as_error_reset(err);

	if ( !py_obj ) {
		// this should never happen, but if it did...
		return as_error_update(err, AEROSPIKE_ERR_CLIENT, "value is null");
	}
	else if ( PyInt_Check(py_obj) ) {
		int64_t i = (int64_t) PyInt_AsLong(py_obj);
		*val = (as_val *) as_integer_new(i);
	}
	else if ( PyLong_Check(py_obj) ) {
		int64_t l = (int64_t) PyLong_AsLongLong(py_obj);
		*val = (as_val *) as_integer_new(l);
	}
	else if ( PyString_Check(py_obj) ) {
		char * s = PyString_AsString(py_obj);
		*val = (as_val *) as_string_new(s, false);
	}
	else if ( PyByteArray_Check(py_obj) ) {
	}
	else if ( PyList_Check(py_obj) ) {
		as_list * list = NULL;
		pyobject_to_list(err, py_obj, &list);
		if ( err->code == AEROSPIKE_OK ) {
			*val = (as_val *) list;
		}
	}
	else if ( PyDict_Check(py_obj) ) {
		as_map * map = NULL;
		pyobject_to_map(err, py_obj, &map);
		if ( err->code == AEROSPIKE_OK ) {
			*val = (as_val *) map;
		}
	}
	else {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT, "value is not a supported type.");
	}

	return err->code;
}

/**
 * Converts a PyObject into an as_record.
 * Returns AEROSPIKE_OK on success. On error, the err argument is populated.
 */
as_status pyobject_to_record(as_error * err, PyObject * py_rec, as_record * rec)
{
	as_error_reset(err);

	if ( !py_rec ) {
		// this should never happen, but if it did...
		return as_error_update(err, AEROSPIKE_ERR_CLIENT, "record is null");
	}
	else if ( PyDict_Check(py_rec) ) {
		PyObject *key = NULL, *value = NULL;
		Py_ssize_t pos = 0;
		Py_ssize_t size = PyDict_Size(py_rec);

		as_record_init(rec, size);

		while (PyDict_Next(py_rec, &pos, &key, &value)) {
			if ( ! PyString_Check(key) ) {
				return as_error_update(err, AEROSPIKE_ERR_CLIENT, "A bin name must be a string.");
			}

			char * name = PyString_AsString(key);

			if ( !value ) {
				// this should never happen, but if it did...
				return as_error_update(err, AEROSPIKE_ERR_CLIENT, "record is null");
			}
			else if ( PyInt_Check(value) ) {
				int64_t val = (int64_t) PyInt_AsLong(value);
				as_record_set_int64(rec, name, val);
			}
			else if ( PyLong_Check(value) ) {
				int64_t val = (int64_t) PyLong_AsLongLong(value);
				as_record_set_int64(rec, name, val);
			}
			else if ( PyString_Check(value) ) {
				as_record_set_strp(rec, name, PyString_AsString(value), false);
			}
			else if ( PyByteArray_Check(value) ) {
			}
			else if ( PyList_Check(value) ) {
				// as_list
				as_list * list = NULL;
				pyobject_to_list(err, value, &list);
				if ( err->code != AEROSPIKE_OK ) {
					break;
				}
				as_record_set_list(rec, name, list);
			}
			else if ( PyDict_Check(value) ) {
				// as_map
				as_map * map = NULL;
				pyobject_to_map(err, value, &map);
				if ( err->code != AEROSPIKE_OK ) {
					break;
				}
				as_record_set_map(rec, name, map);
			}
			else {
			}
		}

		if ( err->code != AEROSPIKE_OK ) {
			as_record_destroy(rec);
		}
	}

	return err->code;
}

typedef struct {
	as_error * err;
	uint32_t count;
	void * udata;
} conversion_data;


as_status val_to_pyobject(as_error * err, const as_val * val, PyObject ** py_val)
{
	as_error_reset(err);

	switch( as_val_type(val) ) {
		case AS_INTEGER: {
			as_integer * i = as_integer_fromval(val);
			*py_val = PyInt_FromLong(i->value);
			break;
		}
		case AS_STRING: {
			as_string * s = as_string_fromval(val);
			*py_val = PyString_FromString(s->value);
			break;
		}
		// case AS_BYTES: {
		// 	break;
		// }
		case AS_LIST: {
			as_list * l = as_list_fromval((as_val *) val);
			if ( l != NULL ) {
				PyObject * py_list = NULL;
				list_to_pyobject(err, l, &py_list);
				if ( err->code == AEROSPIKE_OK ) {
					*py_val = py_list;
				}
			}
			break;
		}
		case AS_MAP: {
			as_map * m = as_map_fromval(val);
			if ( m != NULL ) {
				PyObject * py_map = NULL;
				map_to_pyobject(err, m, &py_map);
				if ( err->code == AEROSPIKE_OK ) {
					*py_val = py_map;
				}
			}
			break;
		}
		case AS_REC: {
			as_record * r = as_record_fromval(val);
			if ( r != NULL ) {
				PyObject * py_rec = NULL;
				record_to_pyobject(err, r, &py_rec);
				if ( err->code == AEROSPIKE_OK ) {
					*py_val = py_rec;
				}
			}
			break;
		}
		default: {
			as_error_update(err, AEROSPIKE_ERR_CLIENT, "Unknown type for value");
			return false;
		}
	}

	return err->code;
}

static bool list_to_pyobject_each(as_val * val, void * udata)
{
	if ( val == NULL ) {
		return false;
	}

	conversion_data * convd = (conversion_data *) udata;
	as_error * err = convd->err;
	PyObject * py_list = (PyObject *) convd->udata;

	PyObject * py_val = NULL;
	val_to_pyobject(convd->err, val, &py_val);

	if ( err->code != AEROSPIKE_OK ) {
		return false;
	}

	PyList_SetItem(py_list, convd->count, py_val);

	convd->count++;
	return true;
}

as_status list_to_pyobject(as_error * err, const as_list * list, PyObject ** py_list)
{
	*py_list = PyList_New(as_list_size((as_list *) list));

	conversion_data convd = {
		.err = err,
		.count = 0,
		.udata = *py_list
	};

	as_list_foreach(list, list_to_pyobject_each, &convd);

	if ( err->code != AEROSPIKE_OK ) {
		PyObject_Del(*py_list);
		return err->code;
	}

	return err->code;
}

static bool map_to_pyobject_each(const as_val * key, const as_val * val, void * udata)
{
	if ( key == NULL || val == NULL ) {
		return false;
	}

	conversion_data * convd = (conversion_data *) udata;
	as_error * err = convd->err;
	PyObject * py_dict = (PyObject *) convd->udata;

	PyObject * py_key = NULL;
	val_to_pyobject(convd->err, key, &py_key);

	if ( err->code != AEROSPIKE_OK ) {
		return false;
	}

	PyObject * py_val = NULL;
	val_to_pyobject(convd->err, val, &py_val);

	if ( err->code != AEROSPIKE_OK ) {
		PyObject_Del(py_key);
		return false;
	}

	PyDict_SetItem(py_dict, py_key, py_val);

	convd->count++;
	return true;
}

as_status map_to_pyobject(as_error * err, const as_map * map, PyObject ** py_map)
{
	*py_map = PyDict_New();

	conversion_data convd = {
		.err = err,
		.count = 0,
		.udata = *py_map
	};

	as_map_foreach(map, map_to_pyobject_each, &convd);

	if ( err->code != AEROSPIKE_OK ) {
		PyObject_Del(*py_map);
		return err->code;
	}

	return err->code;
}

static bool record_to_pyobject_each(const char * name, const as_val * val, void * udata)
{
	if ( name == NULL || val == NULL ) {
		return false;
	}

	conversion_data * convd = (conversion_data *) udata;
	as_error * err = convd->err;
	PyObject * py_bins = (PyObject *) convd->udata;
	PyObject * py_val = NULL;

	val_to_pyobject(err, val, &py_val);

	if ( err->code != AEROSPIKE_OK ) {
		return false;
	}

	PyDict_SetItemString(py_bins, name, py_val);

	convd->count++;
	return true;
}

as_status record_to_pyobject(as_error * err, const as_record * rec, PyObject ** obj)
{
	as_error_reset(err);

	if ( !rec ) {
		// this should never happen, but if it did...
		return as_error_update(err, AEROSPIKE_ERR_CLIENT, "record is null");
	}

	PyObject * py_bins = PyDict_New();

	conversion_data convd = {
		.err = err,
		.count = 0,
		.udata = py_bins
	};

	as_record_foreach(rec, record_to_pyobject_each, &convd);

	if ( err->code != AEROSPIKE_OK ) {
		PyObject_Del(py_bins);
		return err->code;
	}

	PyObject * py_rec = PyDict_New();
	PyDict_SetItemString(py_rec, "ttl", PyInt_FromLong(rec->ttl));
	PyDict_SetItemString(py_rec, "gen", PyInt_FromLong(rec->gen));
	PyDict_SetItemString(py_rec, "bins", py_bins);

	*obj = py_rec;

	return err->code;
}

bool error_to_pyobject(const as_error * err, PyObject ** obj)
{
	PyObject * py_file = NULL;
	if ( err->file ) {
		py_file = PyString_FromString(err->file);
	}
	else {
		Py_INCREF(Py_None);
		py_file = Py_None;
	}
	PyObject * py_line = NULL;
	if ( err->line > 0 ) {
		py_line = PyInt_FromLong(err->line);
	}
	else {
		Py_INCREF(Py_None);
		py_line = Py_None;
	}

	PyObject * py_err = PyDict_New();
	PyDict_SetItemString(py_err, "file", py_file);
	PyDict_SetItemString(py_err, "line", py_line);
	PyDict_SetItemString(py_err, "code", PyLong_FromLongLong(err->code));
	PyDict_SetItemString(py_err, "message", PyString_FromString(err->message));

	*obj = py_err;
	return true;
}
