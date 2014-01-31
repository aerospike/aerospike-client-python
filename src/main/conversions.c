#include <Python.h>
#include <stdbool.h>

#include <aerospike/as_key.h>
#include <aerospike/as_error.h>
#include <aerospike/as_record.h>

#include <aerospike/as_list.h>
#include <aerospike/as_arraylist.h>
#include <aerospike/as_map.h>
#include <aerospike/as_hashmap.h>
#include <aerospike/as_policy.h>

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
as_status pyobject_to_record(as_error * err, PyObject * py_rec, PyObject * py_meta, as_record * rec)
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

		if ( py_meta && PyDict_Check(py_meta) ) {
			PyObject * py_gen = PyDict_GetItemString(py_meta, "gen");
			PyObject * py_ttl = PyDict_GetItemString(py_meta, "ttl");

			if( py_ttl != NULL ){
				if ( PyInt_Check(py_ttl) ) {
					rec->ttl = (uint32_t) PyInt_AsLong(py_ttl);
				}
				else if ( PyLong_Check(py_ttl) ) {
					rec->ttl = (uint32_t) PyLong_AsLongLong(py_ttl);
				}
			}

			if( py_gen != NULL ){
				if ( PyInt_Check(py_gen) ) {
					rec->gen = (uint16_t) PyInt_AsLong(py_gen);
				}
				else if ( PyLong_Check(py_gen) ) {
					rec->gen = (uint16_t) PyLong_AsLongLong(py_gen);
				}
			}
		}
	

		if ( err->code != AEROSPIKE_OK ) {
			as_record_destroy(rec);
		}
	}

	return err->code;
} // end pyobject_to_record()

// Policy names
#define PY_POLICY_W_TIMEOUT "timeout"    // Number of milliseconds to wait
#define PY_POLICY_W_RETRY   "retry"      // Behavior of failed operations
#define PY_POLICY_W_KEY     "key"        // Behavior of the key
#define PY_POLICY_W_GEN     "gen" 		 // Behavior of the Generation value
#define PY_POLICY_W_EXISTS  "exists"     // Behavior for record existence

/**
 * Converts a PyObject into an as_policy_write object.
 * Returns AEROSPIKE_OK on success. On error, the err argument is populated.
 * We assume that the error object and the policy object are already allocated
 * and initialized (although, we do reset the error object here).
 */
as_status pyobject_to_policy_write(as_error * err_p, PyObject * py_policy,
									as_policy_write * policy_p)
{
	static char * meth = "pyobject_to_policy_write()";
	char * name;
	int64_t value;

	as_error_reset(err_p);

	if (!policy_p) {
		// this should never happen, but if it did...
		return as_error_update(err_p, AEROSPIKE_ERR_CLIENT, "policy obj is null");
	}

	if (!py_policy) {
		return AEROSPIKE_OK; // Not a problem.  Return quietly.
	}

	if ( PyDict_Check( py_policy ) ) {
		PyObject *py_key = NULL, *py_value = NULL;
		Py_ssize_t py_pos = 0;

		// Get the values from the write policy dictionary
		while (PyDict_Next(py_policy, &py_pos, &py_key, &py_value)) {
			if (py_key && PyString_Check(py_key)) {
				name = PyString_AsString(py_key);
			} else {
				as_error_update(err_p, AEROSPIKE_ERR_CLIENT,
						"A policy name must be a string.");
				continue;
			}

			if ( py_value && PyInt_Check(py_value)) {
				value = (int64_t) PyInt_AsLong(py_value);
			} else {
				as_error_update(err_p, AEROSPIKE_ERR_CLIENT,
						"Values must be integer types");
				continue;
			}

			if (strcmp(PY_POLICY_W_TIMEOUT, name) == 0) {
				policy_p->timeout = value;
			} else if (strcmp(PY_POLICY_W_RETRY, name) == 0) {
				policy_p->retry = value;
			} else if (strcmp(PY_POLICY_W_KEY, name) == 0) {
				policy_p->key = value;
			} else if (strcmp(PY_POLICY_W_GEN, name) == 0) {
				policy_p->gen = value;
			} else if (strcmp(PY_POLICY_W_EXISTS, name) == 0) {
				policy_p->exists = value;
			} else {
				printf("[ERROR]<%s> Unknown Policy Field(%s)\n", meth, name);
			}
		} // end while
	} // end if valid dictionary object

	// If there are any errors, then what's a good strategy?  Do we forget
	// all we've seen, or do we return as much as we can?
	if (err_p->code != AEROSPIKE_OK) {
		printf("[ERROR]<%s>: Something goofy happened\n", meth);
	}

	return err_p->code;
} // end pyobject_to_policy_write()


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
		case AS_BYTES: {
			as_bytes * bval = as_bytes_fromval(val);
			uint32_t bval_size = as_bytes_size(bval);
			uint8_t * bval_bytes = malloc(bval_size * sizeof(uint8_t));
			memcpy(bval_bytes, as_bytes_get(bval), bval_size);
			*py_val = PyByteArray_FromStringAndSize((char *) bval_bytes, bval_size);
			break;
		}
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
				record_to_pyobject(err, r, NULL, &py_rec);
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

as_status record_to_pyobject(as_error * err, const as_record * rec, const as_key * key, PyObject ** obj)
{
	as_error_reset(err);

	if ( ! rec ) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT, "record is null");
	}

	PyObject * py_rec = NULL;
	PyObject * py_rec_key = NULL;
	PyObject * py_rec_meta = NULL;
	PyObject * py_rec_bins = NULL;

	key_to_pyobject(err, key ? key : &rec->key, &py_rec_key);
	metadata_to_pyobject(err, rec, &py_rec_meta);
	bins_to_pyobject(err, rec, &py_rec_bins);

	py_rec = PyTuple_New(3);
	PyTuple_SetItem(py_rec, 0, py_rec_key);
	PyTuple_SetItem(py_rec, 1, py_rec_meta);
	PyTuple_SetItem(py_rec, 2, py_rec_bins);

	*obj = py_rec;
	
	return err->code;
}

as_status key_to_pyobject(as_error * err, const as_key * key, PyObject ** obj)
{
	as_error_reset(err);

	if ( ! key ) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT, "key is null");
	}

	PyObject * py_key = PyDict_New();


    if ( key->ns && strlen(key->ns) > 0 ) {
		PyDict_SetItemString(py_key, "ns", PyString_FromString(key->ns));
    }

    if ( key->set && strlen(key->set) > 0 ) {
		PyDict_SetItemString(py_key, "set", PyString_FromString(key->set));
    }

    if ( key->valuep ) {
        as_val * val = (as_val *) key->valuep;
        as_val_t type = as_val_type(val);
        switch(type) {
            case AS_INTEGER: {
				as_integer * ival = as_integer_fromval(val);
				PyObject * py_ival = PyInt_FromLong(as_integer_get(ival));
				PyDict_SetItemString(py_key, "key", py_ival);
				break;
			}
            case AS_STRING: {
				as_string * sval = as_string_fromval(val);
				PyObject * py_sval = PyString_FromString(as_string_get(sval));
				PyDict_SetItemString(py_key, "key", py_sval);
				break;
			}
            case AS_BYTES: {
				as_bytes * bval = as_bytes_fromval(val);
				if ( bval ) {
					uint32_t bval_size = as_bytes_size(bval);
					uint8_t * bval_bytes = malloc(bval_size * sizeof(uint8_t));
					memcpy(bval_bytes, as_bytes_get(bval), bval_size);
					PyObject * py_bval = PyByteArray_FromStringAndSize((char *) bval_bytes, bval_size);
					PyDict_SetItemString(py_key, "key", py_bval);
					break;
				}
			}
            default:
            	break;
        }
    }

    if ( key->digest.init ) {
		uint8_t * digest_bytes = malloc(AS_DIGEST_VALUE_SIZE * sizeof(uint8_t));
		memcpy(digest_bytes, key->digest.value, AS_DIGEST_VALUE_SIZE);
		PyObject * py_digest = PyByteArray_FromStringAndSize((char *) digest_bytes, AS_DIGEST_VALUE_SIZE);
		PyDict_SetItemString(py_key, "digest", py_digest);
    }

	*obj = py_key;
	
	return err->code;
}

static bool bins_to_pyobject_each(const char * name, const as_val * val, void * udata)
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

as_status bins_to_pyobject(as_error * err, const as_record * rec, PyObject ** obj)
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

	as_record_foreach(rec, bins_to_pyobject_each, &convd);

	if ( err->code != AEROSPIKE_OK ) {
		PyObject_Del(py_bins);
		return err->code;
	}

	*obj = py_bins;

	return err->code;
}

as_status metadata_to_pyobject(as_error * err, const as_record * rec, PyObject ** obj)
{
	as_error_reset(err);

	if ( !rec ) {
		// this should never happen, but if it did...
		return as_error_update(err, AEROSPIKE_ERR_CLIENT, "record is null");
	}

	PyObject * py_meta = PyDict_New();
	PyDict_SetItemString(py_meta, "ttl", PyInt_FromLong(rec->ttl));
	PyDict_SetItemString(py_meta, "gen", PyInt_FromLong(rec->gen));

	*obj = py_meta;

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
