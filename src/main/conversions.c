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

#include <Python.h>
#include <stdbool.h>

#include <aerospike/as_admin.h>
#include <aerospike/as_error.h>
#include <aerospike/as_key.h>
#include <aerospike/as_record.h>

#include <aerospike/as_arraylist.h>
#include <aerospike/as_hashmap.h>
#include <aerospike/as_list.h>
#include <aerospike/as_map.h>
#include <aerospike/as_nil.h>
#include <aerospike/as_policy.h>

#include "conversions.h"
#include "key.h"

#define PY_KEYT_NAMESPACE 0
#define PY_KEYT_SET 1
#define PY_KEYT_KEY 2
#define PY_KEYT_DIGEST 3

#define PY_EXCEPTION_CODE 0
#define PY_EXCEPTION_MSG 1
#define PY_EXCEPTION_FILE 2
#define PY_EXCEPTION_LINE 3

as_status as_udf_file_to_pyobject( as_error *err, as_udf_file * entry, PyObject ** py_file )
{
	as_error_reset(err);

	*py_file = PyDict_New();

	PyObject * py_name = PyString_FromString(entry->name);
	PyDict_SetItemString(*py_file, "name", py_name);
	Py_DECREF(py_name);

	PyObject * py_hash = PyByteArray_FromStringAndSize((char *) entry->hash, AS_UDF_FILE_HASH_SIZE);
	PyDict_SetItemString(*py_file, "hash", py_hash);
	Py_DECREF(py_hash);

	PyObject * py_type = PyInt_FromLong(entry->type);
	PyDict_SetItemString(*py_file, "type", py_type);
	Py_DECREF(py_type);

	PyObject * py_content = PyByteArray_FromStringAndSize((char *) entry->content.bytes, entry->content.size);
	PyDict_SetItemString(*py_file, "content", py_content);
	Py_DECREF(py_content);

	return err->code;
}

as_status as_udf_files_to_pyobject( as_error *err, as_udf_files *files, PyObject **py_files )
{
	as_error_reset(err);

	*py_files = PyList_New(0);

	for(int i = 0; i < files->size; i++) {

		PyObject * py_file;
		as_udf_file_to_pyobject( err, &((files->entries)[i]), &py_file );
		if( err->code != AEROSPIKE_OK) {
			goto END;
		}

		PyList_Append(*py_files, py_file);
		Py_DECREF(py_file);
	}

END:
	return err->code;
}


as_status strArray_to_pyobject( as_error * err, char str_array_ptr[][AS_ROLE_SIZE], PyObject **py_list, int roles_size )
{
	as_error_reset(err);
	int i;
	char *role;

	*py_list = PyList_New(0);

	for(i = 0; i < roles_size; i++) {
		role = str_array_ptr[i];
		PyObject *py_str = Py_BuildValue("s", role);
		PyList_Append(*py_list, py_str);

		Py_DECREF(py_str);
	}

	return err->code;
}

as_status as_user_roles_array_to_pyobject( as_error *err, as_user_roles **user_roles, PyObject **py_as_user_roles, int users )
{
	as_error_reset(err);
	int i;
	*py_as_user_roles = PyList_New(0);

	for(i = 0; i < users; i++) {

		PyObject * py_user = PyString_FromString(user_roles[i]->user);
		PyObject * py_roles_size = PyInt_FromLong(user_roles[i]->roles_size);
		PyObject * py_roles;
		strArray_to_pyobject(err, user_roles[i]->roles, &py_roles, user_roles[i]->roles_size);
		if( err->code != AEROSPIKE_OK) {
			break;
		}

		PyObject * py_user_roles = PyDict_New();
		PyDict_SetItemString(py_user_roles, "user", py_user);
		PyDict_SetItemString(py_user_roles, "roles_size", py_roles_size);
		PyDict_SetItemString(py_user_roles, "roles", py_roles);

		Py_DECREF(py_user);
		Py_DECREF(py_roles_size);
		Py_DECREF(py_roles);

		PyList_Append(*py_as_user_roles, py_user_roles);

		Py_DECREF(py_user_roles);
	}

	return err->code;
}


as_status as_user_roles_to_pyobject( as_error * err, as_user_roles * user_roles, PyObject ** py_as_user_roles )
{
	as_error_reset(err);

	PyObject * py_user = PyString_FromString(user_roles->user);
	PyObject * py_roles_size = PyInt_FromLong(user_roles->roles_size);
	PyObject * py_roles;

	strArray_to_pyobject(err, user_roles->roles, &py_roles, user_roles->roles_size);
	if( err->code != AEROSPIKE_OK) {
		goto END;
	}

	PyObject * py_user_roles = PyDict_New();

	PyDict_SetItemString(py_user_roles, "user", py_user);
	PyDict_SetItemString(py_user_roles, "roles_size", py_roles_size);
	PyDict_SetItemString(py_user_roles, "roles", py_roles);

	Py_DECREF(py_user);
	Py_DECREF(py_roles_size);
	Py_DECREF(py_roles);

	*py_as_user_roles = PyList_New(0);
	PyList_Append(*py_as_user_roles, py_user_roles);

	Py_DECREF(py_user_roles);

END:
	return err->code;
}

as_status pyobject_to_strArray( as_error * err, PyObject * py_list,  char ** arr )
{
	as_error_reset(err);

	Py_ssize_t size = PyList_Size(py_list);
	if(!PyList_Check(py_list)) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT, "not a list");
	}

	char *s;
	for ( int i = 0; i < size; i++ ) {
		PyObject * py_val = PyList_GetItem(py_list, i);
		if( PyString_Check(py_val) ) {
			s = PyString_AsString(py_val);
			strcpy(arr[i], s);
		}
	}

	return err->code;
}

as_status pyobject_to_list(as_error * err, PyObject * py_list, as_list ** list)
{
	as_error_reset(err);

	Py_ssize_t size = PyList_Size(py_list);

	if ( *list == NULL ) {
		*list = (as_list *) as_arraylist_new((uint32_t) size, 0);
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
		*map = (as_map *) as_hashmap_new((uint32_t) size);
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
			if (key) {
				as_val_destroy(key);
			}
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
	else if ( py_obj == Py_None ) {
		*val = (as_val *) &as_nil;
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
	else if ( PyUnicode_Check(py_obj) ) {
		PyObject * py_ustr = PyUnicode_AsUTF8String(py_obj);
		char * str = PyString_AsString(py_ustr);
		*val = (as_val *) as_string_new(strdup(str), true);
		Py_DECREF(py_ustr);
	}
	else if ( PyByteArray_Check(py_obj) ) {
		uint8_t * b = (uint8_t *) PyByteArray_AsString(py_obj);
		uint32_t z = (uint32_t) PyByteArray_Size(py_obj);
		*val = (as_val *) as_bytes_new_wrap(b, z, false);
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
			else if ( value == Py_None ) {
				as_record_set_nil(rec, name);
			}
			else if ( PyInt_Check(value) ) {
				int64_t val = (int64_t) PyInt_AsLong(value);
				as_record_set_int64(rec, name, val);
			}
			else if ( PyLong_Check(value) ) {
				int64_t val = (int64_t) PyLong_AsLongLong(value);
				as_record_set_int64(rec, name, val);
			}
			else if ( PyUnicode_Check(value) ) {
				PyObject * py_ustr = PyUnicode_AsUTF8String(value);
				char * val = PyString_AsString(py_ustr);
				as_record_set_strp(rec, name, strdup(val), true);
				Py_DECREF(py_ustr);
			}
			else if ( PyString_Check(value) ) {
				char * val = PyString_AsString(value);
				as_record_set_strp(rec, name, val, false);
			}
			else if ( PyByteArray_Check(value) ) {
				uint8_t * val = (uint8_t *) PyByteArray_AsString(value);
				uint32_t sz = (uint32_t) PyByteArray_Size(value);
				as_record_set_rawp(rec, name, val, sz, false);
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
				} else {
					as_error_update(err, AEROSPIKE_ERR_PARAM, "Ttl should be an int or long");
				}
			}

			if( py_gen != NULL ){
				if ( PyInt_Check(py_gen) ) {
					rec->gen = (uint16_t) PyInt_AsLong(py_gen);
				}
				else if ( PyLong_Check(py_gen) ) {
					rec->gen = (uint16_t) PyLong_AsLongLong(py_gen);
				} else {
					as_error_update(err, AEROSPIKE_ERR_PARAM, "Generation should be an int or long");
				}
			}
		}


		if ( err->code != AEROSPIKE_OK ) {
			as_record_destroy(rec);
		}
	}

	return err->code;
}

as_status pyobject_to_key(as_error * err, PyObject * py_keytuple, as_key * key) 
{
	as_error_reset(err);

	Py_ssize_t size = 0;
	PyObject * py_ns = NULL;
	PyObject * py_set = NULL;
	PyObject * py_key = NULL;
	PyObject * py_digest = NULL;

	char * ns = NULL;
	char * set = NULL;

	if ( !py_keytuple ) {
		// this should never happen, but if it did...
		return as_error_update(err, AEROSPIKE_ERR_PARAM, "key is null");
	}
	else if ( PyTuple_Check(py_keytuple) ) {
		size = PyTuple_Size(py_keytuple);

		if ( size < 3 || size > 4 ) {
			return as_error_update(err, AEROSPIKE_ERR_PARAM, "key tuple must be (Namespace, Set, Key) or (Namespace, Set, None, Digest)");
		}

		py_ns = PyTuple_GetItem(py_keytuple, 0);
		py_set = PyTuple_GetItem(py_keytuple, 1);
		py_key = PyTuple_GetItem(py_keytuple, 2);

		if ( size == 4 ) {
			py_digest = PyTuple_GetItem(py_keytuple, 3);
		}
	}
	else if ( PyDict_Check(py_keytuple) ) {
		py_ns = PyDict_GetItemString(py_keytuple, "ns");
		py_set = PyDict_GetItemString(py_keytuple, "set");
		py_key = PyDict_GetItemString(py_keytuple, "key");
		py_digest = PyDict_GetItemString(py_keytuple, "digest");
	}
	else {
		return as_error_update(err, AEROSPIKE_ERR_PARAM, "key is invalid");
	}


	if ( ! py_ns ) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM, "namespace is required");
	}
	else if ( ! PyString_Check(py_ns) ) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM, "namespace must be a string");
	}
	else {
		ns = PyString_AsString(py_ns);
	}

	if ( py_set && py_set != Py_None ) {
		if ( PyString_Check(py_set) ) {
			set = PyString_AsString(py_set);
		}
		else if ( PyUnicode_Check(py_set) ) {
			PyObject * py_ustr = PyUnicode_AsUTF8String(py_set);
			set = PyString_AsString(py_ustr);
			Py_DECREF(py_ustr);
		}
		else {
			return as_error_update(err, AEROSPIKE_ERR_PARAM, "set must be a string");
		}
	}

	if ( py_key && py_key != Py_None ) {
		if ( PyString_Check(py_key) ) {
			char * k = PyString_AsString(py_key);
			as_key_init_strp(key, ns, set, k, true);
		}
		else if ( PyInt_Check(py_key) ) {
			int64_t k = (int64_t) PyInt_AsLong(py_key);
			as_key_init_int64(key, ns, set, k);
		}
		else if ( PyLong_Check(py_key) ) {
			int64_t k = (int64_t) PyLong_AsLongLong(py_key);
			as_key_init_int64(key, ns, set, k);
		}
		else if ( PyUnicode_Check(py_key) ) {
			PyObject * py_ustr = PyUnicode_AsUTF8String(py_key);
			char * k = PyString_AsString(py_ustr);
			as_key_init_strp(key, ns, set, strdup(k), true);
			Py_DECREF(py_ustr);
		}
		else if ( PyByteArray_Check(py_key) ) {
			return as_error_update(err, AEROSPIKE_ERR_PARAM, "key as a byte array is not supported");
		}
		else {
			return as_error_update(err, AEROSPIKE_ERR_PARAM, "key is invalid");
		}
	}
	else if ( py_digest && py_digest != Py_None ) {
		if ( PyByteArray_Check(py_digest) ) {
			uint32_t sz = (uint32_t) PyByteArray_Size(py_digest);

			if ( sz != AS_DIGEST_VALUE_SIZE ) {
				return as_error_update(err, AEROSPIKE_ERR_PARAM, "digest size is invalid. should be 20 bytes, but received %d", sz);
			}

			uint8_t * digest = (uint8_t *) PyByteArray_AsString(py_digest);
			as_key_init_digest(key, ns, set, digest);
		}
		else {
			return as_error_update(err, AEROSPIKE_ERR_PARAM, "digest is invalid. expected a bytearray");
		}
	}
	else {
		return as_error_update(err, AEROSPIKE_ERR_PARAM, "either key or digest is required");
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
				*py_val = PyInt_FromLong((long) as_integer_get(i));
				break;
			}
		case AS_STRING: {
				as_string * s = as_string_fromval(val);
				char * str = as_string_get(s);
				if ( str != NULL ) {
					size_t sz = strlen(str);
					*py_val = PyUnicode_DecodeUTF8(str, sz, NULL);
				}
				else {
					Py_INCREF(Py_None);
					*py_val = Py_None;
				}
				break;
			}
		case AS_BYTES: {
				as_bytes * bval = as_bytes_fromval(val);
				uint32_t bval_size = as_bytes_size(bval);
				*py_val = PyByteArray_FromStringAndSize((char *) as_bytes_get(bval), bval_size);
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
		case AS_NIL: {
				Py_INCREF(Py_None);
				*py_val = Py_None;
				break;
			}
		default: {
				as_error_update(err, AEROSPIKE_ERR_CLIENT, "Unknown type for value");
				return err->code;
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
		Py_DECREF(*py_list);
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

	Py_DECREF(py_key);
	Py_DECREF(py_val);

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

	if ( py_rec_key == NULL ) {
		Py_INCREF(Py_None);
		py_rec_key = Py_None;
	}

	if ( py_rec_meta == NULL ) {
		Py_INCREF(Py_None);
		py_rec_meta = Py_None;
	}

	if ( py_rec_bins == NULL ) {
		Py_INCREF(Py_None);
		py_rec_bins = Py_None;
	}

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

	*obj = NULL;

	if ( ! key ) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT, "key is null");
	}

	PyObject * py_namespace = NULL;
	PyObject * py_set = NULL;
	PyObject * py_key = NULL;
	PyObject * py_digest = NULL;

	if ( key->ns && strlen(key->ns) > 0 ) {
		py_namespace = PyString_FromString(key->ns);
	}

	if ( key->set && strlen(key->set) > 0 ) {
		py_set = PyString_FromString(key->set);
	}

	if ( key->valuep ) {
		as_val * val = (as_val *) key->valuep;
		as_val_t type = as_val_type(val);
		switch(type) {
			case AS_INTEGER: {
				as_integer * ival = as_integer_fromval(val);
				py_key = PyInt_FromLong((long) as_integer_get(ival));
				break;
			}
			case AS_STRING: {
				as_string * sval = as_string_fromval(val);
				py_key = PyUnicode_DecodeUTF8(as_string_get(sval), as_string_len(sval), NULL);
				break;
			}
			case AS_BYTES: {
				as_bytes * bval = as_bytes_fromval(val);
				if ( bval ) {
					uint32_t bval_size = as_bytes_size(bval);
					py_key = PyByteArray_FromStringAndSize((char *) as_bytes_get(bval), bval_size);
				}
				break;
			}
			default: {
				break;
			}
		}
	}

	if ( key->digest.init ) {
		py_digest = PyByteArray_FromStringAndSize((char *) key->digest.value, AS_DIGEST_VALUE_SIZE);
	}


	if ( py_namespace == NULL ) {
		Py_INCREF(Py_None);
		py_namespace = Py_None;
	}

	if ( py_set == NULL ) {
		Py_INCREF(Py_None);
		py_set = Py_None;
	}

	if ( py_key == NULL ) {
		Py_INCREF(Py_None);
		py_key = Py_None;
	}


	if ( py_digest == NULL ) {
		Py_INCREF(Py_None);
		py_digest = Py_None;
	}

	PyObject * py_keyobj = PyTuple_New(4);
	PyTuple_SetItem(py_keyobj, PY_KEYT_NAMESPACE, py_namespace);
	PyTuple_SetItem(py_keyobj, PY_KEYT_SET, py_set);
	PyTuple_SetItem(py_keyobj, PY_KEYT_KEY, py_key);
	PyTuple_SetItem(py_keyobj, PY_KEYT_DIGEST, py_digest);

	*obj = py_keyobj;
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

	Py_DECREF(py_val);

	convd->count++;
	return true;
}

as_status bins_to_pyobject(as_error * err, const as_record * rec, PyObject ** py_bins)
{
	as_error_reset(err);

	if ( !rec ) {
		// this should never happen, but if it did...
		return as_error_update(err, AEROSPIKE_ERR_CLIENT, "record is null");
	}

	*py_bins = PyDict_New();

	conversion_data convd = {
		.err = err,
		.count = 0,
		.udata = *py_bins
	};

	as_record_foreach(rec, bins_to_pyobject_each, &convd);

	if ( err->code != AEROSPIKE_OK ) {
		Py_DECREF(*py_bins);
		return err->code;
	}

	return err->code;
}

as_status metadata_to_pyobject(as_error * err, const as_record * rec, PyObject ** obj)
{
	as_error_reset(err);

	if ( !rec ) {
		// this should never happen, but if it did...
		return as_error_update(err, AEROSPIKE_ERR_CLIENT, "record is null");
	}

	PyObject * py_ttl = PyInt_FromLong(rec->ttl);
	PyObject * py_gen = PyInt_FromLong(rec->gen);

	PyObject * py_meta = PyDict_New();
	PyDict_SetItemString(py_meta, "ttl", py_ttl);
	PyDict_SetItemString(py_meta, "gen", py_gen);

	Py_DECREF(py_ttl);
	Py_DECREF(py_gen);

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

	PyObject * py_code = PyLong_FromLongLong(err->code);
	PyObject * py_message = PyString_FromString(err->message);

	PyObject * py_err = PyTuple_New(4);
	PyTuple_SetItem(py_err, PY_EXCEPTION_CODE, py_code);
	PyTuple_SetItem(py_err, PY_EXCEPTION_MSG, py_message);
	PyTuple_SetItem(py_err, PY_EXCEPTION_FILE, py_file);
	PyTuple_SetItem(py_err, PY_EXCEPTION_LINE, py_line);
	*obj = py_err;
	return true;
}
