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
#include <stdbool.h>

#include <aerospike/as_address.h>
#include <aerospike/as_admin.h>
#include <aerospike/as_error.h>
#include <aerospike/as_key.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/as_record.h>
#include <aerospike/as_geojson.h>

#include <aerospike/as_arraylist.h>
#include <aerospike/as_hashmap.h>
#include <aerospike/as_list.h>
#include <aerospike/as_map.h>
#include <aerospike/as_nil.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_operations.h>
#include <aerospike/as_boolean.h>
#include <aerospike/as_bytes.h>
#include <aerospike/as_double.h>
#include <aerospike/as_record_iterator.h>
#include <aerospike/as_msgpack_ext.h>

#include "conversions.h"
#include "geo.h"
#include "policy.h"
#include "serializer.h"
#include "exceptions.h"
#include "cdt_types.h"
#include "cdt_operation_utils.h"
#include "key_ordered_dict.h"

#define PY_KEYT_NAMESPACE 0
#define PY_KEYT_SET 1
#define PY_KEYT_KEY 2
#define PY_KEYT_DIGEST 3

#define PY_EXCEPTION_CODE 0
#define PY_EXCEPTION_MSG 1
#define PY_EXCEPTION_FILE 2
#define PY_EXCEPTION_LINE 3
#define AS_PY_EXCEPTION_IN_DOUBT 4

#define CTX_KEY "ctx"
#define CDT_CTX_ORDER_KEY "order_key"
#define CDT_CTX_PAD_KEY "pad_key"

static bool requires_int(uint64_t op);

static as_status py_bool_to_py_bytes_blob(AerospikeClient *self, as_error *err,
										  as_static_pool *static_pool,
										  PyObject *py_bool, as_bytes **target,
										  int serializer_type);
static as_status py_bool_to_as_integer(as_error *err, PyObject *py_bool,
									   as_integer **target);
static as_status py_bool_to_as_bool(as_error *err, PyObject *py_bool,
									as_boolean **target);

as_status as_udf_file_to_pyobject(as_error *err, as_udf_file *entry,
								  PyObject **py_file)
{
	as_error_reset(err);

	*py_file = PyDict_New();

	PyObject *py_name = PyString_FromString(entry->name);
	PyDict_SetItemString(*py_file, "name", py_name);
	Py_DECREF(py_name);

	PyObject *py_hash = PyByteArray_FromStringAndSize((char *)entry->hash,
													  AS_UDF_FILE_HASH_SIZE);
	PyDict_SetItemString(*py_file, "hash", py_hash);
	Py_DECREF(py_hash);

	PyObject *py_type = PyInt_FromLong(entry->type);
	PyDict_SetItemString(*py_file, "type", py_type);
	Py_DECREF(py_type);

	PyObject *py_content = PyByteArray_FromStringAndSize(
		(char *)entry->content.bytes, entry->content.size);
	PyDict_SetItemString(*py_file, "content", py_content);
	Py_DECREF(py_content);

	return err->code;
}

as_status as_udf_files_to_pyobject(as_error *err, as_udf_files *files,
								   PyObject **py_files)
{
	as_error_reset(err);

	*py_files = PyList_New(0);

	for (uint32_t i = 0; i < files->size; i++) {

		PyObject *py_file;
		as_udf_file_to_pyobject(err, &((files->entries)[i]), &py_file);
		if (err->code != AEROSPIKE_OK) {
			goto END;
		}

		PyList_Append(*py_files, py_file);
		Py_DECREF(py_file);
	}

END:
	return err->code;
}

as_status char_double_ptr_to_py_list(as_error *err, int num_elements,
									 int element_size, char **str_array_ptr,
									 PyObject *py_list)
{
	as_error_reset(err);

	char *str;

	for (int i = 0; i < num_elements; i++) {
		str = str_array_ptr[i];
		PyObject *py_str = Py_BuildValue("s", str);
		if (py_str == NULL) {
			as_error_update(err, AEROSPIKE_ERR_CLIENT,
							"Unable to build string value from %s.", str);
			break;
		}

		PyList_Append(py_list, py_str);
		Py_DECREF(py_str);
	}

	return err->code;
}

as_status strArray_to_py_list(as_error *err, int num_elements, int element_size,
							  char str_array_ptr[][element_size],
							  PyObject *py_list)
{
	as_error_reset(err);

	char *str;

	for (int i = 0; i < num_elements; i++) {
		str = str_array_ptr[i];
		PyObject *py_str = Py_BuildValue("s", str);
		if (py_str == NULL) {
			as_error_update(err, AEROSPIKE_ERR_CLIENT,
							"Unable to build string value from %s.", str);
			break;
		}

		PyList_Append(py_list, py_str);
		Py_DECREF(py_str);
	}

	return err->code;
}

as_status as_user_array_to_pyobject(as_error *err, as_user **users,
									PyObject **py_as_users, int users_size)
{
	as_error_reset(err);
	int i;

	PyObject *py_users = PyDict_New();
	for (i = 0; i < users_size; i++) {

		PyObject *py_user = PyString_FromString(users[i]->name);
		PyObject *py_roles = PyList_New(0);
		strArray_to_py_list(err, users[i]->roles_size, AS_ROLE_SIZE,
							users[i]->roles, py_roles);
		if (err->code != AEROSPIKE_OK) {
			break;
		}

		PyDict_SetItem(py_users, py_user, py_roles);

		Py_DECREF(py_user);
		Py_DECREF(py_roles);
	}
	*py_as_users = py_users;

	return err->code;
}

as_status as_user_info_array_to_pyobject(as_error *err, as_user **users,
										 PyObject **py_as_users, int users_size)
{
	as_error_reset(err);
	int i;

	PyObject *py_users = PyDict_New();
	for (i = 0; i < users_size; i++) {

		PyObject *py_as_user = NULL;

		as_user_info_to_pyobject(err, users[i], &py_as_user);
		if (err->code != AEROSPIKE_OK) {
			break;
		}
		if (PyDict_SetItemString(py_users, users[i]->name, py_as_user) == -1) {
			Py_DECREF(py_as_user);
			as_error_update(err, AEROSPIKE_ERR_CLIENT,
							"Failed to set user info in users.");
			break;
		}
		Py_DECREF(py_as_user);
	}

	if (err->code != AEROSPIKE_OK) {
		Py_DECREF(py_users);
		py_users = NULL;
	}

	*py_as_users = py_users;

	return err->code;
}

/**
 *******************************************************************************************************
 * Convert a PyObject list of privilege dicts to an array of as_privilege.
 *
 * @param err                   (as_error) Updated to indicate result status, is AERROSPIKE_OK on success.
 * @param py_privileges         (PyObject*) Pointer to list of privilege dicts.
 * @param privileges         	(as_privilege**) Array of pointers to as_privilege structs.
 * @param privileges_size       (int) Number of privilege dicts in py_privileges.
 *
 * Returns an as_status. AERROSPIKE_OK(0) is success value.
 * In case of error, appropriate as_status will be returned.
 *******************************************************************************************************
 */
as_status pyobject_to_as_privileges(as_error *err, PyObject *py_privileges,
									as_privilege **privileges,
									int privileges_size)
{
	as_error_reset(err);
	for (int i = 0; i < privileges_size; i++) {
		PyObject *py_val = PyList_GetItem(py_privileges, i);
		if (PyDict_Check(py_val)) {
			PyObject *py_dict_key = PyString_FromString("code");
			if (PyDict_Contains(py_val, py_dict_key)) {
				PyObject *py_code = NULL;
				py_code = PyDict_GetItemString(py_val, "code");
				privileges[i]->code = PyInt_AsLong(py_code);
			}
			else {
				as_error_update(
					err, AEROSPIKE_ERR_PARAM,
					"Code is a compulsory parameter in privileges dictionary");
				break;
			}
			Py_DECREF(py_dict_key);
			py_dict_key = PyString_FromString("ns");
			if (PyDict_Contains(py_val, py_dict_key)) {
				PyObject *py_ns = PyDict_GetItemString(py_val, "ns");
				strcpy(privileges[i]->ns, PyString_AsString(py_ns));
			}
			else {
				strcpy(privileges[i]->ns, "");
			}
			Py_DECREF(py_dict_key);
			py_dict_key = PyString_FromString("set");
			if (PyDict_Contains(py_val, py_dict_key)) {
				PyObject *py_set = PyDict_GetItemString(py_val, "set");
				strcpy(privileges[i]->set, PyString_AsString(py_set));
			}
			else {
				strcpy(privileges[i]->set, "");
			}
			Py_DECREF(py_dict_key);
		}
	}
	return err->code;
}

as_status as_role_array_to_pyobject_old(as_error *err, as_role **roles,
										PyObject **py_as_roles, int roles_size)
{
	as_error_reset(err);
	int i;

	PyObject *py_roles = PyDict_New();
	for (i = 0; i < roles_size; i++) {

		PyObject *py_role = PyString_FromString(roles[i]->name);
		PyObject *py_privileges = PyList_New(0);

		as_privilege_to_pyobject(err, roles[i]->privileges, py_privileges,
								 roles[i]->privileges_size);
		if (err->code != AEROSPIKE_OK) {
			Py_DECREF(py_role);
			Py_DECREF(py_privileges);
			break;
		}

		PyDict_SetItem(py_roles, py_role, py_privileges);

		Py_DECREF(py_role);
		Py_DECREF(py_privileges);
	}
	*py_as_roles = py_roles;

	return err->code;
}

as_status as_role_array_to_pyobject(as_error *err, as_role **roles,
									PyObject **py_as_roles, int roles_size)
{
	as_error_reset(err);
	int i;

	PyObject *py_roles = PyDict_New();
	for (i = 0; i < roles_size; i++) {

		const char *py_role_name = roles[i]->name;
		PyObject *py_role = PyDict_New();

		as_role_to_pyobject(err, roles[i], py_role);
		if (err->code != AEROSPIKE_OK) {
			break;
		}

		PyDict_SetItemString(py_roles, py_role_name, py_role);

		Py_DECREF(py_role);
	}
	*py_as_roles = py_roles;

	return err->code;
}

as_status as_user_to_pyobject(as_error *err, as_user *user,
							  PyObject **py_as_user)
{
	as_error_reset(err);

	PyObject *py_roles = PyList_New(0);

	strArray_to_py_list(err, user->roles_size, AS_ROLE_SIZE, user->roles,
						py_roles);
	if (err->code != AEROSPIKE_OK) {
		goto END;
	}

	*py_as_user = py_roles;

END:
	return err->code;
}

as_status as_user_info_to_pyobject(as_error *err, as_user *user,
								   PyObject **py_as_user)
{
	as_error_reset(err);

	PyObject *py_info = PyDict_New();
	PyObject *py_roles = PyList_New(0);

	strArray_to_py_list(err, user->roles_size, AS_ROLE_SIZE, user->roles,
						py_roles);
	if (err->code != AEROSPIKE_OK) {
		Py_DECREF(py_roles);
		Py_DECREF(py_info);
		goto END;
	}

	if (PyDict_SetItemString(
			py_info, "read_info",
			Py_BuildValue("i", (user->read_info ? *(user->read_info): 0))) == -1) {
		as_error_update(err, AEROSPIKE_ERR_CLIENT,
						"Failed to set %s in py_info.", "read_info");
		Py_DECREF(py_roles);
		Py_DECREF(py_info);
		goto END;
	}
	if (PyDict_SetItemString(py_info, "write_info",
							 Py_BuildValue("i", (user->write_info ? *(user->write_info): 0))) == -1) {
		as_error_update(err, AEROSPIKE_ERR_CLIENT,
						"Failed to set %s in py_info.", "write_info");
		Py_DECREF(py_roles);
		Py_DECREF(py_info);
		goto END;
	}
	if (PyDict_SetItemString(py_info, "conns_in_use",
							 Py_BuildValue("i", user->conns_in_use)) == -1) {
		as_error_update(err, AEROSPIKE_ERR_CLIENT,
						"Failed to set %s in py_info.", "conns_in_use");
		Py_DECREF(py_roles);
		Py_DECREF(py_info);
		goto END;
	}
	if (PyDict_SetItemString(py_info, "roles", py_roles) == -1) {
		as_error_update(err, AEROSPIKE_ERR_CLIENT,
						"Failed to set %s in py_info.", "roles");
		Py_DECREF(py_roles);
		Py_DECREF(py_info);
		goto END;
	}

	Py_DECREF(py_roles);

	*py_as_user = py_info;

END:
	return err->code;
}

as_status as_role_to_pyobject_old(as_error *err, as_role *role,
								  PyObject **py_as_role)
{
	as_error_reset(err);

	PyObject *py_privileges = PyList_New(0);

	as_privilege_to_pyobject(err, role->privileges, py_privileges,
							 role->privileges_size);
	if (err->code != AEROSPIKE_OK) {
		goto END;
	}

	*py_as_role = py_privileges;

END:
	return err->code;
}

/*
 * as_role_to_pyobject assumes py_as_role_dict is a python list.
 */
as_status as_role_to_pyobject(as_error *err, as_role *role,
							  PyObject *py_as_role_dict)
{
	as_error_reset(err);

	const char *privelege_key = "privileges";
	const char *whitelist_key = "whitelist";
	const char *read_quota_key = "read_quota";
	const char *write_quota_key = "write_quota";

	PyObject *py_read_quota = NULL;
	PyObject *py_write_quota = NULL;

	PyObject *py_privileges = PyList_New(0);
	PyObject *py_whitelist = PyList_New(0);

	if (py_privileges == NULL || py_whitelist == NULL) {
		as_error_update(err, AEROSPIKE_ERR_CLIENT,
						"Failed to create py_as_role_dict, py_privileges, or "
						"py_whitelist.");
		goto END;
	}

	as_privilege_to_pyobject(err, role->privileges, py_privileges,
							 role->privileges_size);
	if (err->code != AEROSPIKE_OK) {
		goto END;
	}

	if (PyDict_SetItemString(py_as_role_dict, privelege_key, py_privileges) ==
		-1) {
		as_error_update(err, AEROSPIKE_ERR_CLIENT,
						"Failed to set %s in py_as_role_dict.", privelege_key);
		goto END;
	}

	if (char_double_ptr_to_py_list(err, role->whitelist_size,
								   AS_IP_ADDRESS_SIZE, role->whitelist,
								   py_whitelist) != AEROSPIKE_OK) {
		goto END;
	}

	if (PyDict_SetItemString(py_as_role_dict, whitelist_key, py_whitelist) ==
		-1) {
		as_error_update(err, AEROSPIKE_ERR_CLIENT,
						"Failed to set %s in py_as_role_dict.", whitelist_key);
		goto END;
	}

	py_read_quota = Py_BuildValue("i", role->read_quota);
	py_write_quota = Py_BuildValue("i", role->write_quota);
	if (py_read_quota == NULL || py_write_quota == NULL) {
		as_error_update(err, AEROSPIKE_ERR_CLIENT,
						"Failed to create py_read_quota or py_write_quota.");
		goto END;
	}

	if (PyDict_SetItemString(py_as_role_dict, read_quota_key, py_read_quota) ==
		-1) {
		as_error_update(err, AEROSPIKE_ERR_CLIENT,
						"Failed to set %s in py_as_role_dict.", read_quota_key);
		goto END;
	}

	if (PyDict_SetItemString(py_as_role_dict, write_quota_key,
							 py_write_quota) == -1) {
		as_error_update(err, AEROSPIKE_ERR_CLIENT,
						"Failed to set %s in py_as_role_dict.",
						write_quota_key);
		goto END;
	}

END:
	Py_XDECREF(py_privileges);
	Py_XDECREF(py_whitelist);
	Py_XDECREF(py_read_quota);
	Py_XDECREF(py_write_quota);
	return err->code;
}

as_status as_privilege_to_pyobject(as_error *err, as_privilege privileges[],
								   PyObject *py_as_privilege,
								   int privilege_size)
{
	as_error_reset(err);

	PyObject *py_ns = NULL;
	PyObject *py_set = NULL;
	PyObject *py_code = NULL;
	for (int i = 0; i < privilege_size; i++) {
		py_ns = PyString_FromString(privileges[i].ns);
		py_set = PyString_FromString(privileges[i].set);
		py_code = PyInt_FromLong(privileges[i].code);

		PyObject *py_privilege = PyDict_New();
		PyDict_SetItemString(py_privilege, "ns", py_ns);
		PyDict_SetItemString(py_privilege, "set", py_set);
		PyDict_SetItemString(py_privilege, "code", py_code);

		Py_DECREF(py_ns);
		Py_DECREF(py_set);
		Py_DECREF(py_code);

		PyList_Append(py_as_privilege, py_privilege);

		Py_DECREF(py_privilege);
	}

	return err->code;
}

as_status pyobject_to_strArray(as_error *err, PyObject *py_list, char **arr,
							   uint32_t max_len)
{

	as_error_reset(err);
	PyObject *py_u_str = NULL;

	if (!PyList_Check(py_list)) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT, "not a list");
	}

	Py_ssize_t size = PyList_Size(py_list);

	char *s;
	for (int i = 0; i < size; i++) {
		PyObject *py_val = PyList_GetItem(py_list, i);

		if (PyString_Check(py_val)) {
			s = PyString_AsString(py_val);

			if (strlen(s) < max_len) {
				strcpy(arr[i], s);
			}
			else {
				as_error_update(err, AEROSPIKE_ERR_CLIENT,
								"String exceeds max length");
				return err->code;
			}
		}
		else if (PyUnicode_Check(py_val)) {
			py_u_str = PyUnicode_AsUTF8String(py_val);
			if (!py_u_str) {
				as_error_update(err, AEROSPIKE_ERR_CLIENT,
								"Unable to convert unicode string");
				return err->code;
			}
			s = PyBytes_AsString(py_u_str);

			if (strlen(s) < max_len) {
				strcpy(arr[i], s);
				Py_DECREF(py_u_str);
			}
			else {
				Py_DECREF(py_u_str);
				as_error_update(err, AEROSPIKE_ERR_CLIENT,
								"String exceeds max length");
				return err->code;
			}
		}
		else {
			as_error_update(err, AEROSPIKE_ERR_CLIENT, "Item is not a string");
			return err->code;
		}
	}

	return err->code;
}

as_status pyobject_to_list(AerospikeClient *self, as_error *err,
						   PyObject *py_list, as_list **list,
						   as_static_pool *static_pool, int serializer_type)
{
	as_error_reset(err);

	Py_ssize_t size = PyList_Size(py_list);

	if (*list == NULL) {
		*list = (as_list *)as_arraylist_new((uint32_t)size, 0);
	}

	for (int i = 0; i < size; i++) {
		PyObject *py_val = PyList_GetItem(py_list, i);
		as_val *val = NULL;
		pyobject_to_val(self, err, py_val, &val, static_pool, serializer_type);
		if (err->code != AEROSPIKE_OK) {
			break;
		}
		as_list_append(*list, val);
	}

	if (err->code != AEROSPIKE_OK) {
		as_list_destroy(*list);
	}

	return err->code;
}

as_status pyobject_to_map(AerospikeClient *self, as_error *err,
						  PyObject *py_dict, as_map **map,
						  as_static_pool *static_pool, int serializer_type)
{
	as_error_reset(err);

	PyObject *py_key = NULL;
	PyObject *py_val = NULL;
	Py_ssize_t pos = 0;
	Py_ssize_t size = PyDict_Size(py_dict);

	if (*map == NULL) {
		*map = (as_map *)as_hashmap_new((uint32_t)size);
	}

	while (PyDict_Next(py_dict, &pos, &py_key, &py_val)) {
		as_val *key = NULL;
		as_val *val = NULL;
		pyobject_to_val(self, err, py_key, &key, static_pool, serializer_type);
		if (err->code != AEROSPIKE_OK) {
			break;
		}
		pyobject_to_val(self, err, py_val, &val, static_pool, serializer_type);
		if (err->code != AEROSPIKE_OK) {
			if (key) {
				as_val_destroy(key);
			}
			break;
		}
		as_map_set(*map, key, val);
	}

	if (err->code != AEROSPIKE_OK) {
		as_map_destroy(*map);
	}

	return err->code;
}

as_status pyobject_to_val(AerospikeClient *self, as_error *err,
						  PyObject *py_obj, as_val **val,
						  as_static_pool *static_pool, int serializer_type)
{
	as_error_reset(err);

	if (!py_obj) {
		// this should never happen, but if it did...
		return as_error_update(err, AEROSPIKE_ERR_CLIENT, "value is null");
	}
	else if (
		PyBool_Check(
			py_obj)) { //TODO Change to true bool support post jump version.
		switch (self->send_bool_as) {
		case SEND_BOOL_AS_PY_BYTES:;
			as_bytes *bool_bytes = NULL;
			if (py_bool_to_py_bytes_blob(self, err, static_pool, py_obj,
										 &bool_bytes,
										 serializer_type) != AEROSPIKE_OK) {
				return err->code;
			}
			*val = (as_val *)bool_bytes;
			break;
		case SEND_BOOL_AS_AS_BOOL:;
			as_boolean *converted_bool = NULL;
			if (py_bool_to_as_bool(err, py_obj, &converted_bool) !=
				AEROSPIKE_OK) {
				return err->code;
			}
			*val = (as_val *)converted_bool;
			break;
		case SEND_BOOL_AS_INTEGER:;
			as_integer *converted_integer = NULL;
			if (py_bool_to_as_integer(err, py_obj, &converted_integer) !=
				AEROSPIKE_OK) {
				return err->code;
			}
			*val = (as_val *)converted_integer;
			break;
		default:
			return as_error_update(err, AEROSPIKE_ERR_CLIENT,
								   "Unknown value for send_bool_as.");
		}
	}
	else if (PyInt_Check(py_obj)) {
		int64_t i = (int64_t)PyInt_AsLong(py_obj);
		if (i == -1 && PyErr_Occurred()) {
			if (PyErr_ExceptionMatches(PyExc_OverflowError)) {
				return as_error_update(err, AEROSPIKE_ERR_PARAM,
									   "integer value exceeds sys.maxsize");
			}
		}
		*val = (as_val *)as_integer_new(i);
	}
	else if (PyLong_Check(py_obj)) {
		int64_t l = (int64_t)PyLong_AsLongLong(py_obj);
		if (l == -1 && PyErr_Occurred()) {
			if (PyErr_ExceptionMatches(PyExc_OverflowError)) {
				return as_error_update(err, AEROSPIKE_ERR_PARAM,
									   "integer value exceeds sys.maxsize");
			}
		}
		*val = (as_val *)as_integer_new(l);
	}
	else if (PyUnicode_Check(py_obj)) {
		PyObject *py_ustr = PyUnicode_AsUTF8String(py_obj);
		char *str = PyBytes_AsString(py_ustr);
		*val = (as_val *)as_string_new(strdup(str), true);
		Py_DECREF(py_ustr);
	}
	else if (PyString_Check(py_obj)) {
		char *s = PyString_AsString(py_obj);
		*val = (as_val *)as_string_new(s, false);
	}
	else if (PyBytes_Check(py_obj)) {
		uint8_t *b = (uint8_t *)PyBytes_AsString(py_obj);
		uint32_t b_len = (uint32_t)PyBytes_Size(py_obj);
		*val = (as_val *)as_bytes_new_wrap(b, b_len, false);
	}
	else if (!strcmp(py_obj->ob_type->tp_name, "aerospike.Geospatial")) {
		PyObject *py_parameter = PyString_FromString("geo_data");
		PyObject *py_data = PyObject_GenericGetAttr(py_obj, py_parameter);
		Py_DECREF(py_parameter);
		char *geo_value =
			PyString_AsString(AerospikeGeospatial_DoDumps(py_data, err));
		Py_DECREF(py_data);
		*val = (as_val *)as_geojson_new(geo_value, false);
	}
	else if (PyByteArray_Check(py_obj)) {
		as_bytes *bytes;
		GET_BYTES_POOL(bytes, static_pool, err);
		if (err->code == AEROSPIKE_OK) {
			if (serialize_based_on_serializer_policy(self, serializer_type,
													 &bytes, py_obj,
													 err) != AEROSPIKE_OK) {
				return err->code;
			}
			*val = (as_val *)bytes;
		}
	}
	else if (PyList_Check(py_obj)) {
		as_list *list = NULL;
		pyobject_to_list(self, err, py_obj, &list, static_pool,
						 serializer_type);
		if (err->code == AEROSPIKE_OK) {
			*val = (as_val *)list;
		}
	}
	else if (PyDict_Check(py_obj)) {
		as_map *map = NULL;
		pyobject_to_map(self, err, py_obj, &map, static_pool, serializer_type);
		if (err->code == AEROSPIKE_OK) {
			if (PyObject_IsInstance(py_obj,
									AerospikeKeyOrderedDict_Get_Type())) {
				// Special case for aerospike.KeyOrderedDict, useful to just in time sort maps for by value operations.
				map->flags |= AS_MAP_KEY_ORDERED;
			}

			*val = (as_val *)map;
		}
	}
	else if (Py_None == py_obj) {
		*val = as_val_reserve(&as_nil);
	}
	else if (!strcmp(py_obj->ob_type->tp_name, "aerospike.null")) {
		*val = (as_val *)as_val_reserve(&as_nil);
	}
	else if (AS_Matches_Classname(py_obj, AS_CDT_WILDCARD_NAME)) {
		*val = (as_val *)as_val_reserve(&as_cmp_wildcard);
	}
	else if (AS_Matches_Classname(py_obj, AS_CDT_INFINITE_NAME)) {
		*val = (as_val *)as_val_reserve(&as_cmp_inf);
	}
	else {
		if (PyFloat_Check(py_obj)) {
			double d = PyFloat_AsDouble(py_obj);
			*val = (as_val *)as_double_new(d);
		}
		else {
			as_bytes *bytes;
			GET_BYTES_POOL(bytes, static_pool, err);
			if (err->code == AEROSPIKE_OK) {
				if (serialize_based_on_serializer_policy(self, serializer_type,
														 &bytes, py_obj,
														 err) != AEROSPIKE_OK) {
					return err->code;
				}
				*val = (as_val *)bytes;
			}
		}
	}

	return err->code;
}

/**
 * Converts a PyObject into an as_record.
 * Returns AEROSPIKE_OK on success. On error, the err argument is populated.
 */
as_status pyobject_to_record(AerospikeClient *self, as_error *err,
							 PyObject *py_rec, PyObject *py_meta,
							 as_record *rec, int serializer_type,
							 as_static_pool *static_pool)
{
	as_error_reset(err);

	if (!py_rec) {
		// this should never happen, but if it did...
		return as_error_update(err, AEROSPIKE_ERR_CLIENT, "record is null");
	}
	else if (PyDict_Check(py_rec)) {
		PyObject *key = NULL, *value = NULL, *py_ukey = NULL;
		Py_ssize_t pos = 0;
		Py_ssize_t size = PyDict_Size(py_rec);
		char *name = NULL;
		long ret_val = 0;

		as_record_init(rec, size);

		while (PyDict_Next(py_rec, &pos, &key, &value)) {

			if (PyUnicode_Check(key)) {
				py_ukey = PyUnicode_AsUTF8String(key);
				if (!py_ukey) {
					return as_error_update(
						err, AEROSPIKE_ERR_CLIENT,
						"Unicode bin name not encoded in utf-8.");
				}
				name = PyBytes_AsString(py_ukey);
			}
			else if (PyString_Check(key)) {
				name = PyString_AsString(key);
			}
			else {
				return as_error_update(
					err, AEROSPIKE_ERR_CLIENT,
					"A bin name must be a string or unicode string.");
			}

			if (self->strict_types) {
				if (strlen(name) > AS_BIN_NAME_MAX_LEN) {
					if (py_ukey) {
						Py_DECREF(py_ukey);
						py_ukey = NULL;
					}
					return as_error_update(
						err, AEROSPIKE_ERR_BIN_NAME,
						"A bin name should not exceed 14 characters limit");
				}
			}

			if (!value) {
				// this should never happen, but if it did...
				return as_error_update(err, AEROSPIKE_ERR_CLIENT,
									   "record is null");
			}
			else if (
				PyBool_Check(
					value)) { //TODO Change to true bool support post jump version.
				switch (self->send_bool_as) {
				case SEND_BOOL_AS_PY_BYTES:;
					as_bytes *bool_bytes = NULL;
					if (py_bool_to_py_bytes_blob(
							self, err, static_pool, value, &bool_bytes,
							serializer_type) != AEROSPIKE_OK) {
						return err->code;
					}
					ret_val = as_record_set_bytes(rec, name, bool_bytes);
					break;
				case SEND_BOOL_AS_AS_BOOL:;
					as_boolean *converted_bool = NULL;
					if (py_bool_to_as_bool(err, value, &converted_bool) !=
						AEROSPIKE_OK) {
						return err->code;
					}
					ret_val = as_record_set_bool(
						rec, name, as_boolean_get(converted_bool));
					break;
				case SEND_BOOL_AS_INTEGER:;
					as_integer *converted_integer = NULL;
					if (py_bool_to_as_integer(err, value, &converted_integer) !=
						AEROSPIKE_OK) {
						return err->code;
					}
					ret_val =
						as_record_set_integer(rec, name, converted_integer);
					break;
				default:
					return as_error_update(err, AEROSPIKE_ERR_CLIENT,
										   "Unknown value for send_bool_as.");
				}
			}
			else if (PyInt_Check(value)) {
				int64_t val = (int64_t)PyInt_AsLong(value);
				if (val == -1 && PyErr_Occurred()) {
					if (PyErr_ExceptionMatches(PyExc_OverflowError)) {
						return as_error_update(
							err, AEROSPIKE_ERR_PARAM,
							"integer value exceeds sys.maxsize");
					}
				}
				ret_val = as_record_set_int64(rec, name, val);
			}
			else if (PyLong_Check(value)) {
				int64_t val = (int64_t)PyLong_AsLongLong(value);
				if (val == -1 && PyErr_Occurred()) {
					if (PyErr_ExceptionMatches(PyExc_OverflowError)) {
						return as_error_update(
							err, AEROSPIKE_ERR_PARAM,
							"integer value exceeds sys.maxsize");
					}
				}
				ret_val = as_record_set_int64(rec, name, val);
			}
			else if (!strcmp(value->ob_type->tp_name, "aerospike.Geospatial")) {
				PyObject *py_geo_string = PyString_FromString("geo_data");
				PyObject *py_data =
					PyObject_GenericGetAttr(value, py_geo_string);
				Py_DECREF(py_geo_string);
				PyObject *py_dumps = AerospikeGeospatial_DoDumps(py_data, err);
				PyObject *py_ustr = NULL;
				char *geo_value = NULL;

				if (PyUnicode_Check(py_dumps)) {
					PyObject *py_ustr = PyUnicode_AsUTF8String(py_dumps);
					if (!py_ustr) {
						return as_error_update(
							err, AEROSPIKE_ERR_CLIENT,
							"Unicode value not encoded in utf-8.");
					}
					geo_value = PyBytes_AsString(py_ustr);
				}
				else {
					geo_value = PyString_AsString(py_dumps);
				}

				ret_val = as_record_set_geojson_strp(rec, name,
													 strdup(geo_value), true);
				if (py_ustr != NULL) {
					Py_DECREF(py_ustr);
				}
				Py_DECREF(py_data);
				Py_DECREF(py_dumps);
			}
			else if (PyUnicode_Check(value)) {
				PyObject *py_ustr = PyUnicode_AsUTF8String(value);
				if (!py_ustr) {
					return as_error_update(
						err, AEROSPIKE_ERR_CLIENT,
						"Unicode value not encoded in utf-8.");
				}
				char *val = PyBytes_AsString(py_ustr);
				ret_val = as_record_set_strp(rec, name, strdup(val), true);
				Py_DECREF(py_ustr);
			}
			else if (PyString_Check(value)) {
				char *val = PyString_AsString(value);
				ret_val = as_record_set_strp(rec, name, val, false);
			}
			else if (PyByteArray_Check(value)) {
				as_bytes *bytes;
				GET_BYTES_POOL(bytes, static_pool, err);
				if (err->code == AEROSPIKE_OK) {
					if (serialize_based_on_serializer_policy(
							self, serializer_type, &bytes, value, err) !=
						AEROSPIKE_OK) {
						return err->code;
					}
					ret_val = as_record_set_bytes(rec, name, bytes);
				}
			}
			else if (PyList_Check(value)) {
				// as_list
				as_list *list = NULL;
				pyobject_to_list(self, err, value, &list, static_pool,
								 serializer_type);
				if (err->code != AEROSPIKE_OK) {
					break;
				}
				ret_val = as_record_set_list(rec, name, list);
			}
			else if (PyDict_Check(value)) {
				// as_map
				as_map *map = NULL;
				pyobject_to_map(self, err, value, &map, static_pool,
								serializer_type);
				if (err->code != AEROSPIKE_OK) {
					break;
				}
				ret_val = as_record_set_map(rec, name, map);
			}
			else if (!strcmp(value->ob_type->tp_name, "aerospike.null")) {
				ret_val = as_record_set_nil(rec, name);
			}
			else {
				if (PyFloat_Check(value)) {
					double val = PyFloat_AsDouble(value);
					ret_val = as_record_set_double(rec, name, val);
				}
				else {
					as_bytes *bytes;
					GET_BYTES_POOL(bytes, static_pool, err);
					if (err->code == AEROSPIKE_OK) {
						if (serialize_based_on_serializer_policy(
								self, serializer_type, &bytes, value, err) !=
							AEROSPIKE_OK) {
							return err->code;
						}
						ret_val = as_record_set_bytes(rec, name, bytes);
					}
				}
			}

			if (py_ukey) {
				Py_DECREF(py_ukey);
				py_ukey = NULL;
			}

			if (self->strict_types) {
				if (!ret_val) {
					return as_error_update(err, AEROSPIKE_ERR_BIN_NAME,
										   "Unable to set key-value pair");
				}
			}
		}

		if (py_meta && py_meta != Py_None) {
			if (!PyDict_Check(py_meta)) {
				as_error_update(err, AEROSPIKE_ERR_PARAM,
								"meta must be a dictionary");
			}
			else {
				PyObject *py_gen = PyDict_GetItemString(py_meta, "gen");
				PyObject *py_ttl = PyDict_GetItemString(py_meta, "ttl");

				if (py_ttl) {
					if (PyInt_Check(py_ttl)) {
						rec->ttl = (uint32_t)PyInt_AsLong(py_ttl);
						if (rec->ttl == (uint32_t)-1 && PyErr_Occurred()) {
							if (PyErr_ExceptionMatches(PyExc_OverflowError)) {
								as_error_update(
									err, AEROSPIKE_ERR_PARAM,
									"integer value exceeds sys.maxsize");
							}
						}
					}
					else if (PyLong_Check(py_ttl)) {
						rec->ttl = (uint32_t)PyLong_AsLongLong(py_ttl);
						if (rec->ttl == (uint32_t)-1 && PyErr_Occurred()) {
							if (PyErr_ExceptionMatches(PyExc_OverflowError)) {
								as_error_update(
									err, AEROSPIKE_ERR_PARAM,
									"integer value exceeds sys.maxsize");
							}
						}
					}
					else {
						as_error_update(err, AEROSPIKE_ERR_PARAM,
										"TTL should be an int or long");
					}
				}

				if (py_gen) {
					if (PyInt_Check(py_gen)) {
						rec->gen = (uint16_t)PyInt_AsLong(py_gen);
						if (rec->gen == (uint16_t)-1 && PyErr_Occurred()) {
							if (PyErr_ExceptionMatches(PyExc_OverflowError)) {
								as_error_update(
									err, AEROSPIKE_ERR_PARAM,
									"integer value exceeds sys.maxsize");
							}
						}
					}
					else if (PyLong_Check(py_gen)) {
						rec->gen = (uint16_t)PyLong_AsLongLong(py_gen);
						if (rec->gen == (uint16_t)-1 && PyErr_Occurred()) {
							if (PyErr_ExceptionMatches(PyExc_OverflowError)) {
								as_error_update(
									err, AEROSPIKE_ERR_PARAM,
									"integer value exceeds sys.maxsize");
							}
						}
					}
					else {
						as_error_update(err, AEROSPIKE_ERR_PARAM,
										"Generation should be an int or long");
					}
				}
			}
		}

		if (err->code != AEROSPIKE_OK) {
			as_record_destroy(rec);
		}
	}
	else {
		as_error_update(err, AEROSPIKE_ERR_PARAM,
						"Record should be passed as bin-value pair");
	}

	return err->code;
}

as_status pyobject_to_key(as_error *err, PyObject *py_keytuple, as_key *key)
{
	as_error_reset(err);

	Py_ssize_t size = 0;
	PyObject *py_ns = NULL;
	PyObject *py_set = NULL;
	PyObject *py_key = NULL;
	PyObject *py_digest = NULL;

	char *ns = NULL;
	char *set = NULL;

	if (!py_keytuple) {
		// this should never happen, but if it did...
		return as_error_update(err, AEROSPIKE_ERR_PARAM, "key is null");
	}
	else if (PyTuple_Check(py_keytuple)) {
		size = PyTuple_Size(py_keytuple);

		if (size < 3 || size > 4) {
			return as_error_update(err, AEROSPIKE_ERR_PARAM,
								   "key tuple must be (Namespace, Set, Key) or "
								   "(Namespace, Set, None, Digest)");
		}

		py_ns = PyTuple_GetItem(py_keytuple, 0);
		py_set = PyTuple_GetItem(py_keytuple, 1);
		py_key = PyTuple_GetItem(py_keytuple, 2);

		if (size == 4) {
			py_digest = PyTuple_GetItem(py_keytuple, 3);
		}
	}
	else if (PyDict_Check(py_keytuple)) {
		py_ns = PyDict_GetItemString(py_keytuple, "ns");
		py_set = PyDict_GetItemString(py_keytuple, "set");
		py_key = PyDict_GetItemString(py_keytuple, "key");
		py_digest = PyDict_GetItemString(py_keytuple, "digest");
	}
	else {
		return as_error_update(err, AEROSPIKE_ERR_PARAM, "key is invalid");
	}

	if (!py_ns) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM,
							   "namespace is required");
	}
	else if (!PyString_Check(py_ns)) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM,
							   "namespace must be a string");
	}
	else {
		ns = PyString_AsString(py_ns);
	}

	PyObject *py_ustr = NULL;
	if (py_set && py_set != Py_None) {
		if (PyString_Check(py_set)) {
			set = PyString_AsString(py_set);
		}
		else if (PyUnicode_Check(py_set)) {
			py_ustr = PyUnicode_AsUTF8String(py_set);
			set = PyBytes_AsString(py_ustr);
		}
		else {
			return as_error_update(err, AEROSPIKE_ERR_PARAM,
								   "set must be a string");
		}
	}

	as_key *returnResult = key;

	if (py_key && py_key != Py_None) {
		if (PyUnicode_Check(py_key)) {
			PyObject *py_ustr = PyUnicode_AsUTF8String(py_key);
			char *k = PyBytes_AsString(py_ustr);
			// free flag has to be true. Because, we are creating a new memory
			// for a primary key string using strdup()
			// This memory is destroyed when we call as_key_destroy()
			returnResult = as_key_init_strp(key, ns, set, strdup(k), true);
			Py_DECREF(py_ustr);
		}
		else if (PyString_Check(py_key)) {
			char *k = PyString_AsString(py_key);
			// free flag is set to false, as char *k is an user memory
			// when as_key_destroy is called, it will try to free this memory
			// which is invalid.
			returnResult = as_key_init_strp(key, ns, set, k, false);
		}
		else if (PyInt_Check(py_key)) {
			int64_t k = (int64_t)PyInt_AsLong(py_key);
			if (-1 == k && PyErr_Occurred()) {
				as_error_update(err, AEROSPIKE_ERR_PARAM,
								"integer value for KEY exceeds sys.maxsize");
			}
			else {
				returnResult = as_key_init_int64(key, ns, set, k);
			}
		}
		else if (PyLong_Check(py_key)) {
			int64_t k = (int64_t)PyLong_AsLongLong(py_key);
			if (-1 == k && PyErr_Occurred()) {
				as_error_update(err, AEROSPIKE_ERR_PARAM,
								"integer value for KEY exceeds sys.maxsize");
			}
			else {
				returnResult = as_key_init_int64(key, ns, set, k);
			}
		}
		else if (PyByteArray_Check(py_key)) {
			uint32_t sz = (uint32_t)PyByteArray_Size(py_key);

			if (sz <= 0) {
				as_error_update(err, AEROSPIKE_ERR_PARAM,
								"Byte array size cannot be 0");
			}
			else {
				uint8_t *byte_array = (uint8_t *)PyByteArray_AsString(py_key);
				returnResult = as_key_init_raw(key, ns, set, byte_array, sz);
			}
		}
		else if (PyBytes_Check(py_key)) {
			char *k = PyBytes_AsString(py_key);
			returnResult = as_key_init_strp(key, ns, set, strdup(k), true);
		}
		else {
			as_error_update(err, AEROSPIKE_ERR_PARAM, "key is invalid");
		}
	}
	else if (py_digest && py_digest != Py_None) {
		if (PyByteArray_Check(py_digest)) {
			uint32_t sz = (uint32_t)PyByteArray_Size(py_digest);

			if (sz != AS_DIGEST_VALUE_SIZE) {
				as_error_update(err, AEROSPIKE_ERR_PARAM,
								"digest size is invalid. should be 20 bytes, "
								"but received %d",
								sz);
			}
			else {
				uint8_t *digest = (uint8_t *)PyByteArray_AsString(py_digest);
				returnResult = as_key_init_digest(key, ns, set, digest);
			}
		}
		else {
			as_error_update(err, AEROSPIKE_ERR_PARAM,
							"digest is invalid. expected a bytearray");
		}
	}
	else {
		as_error_update(err, AEROSPIKE_ERR_PARAM,
						"either key or digest is required");
	}

	if (py_ustr) {
		Py_DECREF(py_ustr);
	}

	if (!returnResult) {
		as_error_update(err, AEROSPIKE_ERR_PARAM, "key is invalid");
	}

	return err->code;
}

typedef struct {
	as_error *err;
	uint32_t count;
	AerospikeClient *client;
	void *udata;
} conversion_data;

as_status do_val_to_pyobject(AerospikeClient *self, as_error *err,
							 const as_val *val, PyObject **py_val,
							 bool cnvt_list_to_map)
{
	as_error_reset(err);
	switch (as_val_type(val)) {
	case AS_INTEGER: {
		as_integer *i = as_integer_fromval(val);
		*py_val = PyInt_FromLong((long)as_integer_get(i));
		if (!*py_val) {
			as_error_update(err, AEROSPIKE_ERR_CLIENT,
							"Failed to create integer or long.");
		}
		break;
	}
	case AS_DOUBLE: {
		as_double *d = as_double_fromval(val);
		*py_val = PyFloat_FromDouble(as_double_get(d));

		if (!*py_val) {
			as_error_update(err, AEROSPIKE_ERR_CLIENT,
							"Failed to create float.");
		}

		break;
	}
	case AS_STRING: {
		as_string *s = as_string_fromval(val);
		char *str = as_string_get(s);
		if (str) {
			*py_val = PyString_FromString(str);
			if (!*py_val) {
				size_t sz = strlen(str);
				*py_val = PyUnicode_DecodeUTF8(str, sz, NULL);
			}
			if (*py_val == NULL) {
				as_error_update(err, AEROSPIKE_ERR_CLIENT,
								"Unknown type for value");
				return err->code;
			}
		}
		else {
			Py_INCREF(Py_None);
			*py_val = Py_None;
		}
		break;
	}
	case AS_BOOLEAN: {
		as_boolean *b = as_boolean_fromval(val);
		*py_val = PyBool_FromLong((long)as_boolean_get(b));
		break;
	}
	case AS_BYTES: {
		//uint32_t bval_size = as_bytes_size(bval);
		as_bytes *bval = as_bytes_fromval(val);
		if (deserialize_based_on_as_bytes_type(self, bval, py_val, err) !=
			AEROSPIKE_OK) {
			return err->code;
		}
		break;
	}
	case AS_LIST: {
		as_list *l = as_list_fromval((as_val *)val);
		if (l) {
			PyObject *py_list = NULL;
			if (cnvt_list_to_map) {
				as_list_of_map_to_py_tuple_list(self, err, l, &py_list);
			}
			else {
				list_to_pyobject(self, err, l, &py_list);
			}
			if (err->code == AEROSPIKE_OK) {
				*py_val = py_list;
			}
		}
		break;
	}
	case AS_MAP: {
		as_map *m = as_map_fromval(val);
		if (m) {
			PyObject *py_map = NULL;
			map_to_pyobject(self, err, m, &py_map);
			if (err->code == AEROSPIKE_OK) {
				*py_val = py_map;
			}
		}
		break;
	}
	case AS_REC: {
		as_record *r = as_record_fromval(val);
		if (r) {
			PyObject *py_rec = NULL;
			record_to_pyobject(self, err, r, NULL, &py_rec);
			if (err->code == AEROSPIKE_OK) {
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
	case AS_GEOJSON: {
		as_geojson *gp = as_geojson_fromval(val);
		char *locstr = as_geojson_get(gp);
		PyObject *py_locstr = PyString_FromString(locstr);
		PyObject *py_loads = AerospikeGeospatial_DoLoads(py_locstr, err);
		Py_DECREF(py_locstr);
		if (err->code != AEROSPIKE_OK) {
			break;
		}
		*py_val = AerospikeGeospatial_New(err, py_loads);
		if (py_loads) {
			Py_DECREF(py_loads);
		}
		break;
	}
	default: {
		as_error_update(err, AEROSPIKE_ERR_CLIENT, "Unknown type for value");
		return err->code;
	}
	}

	return err->code;
}

as_status val_to_pyobject(AerospikeClient *self, as_error *err,
						  const as_val *val, PyObject **py_val)
{
	return do_val_to_pyobject(self, err, val, py_val, false);
}

as_status val_to_pyobject_cnvt_list_to_map(AerospikeClient *self, as_error *err,
										   const as_val *val, PyObject **py_val)
{
	return do_val_to_pyobject(self, err, val, py_val, true);
}

as_status as_list_of_map_to_py_tuple_list(AerospikeClient *self, as_error *err,
										  const as_list *list,
										  PyObject **py_list)
{
	PyObject *py_tuple = NULL;

	int size = as_list_size((as_list *)list);

	if (size % 2 != 0) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT,
							   "Invalid key list of key/value pairs");
	}

	*py_list = PyList_New(0);
	if (!*py_list) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT,
							   "Failed to allocate memory for list.");
	}

	for (int i = 0; i < size; i += 2) {
		as_val *key = as_list_get(list, i);
		as_val *value = as_list_get(list, i + 1);

		if (!key || !value) {
			as_error_update(err, AEROSPIKE_ERR_CLIENT,
							"Null object found in returned list");
			goto CLEANUP;
		}

		PyObject *py_key = NULL;
		PyObject *py_value = NULL;

		if (val_to_pyobject(self, err, key, &py_key) != AEROSPIKE_OK) {
			goto CLEANUP;
		}
		if (val_to_pyobject(self, err, value, &py_value) != AEROSPIKE_OK) {
			Py_XDECREF(py_key);
			goto CLEANUP;
		}
		py_tuple = PyTuple_New(2);

		if (!py_tuple) {
			as_error_update(err, AEROSPIKE_ERR_CLIENT,
							"Failed to allocate memory for tuple");
			Py_XDECREF(py_key);
			Py_XDECREF(py_value);
			goto CLEANUP;
		}

		PyTuple_SetItem(py_tuple, 0, py_key);
		PyTuple_SetItem(py_tuple, 1, py_value);

		PyList_Append(*py_list, py_tuple);
		Py_DECREF(py_tuple);
	}

CLEANUP:
	if (err->code != AEROSPIKE_OK) {
		Py_DECREF(*py_list);
	}

	return err->code;
}

static bool list_to_pyobject_each(as_val *val, void *udata)
{
	if (!val) {
		return false;
	}

	conversion_data *convd = (conversion_data *)udata;
	as_error *err = convd->err;
	PyObject *py_list = (PyObject *)convd->udata;

	PyObject *py_val = NULL;
	val_to_pyobject(convd->client, convd->err, val, &py_val);

	if (err->code != AEROSPIKE_OK) {
		return false;
	}

	PyList_SetItem(py_list, convd->count, py_val);

	convd->count++;
	return true;
}

as_status list_to_pyobject(AerospikeClient *self, as_error *err,
						   const as_list *list, PyObject **py_list)
{
	*py_list = PyList_New(as_list_size((as_list *)list));

	if (!*py_list) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT,
							   "Failed to allocate memory for list");
	}

	conversion_data convd = {
		.err = err, .count = 0, .client = self, .udata = *py_list};

	as_list_foreach(list, list_to_pyobject_each, &convd);

	if (err->code != AEROSPIKE_OK) {
		Py_CLEAR(*py_list);
		return err->code;
	}

	return err->code;
}

static bool map_to_pyobject_each(const as_val *key, const as_val *val,
								 void *udata)
{

	conversion_data *convd = (conversion_data *)udata;
	as_error *err = convd->err;

	if (!key || !val) {
		as_error_update(err, AEROSPIKE_ERR_CLIENT,
						"Received null key or value");
		return false;
	}

	PyObject *py_dict = (PyObject *)convd->udata;

	PyObject *py_key = NULL;
	val_to_pyobject(convd->client, convd->err, key, &py_key);

	if (err->code != AEROSPIKE_OK) {
		return false;
	}

	PyObject *py_val = NULL;
	val_to_pyobject(convd->client, convd->err, val, &py_val);

	if (err->code != AEROSPIKE_OK) {
		Py_DECREF(py_key);
		return false;
	}

	/* We failed to set a dictionary item. This is probably
	 * due to an unhashable keytype
	 */
	if (PyDict_SetItem(py_dict, py_key, py_val) == -1) {
		if (PyErr_Occurred() && PyErr_ExceptionMatches(PyExc_TypeError)) {
			as_error_update(
				err, AEROSPIKE_ERR_CLIENT,
				"Unable to use unhashable type as a dictionary key");
		}
		else {
			as_error_update(err, AEROSPIKE_ERR_CLIENT,
							"Unable to add dictionary item");
		}
		Py_CLEAR(py_key);
		Py_CLEAR(py_val);
		return false;
	}

	Py_DECREF(py_key);
	Py_DECREF(py_val);

	convd->count++;
	return true;
}

as_status map_to_pyobject(AerospikeClient *self, as_error *err,
						  const as_map *map, PyObject **py_map)
{
	*py_map = PyDict_New();

	if (!*py_map) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT,
							   "Failed to allocate memory for dictionary.");
	}

	conversion_data convd = {
		.err = err, .count = 0, .client = self, .udata = *py_map};

	as_map_foreach(map, map_to_pyobject_each, &convd);

	if (err->code != AEROSPIKE_OK) {
		Py_DECREF(*py_map);
		return err->code;
	}

	return err->code;
}

as_status do_record_to_pyobject(AerospikeClient *self, as_error *err,
								const as_record *rec, const as_key *key,
								PyObject **obj, bool cnvt_list_to_map)
{
	as_error_reset(err);
	*obj = NULL;

	if (!rec) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT, "record is null");
	}

	PyObject *py_rec = NULL;
	PyObject *py_rec_key = NULL;
	PyObject *py_rec_meta = NULL;
	PyObject *py_rec_bins = NULL;

	if (key_to_pyobject(err, key ? key : &rec->key, &py_rec_key) !=
		AEROSPIKE_OK) {
		return err->code;
	}

	if (metadata_to_pyobject(err, rec, &py_rec_meta) != AEROSPIKE_OK) {
		Py_CLEAR(py_rec_key);
		return err->code;
	}

	if (bins_to_pyobject(self, err, rec, &py_rec_bins, cnvt_list_to_map) !=
		AEROSPIKE_OK) {
		Py_CLEAR(py_rec_key);
		Py_CLEAR(py_rec_meta);
		return err->code;
	}

	if (!py_rec_key) {
		Py_INCREF(Py_None);
		py_rec_key = Py_None;
	}

	if (!py_rec_meta) {
		Py_INCREF(Py_None);
		py_rec_meta = Py_None;
	}

	if (!py_rec_bins) {
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

as_status record_to_resultpyobject(AerospikeClient *self, 
								as_error *err,
								const as_record *rec,
								PyObject **obj)
{
	as_error_reset(err);
	*obj = NULL;

	if (!rec) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT, "record is null");
	}

	PyObject *py_rec = NULL;
	PyObject *py_rec_meta = NULL;
	PyObject *py_rec_bins = NULL;

	if (metadata_to_pyobject(err, rec, &py_rec_meta) != AEROSPIKE_OK) {
		return err->code;
	}

	if (bins_to_pyobject(self, err, rec, &py_rec_bins, false) !=
		AEROSPIKE_OK) {
		Py_CLEAR(py_rec_meta);
		return err->code;
	}

	if (!py_rec_meta) {
		Py_INCREF(Py_None);
		py_rec_meta = Py_None;
	}

	if (!py_rec_bins) {
		Py_INCREF(Py_None);
		py_rec_bins = Py_None;
	}

	py_rec = PyTuple_New(2);
	PyTuple_SetItem(py_rec, 0, py_rec_meta);
	PyTuple_SetItem(py_rec, 1, py_rec_bins);

	*obj = py_rec;
	return err->code;
}

as_status record_to_pyobject(AerospikeClient *self, as_error *err,
							 const as_record *rec, const as_key *key,
							 PyObject **obj)
{
	return do_record_to_pyobject(self, err, rec, key, obj, false);
}

as_status record_to_pyobject_cnvt_list_to_map(AerospikeClient *self,
											  as_error *err,
											  const as_record *rec,
											  const as_key *key, PyObject **obj)
{
	return do_record_to_pyobject(self, err, rec, key, obj, true);
}

as_status key_to_pyobject(as_error *err, const as_key *key, PyObject **obj)
{
	as_error_reset(err);

	*obj = NULL;

	if (!key) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT, "key is null");
	}

	PyObject *py_namespace = NULL;
	PyObject *py_set = NULL;
	PyObject *py_key = NULL;
	PyObject *py_digest = NULL;

	if (strlen(key->ns) > 0) {
		py_namespace = PyString_FromString(key->ns);
	}

	if (strlen(key->set) > 0) {
		py_set = PyString_FromString(key->set);
	}

	if (key->valuep) {
		as_val *val = (as_val *)key->valuep;
		as_val_t type = as_val_type(val);
		switch (type) {
		case AS_INTEGER: {
			as_integer *ival = as_integer_fromval(val);
			py_key = PyInt_FromLong((long)as_integer_get(ival));
			break;
		}
		case AS_STRING: {
			as_string *sval = as_string_fromval(val);
			py_key = PyString_FromString(as_string_get(sval));
			if (!py_key) {
				py_key = PyUnicode_DecodeUTF8(as_string_get(sval),
											  as_string_len(sval), NULL);
				PyErr_Clear();
			}
			if (!py_key) {
				py_key = PyBytes_FromString(as_string_get(sval));
			}
			if (!py_key) {
				as_error_update(err, AEROSPIKE_ERR_CLIENT,
								"Unknown type for value");
				return err->code;
			}
			break;
		}
		case AS_BYTES: {
			as_bytes *bval = as_bytes_fromval(val);
			if (bval) {
				uint32_t bval_size = as_bytes_size(bval);
				py_key = PyByteArray_FromStringAndSize(
					(char *)as_bytes_get(bval), bval_size);
			}
			break;
		}
		default: {
			break;
		}
		}
	}

	if (key->digest.init) {
		py_digest = PyByteArray_FromStringAndSize((char *)key->digest.value,
												  AS_DIGEST_VALUE_SIZE);
	}

	if (!py_namespace) {
		Py_INCREF(Py_None);
		py_namespace = Py_None;
	}

	if (!py_set) {
		Py_INCREF(Py_None);
		py_set = Py_None;
	}

	if (!py_key) {
		Py_INCREF(Py_None);
		py_key = Py_None;
	}

	if (!py_digest) {
		Py_INCREF(Py_None);
		py_digest = Py_None;
	}

	PyObject *py_keyobj = PyTuple_New(4);
	PyTuple_SetItem(py_keyobj, PY_KEYT_NAMESPACE, py_namespace);
	PyTuple_SetItem(py_keyobj, PY_KEYT_SET, py_set);
	PyTuple_SetItem(py_keyobj, PY_KEYT_KEY, py_key);
	PyTuple_SetItem(py_keyobj, PY_KEYT_DIGEST, py_digest);

	*obj = py_keyobj;
	return err->code;
}

static bool do_bins_to_pyobject_each(const char *name, const as_val *val,
									 void *udata, bool cnvt_list_to_map)
{
	if (!name || !val) {
		return false;
	}

	conversion_data *convd = (conversion_data *)udata;
	as_error *err = convd->err;
	PyObject *py_bins = (PyObject *)convd->udata;
	PyObject *py_val = NULL;

	if (cnvt_list_to_map) {
		val_to_pyobject_cnvt_list_to_map(convd->client, err, val, &py_val);
	}
	else {
		val_to_pyobject(convd->client, err, val, &py_val);
	}

	if (err->code != AEROSPIKE_OK) {
		return false;
	}

	PyDict_SetItemString(py_bins, name, py_val);

	Py_DECREF(py_val);

	convd->count++;
	return true;
}

static bool bins_to_pyobject_each_cnvt_list_to_map(const char *name,
												   const as_val *val,
												   void *udata)
{
	return do_bins_to_pyobject_each(name, val, udata, true);
}

static bool bins_to_pyobject_each(const char *name, const as_val *val,
								  void *udata)
{
	return do_bins_to_pyobject_each(name, val, udata, false);
}

as_status bins_to_pyobject(AerospikeClient *self, as_error *err,
						   const as_record *rec, PyObject **py_bins,
						   bool cnvt_list_to_map)
{
	as_error_reset(err);

	if (!rec) {
		// this should never happen, but if it did...
		return as_error_update(err, AEROSPIKE_ERR_CLIENT, "record is null");
	}

	*py_bins = PyDict_New();

	conversion_data convd = {
		.err = err, .count = 0, .client = self, .udata = *py_bins};

	as_record_foreach(rec,
					  cnvt_list_to_map ? bins_to_pyobject_each_cnvt_list_to_map
									   : bins_to_pyobject_each,
					  &convd);

	if (err->code != AEROSPIKE_OK) {
		Py_DECREF(*py_bins);
		return err->code;
	}

	return err->code;
}

/*
 * operate_bins_to_pyobject
 *
 * Because operate returns a record with multiple entries per bin value, this function
 * iterates over each bin and adds a tuple of the form (bin_name, bin_value). These tuples
 * are stored in a list.
 * e.g. [('bin1', 5), ('bin1', 6), ('bin2', [3,4,5])]
 * Other converter functions assume one entry per bin name.
 * @param self A pointer to a Python Client aerospike wrapper, needed for deserialization
 * @param err pointer to as_error. It will be filled if an error is encountered.
 * @param rec the C client record to be converted to a list of tuples of bin values
 * @param py_bins Pointer to pointer to PyObject. It will be allocated and filled with list of tuples of bin values
 */
as_status operate_bins_to_pyobject(AerospikeClient *self, as_error *err,
								   const as_record *rec, PyObject **py_bins)
{
	as_error_reset(err);
	PyObject *py_bin_value = NULL;
	PyObject *py_bin_pair = NULL;

	if (!rec) {
		// this should never happen, but if it did...
		return as_error_update(err, AEROSPIKE_ERR_CLIENT, "record is null");
	}

	*py_bins = PyList_New(0);
	as_record_iterator it;
	as_record_iterator_init(&it, rec);

	while (as_record_iterator_has_next(&it)) {
		as_bin *bin = as_record_iterator_next(&it);
		py_bin_value = NULL;
		py_bin_pair = NULL;
		val_to_pyobject(self, err, (as_val *)as_bin_get_value(bin),
						&py_bin_value);
		if (err->code != AEROSPIKE_OK) {
			goto CLEANUP;
		}
		if (!py_bin_value) {
			as_error_update(err, AEROSPIKE_ERR_CLIENT,
							"Null entry in operate ordered conversion");
			goto CLEANUP;
		}
		py_bin_pair = Py_BuildValue("sO", as_bin_get_name(bin), py_bin_value);
		if (!py_bin_pair) {
			as_error_update(err, AEROSPIKE_ERR_CLIENT,
							"Unable to build bin entry");
			Py_DECREF(py_bin_value);
			goto CLEANUP;
		}
		Py_DECREF(py_bin_value);
		PyList_Append(*py_bins, py_bin_pair);
		Py_DECREF(py_bin_pair);
	}

CLEANUP:
	if (err->code != AEROSPIKE_OK) {
		Py_DECREF(*py_bins);
	}

	as_record_iterator_destroy(&it);
	return err->code;
}

as_status metadata_to_pyobject(as_error *err, const as_record *rec,
							   PyObject **obj)
{
	as_error_reset(err);

	if (!rec) {
		// this should never happen, but if it did...
		return as_error_update(err, AEROSPIKE_ERR_CLIENT, "record is null");
	}

	PyObject *py_ttl = PyInt_FromLong(rec->ttl);
	PyObject *py_gen = PyInt_FromLong(rec->gen);

	PyObject *py_meta = PyDict_New();
	PyDict_SetItemString(py_meta, "ttl", py_ttl);
	PyDict_SetItemString(py_meta, "gen", py_gen);

	Py_DECREF(py_ttl);
	Py_DECREF(py_gen);

	*obj = py_meta;
	return err->code;
}

bool error_to_pyobject(const as_error *err, PyObject **obj)
{
	PyObject *py_file = NULL;
	if (err->file) {
		py_file = PyString_FromString(err->file);
	}
	else {
		Py_INCREF(Py_None);
		py_file = Py_None;
	}
	PyObject *py_line = NULL;
	if (err->line > 0) {
		py_line = PyInt_FromLong(err->line);
	}
	else {
		Py_INCREF(Py_None);
		py_line = Py_None;
	}

	PyObject *py_code = PyLong_FromLongLong(err->code);
	PyObject *py_message = PyString_FromString(err->message);

	PyObject *py_in_doubt = err->in_doubt ? Py_True : Py_False;
	Py_INCREF(py_in_doubt);

	PyObject *py_err = PyTuple_New(5);
	PyTuple_SetItem(py_err, PY_EXCEPTION_CODE, py_code);
	PyTuple_SetItem(py_err, PY_EXCEPTION_MSG, py_message);
	PyTuple_SetItem(py_err, PY_EXCEPTION_FILE, py_file);
	PyTuple_SetItem(py_err, PY_EXCEPTION_LINE, py_line);
	PyTuple_SetItem(py_err, AS_PY_EXCEPTION_IN_DOUBT, py_in_doubt);
	*obj = py_err;
	return true;
}

void initialize_bin_for_strictypes(AerospikeClient *self, as_error *err,
								   PyObject *py_value, as_binop *binop,
								   char *bin, as_static_pool *static_pool)
{

	as_bin *binop_bin = &binop->bin;
	if (PyInt_Check(py_value)) {
		int val = PyInt_AsLong(py_value);
		as_integer_init((as_integer *)&binop_bin->value, val);
		binop_bin->valuep = &binop_bin->value;
	}
	else if (PyLong_Check(py_value)) {
		long val = PyLong_AsLong(py_value);
		as_integer_init((as_integer *)&binop_bin->value, val);
		binop_bin->valuep = &binop_bin->value;
	}
	else if (PyString_Check(py_value)) {
		char *val = PyString_AsString(py_value);
		as_string_init((as_string *)&binop_bin->value, val, false);
		binop_bin->valuep = &binop_bin->value;
	}
	else if (PyUnicode_Check(py_value)) {
		PyObject *py_ustr1 = PyUnicode_AsUTF8String(py_value);
		char *val = PyBytes_AsString(py_ustr1);
		as_string_init((as_string *)&binop_bin->value, val, false);
		binop_bin->valuep = &binop_bin->value;
		Py_XDECREF(py_ustr1);
	}
	else if (PyFloat_Check(py_value)) {
		int64_t val = PyFloat_AsDouble(py_value);
		as_double_init((as_double *)&binop_bin->value, val);
		binop_bin->valuep = &binop_bin->value;
	}
	else if (PyList_Check(py_value)) {
		as_list *list = NULL;
		pyobject_to_list(self, err, py_value, &list, static_pool,
						 SERIALIZER_PYTHON);
		((as_val *)&binop_bin->value)->type = AS_UNKNOWN;
		binop_bin->valuep = (as_bin_value *)list;
	}
	else if (PyDict_Check(py_value)) {
		as_map *map = NULL;
		pyobject_to_map(self, err, py_value, &map, static_pool,
						SERIALIZER_PYTHON);
		((as_val *)&binop_bin->value)->type = AS_UNKNOWN;
		binop_bin->valuep = (as_bin_value *)map;
	}
	else if (!strcmp(py_value->ob_type->tp_name, "aerospike.Geospatial")) {
		PyObject *py_data =
			PyObject_GenericGetAttr(py_value, PyString_FromString("geo_data"));
		char *geo_value =
			PyString_AsString(AerospikeGeospatial_DoDumps(py_data, err));
		as_geojson_init((as_geojson *)&binop_bin->value, geo_value, false);
		binop_bin->valuep = &binop_bin->value;
	}
	else if (!strcmp(py_value->ob_type->tp_name, "aerospike.null")) {
		((as_val *)&binop_bin->value)->type = AS_UNKNOWN;
		binop_bin->valuep = (as_bin_value *)&as_nil;
	}
	else if (PyByteArray_Check(py_value)) {
		as_bytes *bytes;
		GET_BYTES_POOL(bytes, static_pool, err);
		serialize_based_on_serializer_policy(self, SERIALIZER_PYTHON, &bytes,
											 py_value, err);
		as_bytes_init_wrap((as_bytes *)&binop_bin->value, bytes->value,
						   bytes->size, true);
		binop_bin->valuep = &binop_bin->value;
	}
	else {
		as_bytes *bytes;
		GET_BYTES_POOL(bytes, static_pool, err);
		serialize_based_on_serializer_policy(self, SERIALIZER_PYTHON, &bytes,
											 py_value, err);
		((as_val *)&binop_bin->value)->type = AS_UNKNOWN;
		binop_bin->valuep = (as_bin_value *)bytes;
	}
	strcpy(binop_bin->name, bin);
}

as_status bin_strict_type_checking(AerospikeClient *self, as_error *err,
								   PyObject *py_bin, char **bin)
{
	as_error_reset(err);

	if (py_bin) {
		if (PyString_Check(py_bin)) {
			*bin = PyString_AsString(py_bin);
		}
		else if (PyByteArray_Check(py_bin)) {
			*bin = PyByteArray_AsString(py_bin);
		}
		else {
			as_error_update(err, AEROSPIKE_ERR_PARAM,
							"Bin name should be of type string");
			goto CLEANUP;
		}

		if (self->strict_types) {
			if (strlen(*bin) > AS_BIN_NAME_MAX_LEN) {
				as_error_update(
					err, AEROSPIKE_ERR_BIN_NAME,
					"A bin name should not exceed 14 characters limit");
			}
		}
	}

CLEANUP:
	if (err->code != AEROSPIKE_OK) {
		PyObject *py_err = NULL;
		error_to_pyobject(err, &py_err);
		PyObject *exception_type = raise_exception(err);
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
	}
	return err->code;
}

/**
 *******************************************************************************************************
 * This function checks for metadata and if present set it into the
 * as_operations.
 *
 * @param py_meta               The dictionary of metadata.
 * @param ops                   The as_operations object.
 * @param err                   The as_error to be populated by the function
 *                              with the encountered error if any.
 *
 * Returns: error code.
 *******************************************************************************************************
 */
as_status check_for_meta(PyObject *py_meta, as_operations *ops, as_error *err)
{
	as_error_reset(err);
	if (py_meta && PyDict_Check(py_meta)) {
		PyObject *py_gen = PyDict_GetItemString(py_meta, "gen");
		PyObject *py_ttl = PyDict_GetItemString(py_meta, "ttl");
		uint32_t ttl = 0;
		uint16_t gen = 0;
		if (py_ttl) {
			if (PyInt_Check(py_ttl)) {
				ttl = (uint32_t)PyInt_AsLong(py_ttl);
			}
			else if (PyLong_Check(py_ttl)) {
				ttl = (uint32_t)PyLong_AsLongLong(py_ttl);
			}
			else {
				return as_error_update(err, AEROSPIKE_ERR_PARAM,
									   "Ttl should be an int or long");
			}

			if ((uint32_t)-1 == ttl && PyErr_Occurred()) {
				return as_error_update(
					err, AEROSPIKE_ERR_PARAM,
					"integer value for ttl exceeds sys.maxsize");
			}
			ops->ttl = ttl;
		}

		if (py_gen) {
			if (PyInt_Check(py_gen)) {
				gen = (uint16_t)PyInt_AsLong(py_gen);
			}
			else if (PyLong_Check(py_gen)) {
				gen = (uint16_t)PyLong_AsLongLong(py_gen);
			}
			else {
				return as_error_update(err, AEROSPIKE_ERR_PARAM,
									   "Generation should be an int or long");
			}

			if ((uint16_t)-1 == gen && PyErr_Occurred()) {
				return as_error_update(
					err, AEROSPIKE_ERR_PARAM,
					"integer value for gen exceeds sys.maxsize");
			}
			ops->gen = gen;
		}
	}
	else if (py_meta && (py_meta != Py_None)) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM,
							   "Metadata should be of type dictionary");
	}
	return err->code;
}

as_status pyobject_to_index(AerospikeClient *self, as_error *err,
							PyObject *py_value, long *long_val)
{
	if (PyInt_Check(py_value)) {
		*long_val = PyInt_AsLong(py_value);
	}
	else if (PyLong_Check(py_value)) {
		*long_val = PyLong_AsLong(py_value);
		if (*long_val == -1 && PyErr_Occurred() && self->strict_types) {
			if (PyErr_ExceptionMatches(PyExc_OverflowError)) {
				return as_error_update(err, AEROSPIKE_ERR_PARAM,
									   "integer value exceeds sys.maxsize");
			}
		}
	}
	else {
		return as_error_update(err, AEROSPIKE_ERR_PARAM,
							   "Offset should be of int or long type");
	}

	return err->code;
}

as_status as_batch_read_results_to_pyobject(as_error *err,
											AerospikeClient *client,
											const as_batch_read *results,
											uint32_t size,
											PyObject **py_records)
{
	*py_records = NULL;
	PyObject *temp_py_recs = PyList_New(0);

	if (!temp_py_recs) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT,
							   "Failed to allocate memory for batch results");
	}

	// Loop over results array
	for (uint32_t i = 0; i < size; i++) {
		PyObject *py_rec = NULL;
		PyObject *py_key = NULL;
		if (results[i].result == AEROSPIKE_OK) {
			/* There was a record for the item, but we failed to convert it, probably a deserialize issue, error out */
			record_to_pyobject(client, err, &results[i].record, results[i].key,
							   &py_rec);
			if (!py_rec || err->code != AEROSPIKE_OK) {
				Py_XDECREF(temp_py_recs);
				return err->code;
			}
			/* The record wasn't found, build a (key, None, None) tuple */
		}
		else {
			key_to_pyobject(err, results[i].key, &py_key);
			if (!py_key || err->code != AEROSPIKE_OK) {
				Py_XDECREF(temp_py_recs);
				return err->code;
			}
			py_rec = Py_BuildValue("OOO", py_key, Py_None, Py_None);
			Py_DECREF(py_key);
		}

		if (!py_rec) {
			/* This means that build value, failed, so we are in trouble*/
			Py_XDECREF(temp_py_recs);
			return as_error_update(
				err, AEROSPIKE_ERR_CLIENT,
				"Failed to allocate memory for record entry");
		}

		if (PyList_Append(temp_py_recs, py_rec) != 0) {
			Py_DECREF(py_rec);
			Py_DECREF(temp_py_recs);
			return as_error_update(err, AEROSPIKE_ERR_CLIENT,
								   "Failed to add record to results");
		}
		Py_DECREF(py_rec);
	}

	// Release Python State
	*py_records = temp_py_recs;
	return AEROSPIKE_OK;
}

as_status batch_read_records_to_pyobject(AerospikeClient *self, as_error *err,
										 as_batch_read_records *records,
										 PyObject **py_recs)
{
	*py_recs = PyList_New(0);

	if (!(*py_recs)) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT,
							   "Failed to allocate return list of records");
	}
	as_vector *list = &records->list;
	for (uint32_t i = 0; i < list->size; i++) {

		as_batch_read_record *batch = as_vector_get(list, i);
		PyObject *py_rec = NULL;
		PyObject *py_key = NULL;

		/* There should be a record, so convert it to a tuple */
		if (batch->result == AEROSPIKE_OK) {
			record_to_pyobject(self, err, &batch->record, &batch->key, &py_rec);
			if (!py_rec || err->code != AEROSPIKE_OK) {
				Py_CLEAR(*py_recs);
				return err->code;
			}
			/* No record, convert to (key, None, None) */
		}
		else {
			key_to_pyobject(err, &batch->key, &py_key);
			if (!py_key || err->code != AEROSPIKE_OK) {
				Py_CLEAR(*py_recs);
				return err->code;
			}
			py_rec = Py_BuildValue("OOO", py_key, Py_None, Py_None);
			Py_DECREF(py_key);
			if (!py_rec) {
				as_error_update(err, AEROSPIKE_ERR_CLIENT,
								"Failed to create a record tuple");
				Py_CLEAR(*py_recs);
				return err->code;
			}
		}

		if (PyList_Append(*py_recs, py_rec) != 0) {
			as_error_update(err, AEROSPIKE_ERR_CLIENT,
							"Failed to add record tuple to return list");
			Py_XDECREF(py_rec);
			Py_CLEAR(*py_recs);
			return err->code;
		}
		Py_DECREF(py_rec);
	}
	return AEROSPIKE_OK;
}

/*
This fetches a string from a Python String like. If it is a unicode in Python27, we need to convert it
to a bytes like object first, and keep track of the intermediate object for later deletion.
*/
as_status string_and_pyuni_from_pystring(PyObject *py_string,
										 PyObject **pyuni_r, char **c_str_ptr,
										 as_error *err)
{
	/* Not needed if we drop support for Python > 3 < 3.3 */

	PyObject *intermediate_uni = NULL;
	*c_str_ptr = NULL;
	if (PyString_Check(py_string)) {
		*c_str_ptr = PyString_AsString(py_string);
		return AEROSPIKE_OK;
	}
	else if (PyUnicode_Check(py_string)) {
		intermediate_uni = PyUnicode_AsUTF8String(py_string);
		if (!intermediate_uni) {
			return as_error_update(err, AEROSPIKE_ERR_PARAM,
								   "Invalid unicode value");
		}

		*c_str_ptr = PyBytes_AsString(intermediate_uni);
		*pyuni_r = intermediate_uni;
		return AEROSPIKE_OK;
	}
	return as_error_update(err, AEROSPIKE_ERR_PARAM, "String value required");
}

// This function converts a list of cdt_ctx from aerospike_helpers.ctx to
// an as_cdt_ctx object for use with the c-client. the cdt_ctx parameter should be an uninitialized as_cdt_ctx
// object. This function will initilaise it, and free it IF an error occurs, otherwise, the caller must destroy
// the as_cdt_ctx when it is done.
as_status get_cdt_ctx(AerospikeClient *self, as_error *err, as_cdt_ctx *cdt_ctx,
					  PyObject *op_dict, bool *ctx_in_use,
					  as_static_pool *static_pool, int serializer_type)
{
	PyObject *py_ctx = PyDict_GetItemString(op_dict, CTX_KEY);
	long int_val = 0;
	as_val *val = NULL;

	if (!py_ctx) {
		return AEROSPIKE_OK;
	}

	if (PyList_Check(py_ctx)) {
		Py_ssize_t py_list_size = PyList_Size(py_ctx);
		as_cdt_ctx_init(cdt_ctx, (int)py_list_size);

		for (int i = 0; i < py_list_size; i++) {
			PyObject *py_val = PyList_GetItem(py_ctx, (Py_ssize_t)i);

			PyObject *id_temp = PyObject_GetAttrString(py_val, "id");
			if (PyErr_Occurred()) {
				as_cdt_ctx_destroy(cdt_ctx);
				return as_error_update(err, AEROSPIKE_ERR_PARAM,
									   "Failed to convert %s, id", CTX_KEY);
			}

			PyObject *value_temp = PyObject_GetAttrString(py_val, "value");
			if (PyErr_Occurred()) {
				as_cdt_ctx_destroy(cdt_ctx);
				return as_error_update(err, AEROSPIKE_ERR_PARAM,
									   "Failed to convert %s, value", CTX_KEY);
			}

			PyObject *extra_args_temp =
				PyObject_GetAttrString(py_val, "extra_args");
			if (PyErr_Occurred()) {
				as_cdt_ctx_destroy(cdt_ctx);
				return as_error_update(err, AEROSPIKE_ERR_PARAM,
									   "Failed to convert %s", CTX_KEY);
			}

			uint64_t item_type = PyLong_AsUnsignedLong(id_temp);
			if (PyErr_Occurred()) {
				as_cdt_ctx_destroy(cdt_ctx);
				return as_error_update(err, AEROSPIKE_ERR_PARAM,
									   "Failed to convert %s, id to uint64_t",
									   CTX_KEY);
			}

			// add an as_cdt_ctx with value to cdt_ctx
			if (requires_int(item_type)) {
				int_val = PyLong_AsLong(value_temp);
				if (PyErr_Occurred()) {
					as_cdt_ctx_destroy(cdt_ctx);
					return as_error_update(
						err, AEROSPIKE_ERR_PARAM,
						"Failed to convert %s, value to long", CTX_KEY);
				}
				switch (item_type) {
				case AS_CDT_CTX_LIST_INDEX:
					as_cdt_ctx_add_list_index(cdt_ctx, int_val);
					break;
				case AS_CDT_CTX_LIST_RANK:
					as_cdt_ctx_add_list_rank(cdt_ctx, int_val);
					break;
				case AS_CDT_CTX_MAP_INDEX:
					as_cdt_ctx_add_map_index(cdt_ctx, int_val);
					break;
				case AS_CDT_CTX_MAP_RANK:
					as_cdt_ctx_add_map_rank(cdt_ctx, int_val);
					break;
				case CDT_CTX_LIST_INDEX_CREATE:;
					int list_order = 0;
					int pad = 0;
					get_int_from_py_dict(err, CDT_CTX_ORDER_KEY,
										 extra_args_temp, &list_order);
					get_int_from_py_dict(err, CDT_CTX_PAD_KEY, extra_args_temp,
										 &pad);
					as_cdt_ctx_add_list_index_create(cdt_ctx, int_val,
													 list_order, pad);
					break;
				default:
					as_cdt_ctx_destroy(cdt_ctx);
					return as_error_update(
						err, AEROSPIKE_ERR_PARAM,
						"Failed to convert, unknown ctx operation %s", CTX_KEY);
				}
			}
			else {
				if (pyobject_to_val(self, err, value_temp, &val, static_pool,
									serializer_type) != AEROSPIKE_OK) {
					return as_error_update(
						err, AEROSPIKE_ERR_PARAM,
						"Failed to convert %s, value to as_val", CTX_KEY);
				}
				switch (item_type) {
				case AS_CDT_CTX_LIST_VALUE:
					as_cdt_ctx_add_list_value(cdt_ctx, val);
					break;
				case AS_CDT_CTX_MAP_KEY:
					as_cdt_ctx_add_map_key(cdt_ctx, val);
					break;
				case AS_CDT_CTX_MAP_VALUE:
					as_cdt_ctx_add_map_value(cdt_ctx, val);
					break;
				case CDT_CTX_MAP_KEY_CREATE:;
					int map_order = 0;
					get_int_from_py_dict(err, CDT_CTX_ORDER_KEY,
										 extra_args_temp, &map_order);
					as_cdt_ctx_add_map_key_create(cdt_ctx, val, map_order);
					break;
				default:
					as_cdt_ctx_destroy(cdt_ctx);
					return as_error_update(
						err, AEROSPIKE_ERR_PARAM,
						"Failed to convert, unknown ctx operation %s", CTX_KEY);
				}
			}

			Py_DECREF(id_temp);
			Py_DECREF(value_temp);
		}
	}
	else {
		return as_error_update(err, AEROSPIKE_ERR_PARAM, "Failed to convert %s",
							   CTX_KEY);
	}

	*ctx_in_use = true;
	return AEROSPIKE_OK;
}

static bool requires_int(uint64_t op)
{
	return op == AS_CDT_CTX_LIST_INDEX || op == AS_CDT_CTX_LIST_RANK ||
		   op == AS_CDT_CTX_MAP_INDEX || op == AS_CDT_CTX_MAP_RANK ||
		   op == CDT_CTX_LIST_INDEX_CREATE;
}

/*
 * py_bool_to_py_bytes_blob serializes py_bool.
 * Target should be a NULL pointer to an as_integer. py_bool_to_py_bytes_blob will get memory for target
 * from the static pool, static_pool. The pool should be destroyed after use, by the caller.
 */
static as_status py_bool_to_py_bytes_blob(AerospikeClient *self, as_error *err,
										  as_static_pool *static_pool,
										  PyObject *py_bool, as_bytes **target,
										  int serializer_type)
{
	GET_BYTES_POOL(*target, static_pool, err);
	if (err->code != AEROSPIKE_OK) {
		return err->code;
	}

	if (serialize_based_on_serializer_policy(self, serializer_type, target,
											 py_bool, err) != AEROSPIKE_OK) {
		return err->code;
	}

	return AEROSPIKE_OK;
}

/*
 * py_bool_to_as_integer converts a python object to an as_integer based on its truth value.
 * Target should be a NULL pointer to an as_integer. py_bool_to_as_integer will allocate a new
 * as_integer on the heap and set target to point to it.
 * The caller is responsible for freeing target.
 */
static as_status py_bool_to_as_integer(as_error *err, PyObject *py_bool,
									   as_integer **target)
{
	int py_bool_as_num = PyObject_IsTrue(py_bool);
	if (py_bool_as_num == -1) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT,
							   "Failed to get truth value of py_bool.");
	}

	*target = as_integer_new((int64_t)py_bool_as_num);
	if (*target == NULL) {
		return as_error_update(
			err, AEROSPIKE_ERR_CLIENT,
			"Failed to create new as_integer from py_bool_as_num.");
	}

	return AEROSPIKE_OK;
}

/*
 * py_bool_to_as_bool converts a python object to an as_boolean based on its truth value.
 * Target should be a NULL pointer to an as_boolean. py_bool_to_as_bool will allocate a new
 * as_boolean on the heap and set target to point to it.
 * The caller is responsible for freeing target.
 */
static as_status py_bool_to_as_bool(as_error *err, PyObject *py_bool,
									as_boolean **target)
{
	int py_bool_as_num = PyObject_IsTrue(py_bool);
	if (py_bool_as_num == -1) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT,
							   "Failed to get truth value of py_bool.");
	}

	*target = as_boolean_new((bool)py_bool_as_num);
	if (*target == NULL) {
		return as_error_update(
			err, AEROSPIKE_ERR_CLIENT,
			"Failed to create new as_boolean from py_bool_as_num.");
	}

	return AEROSPIKE_OK;
}

/*
 * get_int_from_py_int assumes py_long is not NULL
 */
as_status get_int_from_py_int(as_error *err, PyObject *py_long,
							  int *int_pointer, const char *py_object_name)
{
	if (!PyLong_Check(py_long)) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM,
							   "%s must be an integer.", py_object_name);
	}

	int64_t int64_to_return = PyLong_AsLong(py_long);
	if (PyErr_Occurred()) {
		if (PyErr_ExceptionMatches(PyExc_OverflowError)) {
			return as_error_update(err, AEROSPIKE_ERR_PARAM,
								   "%s too large for C long.", py_object_name);
		}

		return as_error_update(err, AEROSPIKE_ERR_PARAM,
							   "Failed to convert %s.", py_object_name);
	}

	if (int64_to_return > INT_MAX || int64_to_return < INT_MIN) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM,
							   "%s too large for C int.", py_object_name);
	}

	*int_pointer = int64_to_return;

	return AEROSPIKE_OK;
}
