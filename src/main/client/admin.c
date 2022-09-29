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

#include <aerospike/aerospike.h>
#include <aerospike/as_address.h>
#include <aerospike/as_admin.h>
#include <aerospike/as_config.h>
#include <aerospike/as_error.h>
#include <aerospike/as_policy.h>

#include "admin.h"
#include "client.h"
#include "conversions.h"
#include "exceptions.h"
#include "policy.h"
#include "global_hosts.h"

/**
 *******************************************************************************************************
 * Create a user in the Aerospike DB.
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns an integer status. 0(Zero) is success value.
 * In case of error,appropriate exceptions will be raised.
 *******************************************************************************************************
 */
PyObject *AerospikeClient_Admin_Create_User(AerospikeClient *self,
											PyObject *args, PyObject *kwds)
{
	// Initialize error
	as_error err;
	as_error_init(&err);

	// Python Function Arguments
	PyObject *py_policy = NULL;
	PyObject *py_user = NULL;
	PyObject *py_password = NULL;
	PyObject *py_roles = NULL;

	as_policy_admin admin_policy;
	as_policy_admin *admin_policy_p = NULL;

	// Python Function Keyword Arguments
	static char *kwlist[] = {"user", "password", "roles", "policy", NULL};

	// Python Function Argument Parsing
	if (PyArg_ParseTupleAndKeywords(args, kwds, "OOO|O:admin_create_user",
									kwlist, &py_user, &py_password, &py_roles,
									&py_policy) == false) {
		return NULL;
	}

	// Aerospike Operation Arguments
	int roles_size = 0;
	char **roles = NULL;
	char *user = NULL, *password = NULL;

	if (!self || !self->as) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
		goto CLEANUP;
	}

	if (!self->is_conn_16) {
		as_error_update(&err, AEROSPIKE_ERR_CLUSTER,
						"No connection to aerospike cluster");
		goto CLEANUP;
	}

	// Convert python object to an array of roles
	if (PyList_Check(py_roles)) {
		roles_size = PyList_Size(py_roles);
		roles = alloca(sizeof(char *) * roles_size);
		for (int i = 0; i < roles_size; i++) {
			roles[i] = cf_malloc(sizeof(char) * AS_ROLE_SIZE);
			memset(roles[i], 0, sizeof(char) * AS_ROLE_SIZE);
		}
	}
	else {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Roles should be a list");
		goto CLEANUP;
	}

	pyobject_to_strArray(&err, py_roles, roles, AS_ROLE_SIZE);
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	// Convert python objects to username and password strings
	if (!PyUnicode_Check(py_user)) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM,
						"Username should be a string");
		goto CLEANUP;
	}

	user = PyString_AsString(py_user);

	if (!PyUnicode_Check(py_password)) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM,
						"Password should be a string");
		goto CLEANUP;
	}

	password = PyString_AsString(py_password);

	// Convert python object to policy_admin
	pyobject_to_policy_admin(self, &err, py_policy, &admin_policy,
							 &admin_policy_p, &self->as->config.policies.admin);
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	// Invoke operation
	Py_BEGIN_ALLOW_THREADS
	aerospike_create_user(self->as, &err, admin_policy_p, user, password,
						  (const char **)roles, roles_size);
	Py_END_ALLOW_THREADS
	if (err.code != AEROSPIKE_OK) {
		as_error_update(&err, err.code, NULL);
		goto CLEANUP;
	}

CLEANUP:
	for (int i = 0; i < roles_size; i++) {
		if (roles[i])
			cf_free(roles[i]);
	}

	if (err.code != AEROSPIKE_OK) {
		PyObject *py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyObject *exception_type = raise_exception(&err);
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return NULL;
	}

	return PyLong_FromLong(0);
}

/**
 *******************************************************************************************************
 * Drops a user from the Aerospike DB.
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns an integer status. 0(Zero) is success value.
 * In case of error,appropriate exceptions will be raised.
 *******************************************************************************************************
 */
PyObject *AerospikeClient_Admin_Drop_User(AerospikeClient *self, PyObject *args,
										  PyObject *kwds)
{
	// Initialize error
	as_error err;
	as_error_init(&err);

	// Python Function Arguments
	PyObject *py_policy = NULL;
	PyObject *py_user = NULL;

	as_policy_admin admin_policy;
	as_policy_admin *admin_policy_p = NULL;

	// Python Function Keyword Arguments
	static char *kwlist[] = {"user", "policy", NULL};

	// Python Function Argument Parsing
	if (PyArg_ParseTupleAndKeywords(args, kwds, "O|O:admin_drop_user", kwlist,
									&py_user, &py_policy) == false) {
		return NULL;
	}

	if (!self || !self->as) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
		goto CLEANUP;
	}

	if (!self->is_conn_16) {
		as_error_update(&err, AEROSPIKE_ERR_CLUSTER,
						"No connection to aerospike cluster");
		goto CLEANUP;
	}

	// Aerospike Operation Arguments
	char *user = NULL;

	// Convert python object to policy_admin
	pyobject_to_policy_admin(self, &err, py_policy, &admin_policy,
							 &admin_policy_p, &self->as->config.policies.admin);
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	// Convert python object to username string
	if (!PyUnicode_Check(py_user)) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM,
						"Username should be a string");
		goto CLEANUP;
	}

	user = PyString_AsString(py_user);

	//Invoke operation
	Py_BEGIN_ALLOW_THREADS
	aerospike_drop_user(self->as, &err, admin_policy_p, user);
	Py_END_ALLOW_THREADS

	char *alias_to_search = NULL;
	alias_to_search = return_search_string(self->as);
	PyObject *py_persistent_item = NULL;

	py_persistent_item = PyDict_GetItemString(py_global_hosts, alias_to_search);
	if (py_persistent_item) {
		PyDict_DelItemString(py_global_hosts, alias_to_search);
		AerospikeGlobalHosts_Del(py_persistent_item);
	}
	PyMem_Free(alias_to_search);
	alias_to_search = NULL;

	if (err.code != AEROSPIKE_OK) {
		as_error_update(&err, err.code, NULL);
		goto CLEANUP;
	}

CLEANUP:

	if (err.code != AEROSPIKE_OK) {
		PyObject *py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyObject *exception_type = raise_exception(&err);
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return NULL;
	}

	return PyLong_FromLong(0);
}

/**
 *******************************************************************************************************
 * Sets the password of a particular user in the Aerospike DB.
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns an integer status. 0(Zero) is success value.
 * In case of error,appropriate exceptions will be raised.
 *******************************************************************************************************
 */
PyObject *AerospikeClient_Admin_Set_Password(AerospikeClient *self,
											 PyObject *args, PyObject *kwds)
{
	// Initialize error
	as_error err;
	as_error_init(&err);

	// Python Function Arguments
	PyObject *py_policy = NULL;
	PyObject *py_user = NULL;
	PyObject *py_password = NULL;

	as_policy_admin admin_policy;
	as_policy_admin *admin_policy_p = NULL;

	// Python Function Keyword Arguments
	static char *kwlist[] = {"user", "password", "policy", NULL};

	// Python Function Argument Parsing
	if (PyArg_ParseTupleAndKeywords(args, kwds, "OO|O:admin_set_password",
									kwlist, &py_user, &py_password,
									&py_policy) == false) {
		return NULL;
	}

	if (!self || !self->as) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
		goto CLEANUP;
	}

	if (!self->is_conn_16) {
		as_error_update(&err, AEROSPIKE_ERR_CLUSTER,
						"No connection to aerospike cluster");
		goto CLEANUP;
	}

	// Aerospike Operation Arguments
	char *user = NULL, *password = NULL;

	// Convert python object to policy_admin
	pyobject_to_policy_admin(self, &err, py_policy, &admin_policy,
							 &admin_policy_p, &self->as->config.policies.admin);
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	// Convert python objects into username and password strings
	if (!PyUnicode_Check(py_user)) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM,
						"Username should be a string");
		goto CLEANUP;
	}

	user = PyString_AsString(py_user);

	if (!PyUnicode_Check(py_password)) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM,
						"Password should be a string");
		goto CLEANUP;
	}

	password = PyString_AsString(py_password);

	// Invoke operation
	Py_BEGIN_ALLOW_THREADS
	aerospike_set_password(self->as, &err, admin_policy_p, user, password);
	Py_END_ALLOW_THREADS
	if (err.code != AEROSPIKE_OK) {
		as_error_update(&err, err.code, NULL);
		goto CLEANUP;
	}

CLEANUP:

	if (err.code != AEROSPIKE_OK) {
		PyObject *py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyObject *exception_type = raise_exception(&err);
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return NULL;
	}

	return PyLong_FromLong(0);
}

/**
 *******************************************************************************************************
 * Changes the password of a particular user in the Aerospike DB.
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns an integer status. 0(Zero) is success value.
 * In case of error,appropriate exceptions will be raised.
 *******************************************************************************************************
 */
PyObject *AerospikeClient_Admin_Change_Password(AerospikeClient *self,
												PyObject *args, PyObject *kwds)
{
	// Initialize error
	as_error err;
	as_error_init(&err);

	// Python Function Arguments
	PyObject *py_policy = NULL;
	PyObject *py_user = NULL;
	PyObject *py_password = NULL;

	as_policy_admin admin_policy;
	as_policy_admin *admin_policy_p = NULL;

	// Python Function Keyword Arguments
	static char *kwlist[] = {"user", "password", "policy", NULL};

	// Python Function Argument Parsing
	if (PyArg_ParseTupleAndKeywords(args, kwds, "OO|O:admin_change_password",
									kwlist, &py_user, &py_password,
									&py_policy) == false) {
		return NULL;
	}

	if (!self || !self->as) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
		goto CLEANUP;
	}

	if (!self->is_conn_16) {
		as_error_update(&err, AEROSPIKE_ERR_CLUSTER,
						"No connection to aerospike cluster");
		goto CLEANUP;
	}

	// Aerospike Operation Arguments
	char *user = NULL, *password = NULL;

	// Convert python object to policy_admin
	pyobject_to_policy_admin(self, &err, py_policy, &admin_policy,
							 &admin_policy_p, &self->as->config.policies.admin);
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	// Convert python objects into username and password strings
	if (!PyUnicode_Check(py_user)) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM,
						"Username should be a string");
		goto CLEANUP;
	}

	user = PyString_AsString(py_user);

	if (!PyUnicode_Check(py_password)) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM,
						"Password should be a string");
		goto CLEANUP;
	}

	password = PyString_AsString(py_password);

	// Invoke operation
	Py_BEGIN_ALLOW_THREADS
	aerospike_change_password(self->as, &err, admin_policy_p, user, password);
	Py_END_ALLOW_THREADS

	char *alias_to_search = NULL;
	alias_to_search = return_search_string(self->as);
	PyObject *py_persistent_item = NULL;

	py_persistent_item = PyDict_GetItemString(py_global_hosts, alias_to_search);
	if (py_persistent_item) {
		PyDict_DelItemString(py_global_hosts, alias_to_search);
		AerospikeGlobalHosts_Del(py_persistent_item);
	}
	PyMem_Free(alias_to_search);
	alias_to_search = NULL;

	if (err.code != AEROSPIKE_OK) {
		as_error_update(&err, err.code, NULL);
		goto CLEANUP;
	}

CLEANUP:

	if (err.code != AEROSPIKE_OK) {
		PyObject *py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyObject *exception_type = raise_exception(&err);
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return NULL;
	}

	return PyLong_FromLong(0);
}

/**
 *******************************************************************************************************
 * Grants a role to a user in the Aerospike DB.
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns an integer status. 0(Zero) is success value.
 * In case of error,appropriate exceptions will be raised.
 *******************************************************************************************************
 */
PyObject *AerospikeClient_Admin_Grant_Roles(AerospikeClient *self,
											PyObject *args, PyObject *kwds)
{
	// Initialize error
	as_error err;
	as_error_init(&err);

	// Python Function Arguments
	PyObject *py_policy = NULL;
	PyObject *py_user = NULL;
	PyObject *py_roles = NULL;

	as_policy_admin admin_policy;
	as_policy_admin *admin_policy_p = NULL;

	// Python Function Keyword Arguments
	static char *kwlist[] = {"user", "roles", "policy", NULL};

	// Python Function Argument Parsing
	if (PyArg_ParseTupleAndKeywords(args, kwds, "OO|O:admin_grant_roles",
									kwlist, &py_user, &py_roles,
									&py_policy) == false) {
		return NULL;
	}

	// Aerospike Operation Arguments
	int roles_size = 0;
	char **roles = NULL;
	char *user = NULL;

	if (!self || !self->as) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
		goto CLEANUP;
	}

	if (!self->is_conn_16) {
		as_error_update(&err, AEROSPIKE_ERR_CLUSTER,
						"No connection to aerospike cluster");
		goto CLEANUP;
	}

	// Convert python object to array of roles
	if (PyList_Check(py_roles)) {
		roles_size = PyList_Size(py_roles);
		roles = alloca(sizeof(char *) * roles_size);
		for (int i = 0; i < roles_size; i++) {
			roles[i] = cf_malloc(sizeof(char) * AS_ROLE_SIZE);
			memset(roles[i], 0, sizeof(char) * AS_ROLE_SIZE);
		}
	}

	pyobject_to_strArray(&err, py_roles, roles, AS_ROLE_SIZE);
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	// Convert python object into username string
	if (!PyUnicode_Check(py_user)) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM,
						"Username should be a string");
		goto CLEANUP;
	}

	user = PyString_AsString(py_user);

	// Convert python object to policy_admin
	pyobject_to_policy_admin(self, &err, py_policy, &admin_policy,
							 &admin_policy_p, &self->as->config.policies.admin);
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	// Invoke operation
	Py_BEGIN_ALLOW_THREADS
	aerospike_grant_roles(self->as, &err, admin_policy_p, user,
						  (const char **)roles, roles_size);
	Py_END_ALLOW_THREADS
	if (err.code != AEROSPIKE_OK) {
		as_error_update(&err, err.code, NULL);
		goto CLEANUP;
	}

CLEANUP:
	for (int i = 0; i < roles_size; i++) {
		if (roles[i])
			cf_free(roles[i]);
	}

	if (err.code != AEROSPIKE_OK) {
		PyObject *py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyObject *exception_type = raise_exception(&err);
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return NULL;
	}

	return PyLong_FromLong(0);
}

/**
 *******************************************************************************************************
 * Revokes roles of a user in the Aerospike DB.
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns an integer status. 0(Zero) is success value.
 * In case of error,appropriate exceptions will be raised.
 *******************************************************************************************************
 */
PyObject *AerospikeClient_Admin_Revoke_Roles(AerospikeClient *self,
											 PyObject *args, PyObject *kwds)
{
	// Initialize error
	as_error err;
	as_error_init(&err);

	// Python Function Arguments
	PyObject *py_policy = NULL;
	PyObject *py_user = NULL;
	PyObject *py_roles = NULL;

	as_policy_admin admin_policy;
	as_policy_admin *admin_policy_p = NULL;

	// Python Function Keyword Arguments
	static char *kwlist[] = {"user", "roles", "policy", NULL};

	// Python Function Argument Parsing
	if (PyArg_ParseTupleAndKeywords(args, kwds, "OO|O:admin_revoke_roles",
									kwlist, &py_user, &py_roles,
									&py_policy) == false) {
		return NULL;
	}

	// Aerospike Operation Arguments
	char *user = NULL;
	int roles_size = 0;
	char **roles = NULL;

	if (!self || !self->as) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
		goto CLEANUP;
	}

	if (!self->is_conn_16) {
		as_error_update(&err, AEROSPIKE_ERR_CLUSTER,
						"No connection to aerospike cluster");
		goto CLEANUP;
	}

	// Convert python object to array of roles
	if (PyList_Check(py_roles)) {
		roles_size = PyList_Size(py_roles);
		roles = alloca(sizeof(char *) * roles_size);
		for (int i = 0; i < roles_size; i++) {
			roles[i] = cf_malloc(sizeof(char) * AS_ROLE_SIZE);
			memset(roles[i], 0, sizeof(char) * AS_ROLE_SIZE);
		}
	}

	pyobject_to_strArray(&err, py_roles, roles, AS_ROLE_SIZE);
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	if (py_policy == Py_None) {
		py_policy = PyDict_New();
	}

	// Convert python object to username string
	if (!PyUnicode_Check(py_user)) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM,
						"Username should be a string");
		goto CLEANUP;
	}

	user = PyString_AsString(py_user);

	// Convert python object to policy_admin
	pyobject_to_policy_admin(self, &err, py_policy, &admin_policy,
							 &admin_policy_p, &self->as->config.policies.admin);
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	// Invoke operation
	Py_BEGIN_ALLOW_THREADS
	aerospike_revoke_roles(self->as, &err, admin_policy_p, user,
						   (const char **)roles, roles_size);
	Py_END_ALLOW_THREADS
	if (err.code != AEROSPIKE_OK) {
		as_error_update(&err, err.code, NULL);
		goto CLEANUP;
	}

CLEANUP:
	for (int i = 0; i < roles_size; i++) {
		if (roles[i])
			cf_free(roles[i]);
	}

	if (err.code != AEROSPIKE_OK) {
		PyObject *py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyObject *exception_type = raise_exception(&err);
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return NULL;
	}

	return PyLong_FromLong(0);
}

/**
 *******************************************************************************************************
 * Queries a user in the Aerospike DB.
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns an integer status. 0(Zero) is success value.
 * In case of error,appropriate exceptions will be raised.
 *******************************************************************************************************
 */
PyObject *AerospikeClient_Admin_Query_User(AerospikeClient *self,
										   PyObject *args, PyObject *kwds)
{
	// Initialize error
	as_error err;
	as_error_init(&err);

	// Python Function Arguments
	PyObject *py_policy = NULL;
	PyObject *py_user_name = NULL;

	// Python Function Result
	PyObject *py_user = NULL;

	as_policy_admin admin_policy;
	as_policy_admin *admin_policy_p = NULL;

	// Python Function Keyword Arguments
	static char *kwlist[] = {"user", "policy", NULL};

	// Python Function Argument Parsing
	if (PyArg_ParseTupleAndKeywords(args, kwds, "O|O:admin_query_user", kwlist,
									&py_user_name, &py_policy) == false) {
		return NULL;
	}

	// Aerospike Operation Arguments
	char *user_name = NULL;
	as_user *user = NULL;

	if (!self || !self->as) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
		goto CLEANUP;
	}

	if (!self->is_conn_16) {
		as_error_update(&err, AEROSPIKE_ERR_CLUSTER,
						"No connection to aerospike cluster");
		goto CLEANUP;
	}

	// Convert python object to policy_admin
	pyobject_to_policy_admin(self, &err, py_policy, &admin_policy,
							 &admin_policy_p, &self->as->config.policies.admin);
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	// Convert python object to username string
	if (!PyUnicode_Check(py_user_name)) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM,
						"Username should be a string");
		goto CLEANUP;
	}

	user_name = PyString_AsString(py_user_name);

	// Invoke operation
	Py_BEGIN_ALLOW_THREADS
	aerospike_query_user(self->as, &err, admin_policy_p, user_name, &user);
	Py_END_ALLOW_THREADS
	if (err.code != AEROSPIKE_OK) {
		as_error_update(&err, err.code, NULL);
		goto CLEANUP;
	}

	// Convert returned as_user struct to python object
	as_user_to_pyobject(&err, user, &py_user);
	if (err.code != AEROSPIKE_OK) {
		as_error_update(&err, err.code, NULL);
		goto CLEANUP;
	}

CLEANUP:
	if (user) {
		as_user_destroy(user);
	}

	if (err.code != AEROSPIKE_OK) {
		PyObject *py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyObject *exception_type = raise_exception(&err);
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return NULL;
	}

	return py_user;
}

/**
 *******************************************************************************************************
 * Queries a user's info in the Aerospike DB.
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns an integer status. 0(Zero) is success value.
 * In case of error,appropriate exceptions will be raised.
 *******************************************************************************************************
 */
PyObject *AerospikeClient_Admin_Query_User_Info(AerospikeClient *self,
												PyObject *args, PyObject *kwds)
{
	// Initialize error
	as_error err;
	as_error_init(&err);

	// Python Function Arguments
	PyObject *py_policy = NULL;
	PyObject *py_user_name = NULL;

	// Python Function Result
	PyObject *py_user = NULL;

	as_policy_admin admin_policy;
	as_policy_admin *admin_policy_p = NULL;

	// Python Function Keyword Arguments
	static char *kwlist[] = {"user", "policy", NULL};

	// Python Function Argument Parsing
	if (PyArg_ParseTupleAndKeywords(args, kwds, "O|O:admin_query_user_info",
									kwlist, &py_user_name,
									&py_policy) == false) {
		return NULL;
	}

	// Aerospike Operation Arguments
	char *user_name = NULL;
	as_user *user = NULL;

	if (!self || !self->as) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
		goto CLEANUP;
	}

	if (!self->is_conn_16) {
		as_error_update(&err, AEROSPIKE_ERR_CLUSTER,
						"No connection to aerospike cluster");
		goto CLEANUP;
	}

	// Convert python object to policy_admin
	pyobject_to_policy_admin(self, &err, py_policy, &admin_policy,
							 &admin_policy_p, &self->as->config.policies.admin);
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	// Convert python object to username string
	if (!PyUnicode_Check(py_user_name)) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM,
						"Username should be a string");
		goto CLEANUP;
	}

	user_name = PyString_AsString(py_user_name);

	// Invoke operation
	Py_BEGIN_ALLOW_THREADS
	aerospike_query_user(self->as, &err, admin_policy_p, user_name, &user);
	Py_END_ALLOW_THREADS
	if (err.code != AEROSPIKE_OK) {
		as_error_update(&err, err.code, NULL);
		goto CLEANUP;
	}

	// Convert returned as_user struct to python object
	as_user_info_to_pyobject(&err, user, &py_user);
	if (err.code != AEROSPIKE_OK) {
		as_error_update(&err, err.code, NULL);
		goto CLEANUP;
	}

CLEANUP:
	if (user) {
		as_user_destroy(user);
	}

	if (err.code != AEROSPIKE_OK) {
		PyObject *py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyObject *exception_type = raise_exception(&err);
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return NULL;
	}

	return py_user;
}

/**
 *******************************************************************************************************
 * Queries all users in the Aerospike DB.
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns an integer status. 0(Zero) is success value.
 * In case of error,appropriate exceptions will be raised.
 *******************************************************************************************************
 */
PyObject *AerospikeClient_Admin_Query_Users(AerospikeClient *self,
											PyObject *args, PyObject *kwds)
{
	// Initialize error
	as_error err;
	as_error_init(&err);

	// Python Function Arguments
	PyObject *py_policy = NULL;

	// Python Function Result
	PyObject *py_users = NULL;

	as_policy_admin admin_policy;
	as_policy_admin *admin_policy_p = NULL;

	// Python Function Keyword Arguments
	static char *kwlist[] = {"policy", NULL};

	// Python Function Argument Parsing
	if (PyArg_ParseTupleAndKeywords(args, kwds, "|O:admin_query_users", kwlist,
									&py_policy) == false) {
		return NULL;
	}

	// Aerospike Operation Arguments
	int users_size = 0;
	as_user **users = NULL;

	if (!self || !self->as) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
		goto CLEANUP;
	}

	if (!self->is_conn_16) {
		as_error_update(&err, AEROSPIKE_ERR_CLUSTER,
						"No connection to aerospike cluster");
		goto CLEANUP;
	}

	// Convert python object to policy_admin
	pyobject_to_policy_admin(self, &err, py_policy, &admin_policy,
							 &admin_policy_p, &self->as->config.policies.admin);
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	// Invoke operation
	Py_BEGIN_ALLOW_THREADS
	aerospike_query_users(self->as, &err, admin_policy_p, &users, &users_size);
	Py_END_ALLOW_THREADS
	if (err.code != AEROSPIKE_OK) {
		as_error_update(&err, err.code, NULL);
		goto CLEANUP;
	}

	// Convert returned array of as_user structs into python object;
	as_user_array_to_pyobject(&err, users, &py_users, users_size);
	if (err.code != AEROSPIKE_OK) {
		as_error_update(&err, err.code, NULL);
		goto CLEANUP;
	}

CLEANUP:
	if (users) {
		as_users_destroy(users, users_size);
	}

	if (err.code != AEROSPIKE_OK) {
		PyObject *py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyObject *exception_type = raise_exception(&err);
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return NULL;
	}

	return py_users;
}

/**
 *******************************************************************************************************
 * Queries all users info in the Aerospike DB.
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns an integer status. 0(Zero) is success value.
 * In case of error,appropriate exceptions will be raised.
 *******************************************************************************************************
 */
PyObject *AerospikeClient_Admin_Query_Users_Info(AerospikeClient *self,
												 PyObject *args, PyObject *kwds)
{
	// Initialize error
	as_error err;
	as_error_init(&err);

	// Python Function Arguments
	PyObject *py_policy = NULL;

	// Python Function Result
	PyObject *py_users = NULL;

	as_policy_admin admin_policy;
	as_policy_admin *admin_policy_p = NULL;

	// Python Function Keyword Arguments
	static char *kwlist[] = {"policy", NULL};

	// Python Function Argument Parsing
	if (PyArg_ParseTupleAndKeywords(args, kwds, "|O:admin_query_users_info",
									kwlist, &py_policy) == false) {
		return NULL;
	}

	// Aerospike Operation Arguments
	int users_size = 0;
	as_user **users = NULL;

	if (!self || !self->as) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
		goto CLEANUP;
	}

	if (!self->is_conn_16) {
		as_error_update(&err, AEROSPIKE_ERR_CLUSTER,
						"No connection to aerospike cluster");
		goto CLEANUP;
	}

	// Convert python object to policy_admin
	pyobject_to_policy_admin(self, &err, py_policy, &admin_policy,
							 &admin_policy_p, &self->as->config.policies.admin);
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	// Invoke operation
	Py_BEGIN_ALLOW_THREADS
	aerospike_query_users(self->as, &err, admin_policy_p, &users, &users_size);
	Py_END_ALLOW_THREADS
	if (err.code != AEROSPIKE_OK) {
		as_error_update(&err, err.code, NULL);
		goto CLEANUP;
	}

	// Convert returned array of as_user structs into python object;
	as_user_info_array_to_pyobject(&err, users, &py_users, users_size);
	if (err.code != AEROSPIKE_OK) {
		as_error_update(&err, err.code, NULL);
		goto CLEANUP;
	}

CLEANUP:
	if (users) {
		as_users_destroy(users, users_size);
	}

	if (err.code != AEROSPIKE_OK) {
		PyObject *py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyObject *exception_type = raise_exception(&err);
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return NULL;
	}

	return py_users;
}

/**
 *******************************************************************************************************
 * Create a role in the Aerospike DB.
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns an integer status. 0(Zero) is success value.
 * In case of error,appropriate exceptions will be raised.
 *******************************************************************************************************
 */
PyObject *AerospikeClient_Admin_Create_Role(AerospikeClient *self,
											PyObject *args, PyObject *kwds)
{
	// Initialize error.
	as_error err;
	as_error_init(&err);

	// Python Function Arguments.
	PyObject *py_role = NULL;
	PyObject *py_privileges = NULL;
	PyObject *py_policy = NULL;
	PyObject *py_whitelist = NULL;
	PyObject *py_read_quota = NULL;
	PyObject *py_write_quota = NULL;

	// Aerospike Operation Arguments.
	int privileges_size = 0;
	int whitelist_size = 0;
	as_privilege **privileges = NULL;
	char **whitelist = NULL;

	// Python Function Keyword Arguments.
	static char *kwlist[] = {"role",	   "privileges",  "policy", "whitelist",
							 "read_quota", "write_quota", NULL};

	// Python Function Argument Parsing.
	if (PyArg_ParseTupleAndKeywords(args, kwds, "O|OOOOO:admin_create_role",
									kwlist, &py_role, &py_privileges,
									&py_policy, &py_whitelist, &py_read_quota,
									&py_write_quota) == false) {
		return NULL;
	}

	// sanity connection checks.
	if (!self || !self->as) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
		goto CLEANUP;
	}

	if (!self->is_conn_16) {
		as_error_update(&err, AEROSPIKE_ERR_CLUSTER,
						"No connection to aerospike cluster");
		goto CLEANUP;
	}

	const char *role = NULL;
	if (PyUnicode_Check(py_role)) {
		role = PyUnicode_AsUTF8(py_role);
	}
	else {
		as_error_update(&err, AEROSPIKE_ERR_PARAM,
						"Role name should be a string");
		goto CLEANUP;
	}

	// Convert python object to an array of privileges.
	if (py_privileges != NULL) {
		if (!PyList_Check(py_privileges)) {
			as_error_update(&err, AEROSPIKE_ERR_PARAM,
							"Privileges should be a list");
			goto CLEANUP;
		}

		privileges_size = PyList_Size(py_privileges);
		privileges = (as_privilege **)cf_malloc(sizeof(as_privilege *) *
												privileges_size);
		for (int i = 0; i < privileges_size; ++i) {
			privileges[i] = (as_privilege *)cf_malloc(sizeof(as_privilege));
		}

		if (pyobject_to_as_privileges(&err, py_privileges, privileges,
									  privileges_size) != AEROSPIKE_OK) {
			goto CLEANUP;
		}
	}

	// Convert python object to an admin policy.
	as_policy_admin admin_policy;
	as_policy_admin *admin_policy_p = NULL;
	if (pyobject_to_policy_admin(
			self, &err, py_policy, &admin_policy, &admin_policy_p,
			&self->as->config.policies.admin) != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	if (py_whitelist != NULL) {
		if (!PyList_Check(py_whitelist)) {
			as_error_update(&err, AEROSPIKE_ERR_PARAM,
							"Whitelist must be a list of IP strings.");
			goto CLEANUP;
		}

		whitelist_size = PyList_Size(py_whitelist);
		whitelist = (char **)cf_malloc(sizeof(char *) * whitelist_size);
		for (int i = 0; i < whitelist_size; i++) {
			whitelist[i] = cf_malloc(sizeof(char) * AS_IP_ADDRESS_SIZE);
		}

		if (pyobject_to_strArray(&err, py_whitelist, whitelist,
								 AS_IP_ADDRESS_SIZE) != AEROSPIKE_OK) {
			goto CLEANUP;
		}
	}

	int read_quota = 0;
	if (py_read_quota != NULL) {
		get_int_from_py_int(&err, py_read_quota, &read_quota, "py_read_quota");
	}

	int write_quota = 0;
	if (py_write_quota != NULL) {
		get_int_from_py_int(&err, py_write_quota, &write_quota,
							"py_write_quota");
	}

	Py_BEGIN_ALLOW_THREADS
	aerospike_create_role_quotas(
		self->as, &err, admin_policy_p, role, privileges, privileges_size,
		(const char **)whitelist, whitelist_size, read_quota, write_quota);
	Py_END_ALLOW_THREADS

CLEANUP:
	if (privileges) {
		for (int i = 0; i < privileges_size; i++) {
			if (privileges[i])
				cf_free(privileges[i]);
		}

		cf_free(privileges);
	}

	if (whitelist) {
		for (int i = 0; i < whitelist_size; i++) {
			if (whitelist[i])
				cf_free(whitelist[i]);
		}

		cf_free(whitelist);
	}

	if (err.code != AEROSPIKE_OK) {
		PyObject *py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyObject *exception_type = raise_exception(&err);
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return NULL;
	}

	return PyLong_FromLong(0);
}

/**
 *******************************************************************************************************
 * Add whitelist to a role in the Aerospike DB.
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns an integer status. 0(Zero) is success value.
 * In case of error,appropriate exceptions will be raised.
 *******************************************************************************************************
 */
PyObject *AerospikeClient_Admin_Set_Whitelist(AerospikeClient *self,
											  PyObject *args, PyObject *kwds)
{
	// Initialize error.
	as_error err;
	as_error_init(&err);

	// Python Function Arguments.
	PyObject *py_role = NULL;
	PyObject *py_whitelist = NULL;
	PyObject *py_policy = NULL;

	// C API args.
	char **whitelist = NULL;

	// Sanity connection checks.
	if (!self || !self->as) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object.");
		goto CLEANUP;
	}

	if (!self->is_conn_16) {
		as_error_update(&err, AEROSPIKE_ERR_CLUSTER,
						"No connection to aerospike cluster.");
		goto CLEANUP;
	}

	// Python Function Keyword Arguments.
	static char *kwlist[] = {"role", "whitelist", "policy", NULL};

	// Python Function Argument Parsing.
	if (PyArg_ParseTupleAndKeywords(args, kwds, "OO|O:admin_set_whitelist",
									kwlist, &py_role, &py_whitelist,
									&py_policy) == false) {
		return NULL;
	}

	const char *role = NULL;
	if (PyUnicode_Check(py_role)) {
		role = PyUnicode_AsUTF8(py_role);
	}
	else {
		as_error_update(&err, AEROSPIKE_ERR_PARAM,
						"Role name should be a string.");
		goto CLEANUP;
	}

	int whitelist_size = 0;
	if (PyList_Check(py_whitelist)) {
		whitelist_size = PyList_Size(py_whitelist);

		whitelist = (char **)cf_malloc(sizeof(char *) * whitelist_size);
		for (int i = 0; i < whitelist_size; i++) {
			whitelist[i] = cf_malloc(sizeof(char) * AS_IP_ADDRESS_SIZE);
		}

		if (pyobject_to_strArray(&err, py_whitelist, whitelist,
								 AS_IP_ADDRESS_SIZE) != AEROSPIKE_OK) {
			goto CLEANUP;
		}
	}
	else if (py_whitelist == Py_None) {
		whitelist_size = 0;
	}
	else {
		as_error_update(&err, AEROSPIKE_ERR_PARAM,
						"Whitelist must be a list of IP strings, or None.");
		goto CLEANUP;
	}

	as_policy_admin admin_policy;
	as_policy_admin *admin_policy_p = NULL;
	pyobject_to_policy_admin(self, &err, py_policy, &admin_policy,
							 &admin_policy_p, &self->as->config.policies.admin);
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	// Invoke operation.
	Py_BEGIN_ALLOW_THREADS
	aerospike_set_whitelist(self->as, &err, admin_policy_p, role,
							(const char **)whitelist, whitelist_size);
	Py_END_ALLOW_THREADS

CLEANUP:
	if (whitelist != NULL) {
		for (int i = 0; i < whitelist_size; i++) {
			if (whitelist[i])
				cf_free(whitelist[i]);
		}

		cf_free(whitelist);
	}

	if (err.code != AEROSPIKE_OK) {
		PyObject *py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyObject *exception_type = raise_exception(&err);
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return NULL;
	}

	return PyLong_FromLong(0);
}

/**
 *******************************************************************************************************
 * Add quotas to a role in the Aerospike DB.
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns an integer status. 0(Zero) is success value.
 * In case of error,appropriate exceptions will be raised.
 *******************************************************************************************************
 */
PyObject *AerospikeClient_Admin_Set_Quotas(AerospikeClient *self,
										   PyObject *args, PyObject *kwds)
{
	// Initialize error.
	as_error err;
	as_error_init(&err);

	// Python Function Arguments.
	PyObject *py_role = NULL;
	PyObject *py_read_quota = NULL;
	PyObject *py_write_quota = NULL;
	PyObject *py_policy = NULL;

	// Sanity connection checks.
	if (!self || !self->as) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object.");
		goto CLEANUP;
	}

	if (!self->is_conn_16) {
		as_error_update(&err, AEROSPIKE_ERR_CLUSTER,
						"No connection to aerospike cluster.");
		goto CLEANUP;
	}

	// Python Function Keyword Arguments.
	static char *kwlist[] = {"role", "read_quota", "write_quota", "policy",
							 NULL};

	// Python Function Argument Parsing.
	if (PyArg_ParseTupleAndKeywords(args, kwds, "O|OOO:admin_set_quotas",
									kwlist, &py_role, &py_read_quota,
									&py_write_quota, &py_policy) == false) {
		return NULL;
	}

	const char *role = NULL;
	if (PyUnicode_Check(py_role)) {
		role = PyString_AsString(py_role);
	}
	else {
		as_error_update(&err, AEROSPIKE_ERR_PARAM,
						"Role name should be a string.");
		goto CLEANUP;
	}

	int read_quota = -1;
	if (py_read_quota != NULL) {
		get_int_from_py_int(&err, py_read_quota, &read_quota, "py_read_quota");
	}

	int write_quota = -1;
	if (py_write_quota != NULL) {
		get_int_from_py_int(&err, py_write_quota, &write_quota,
							"py_write_quota");
	}

	as_policy_admin admin_policy;
	as_policy_admin *admin_policy_p = NULL;
	pyobject_to_policy_admin(self, &err, py_policy, &admin_policy,
							 &admin_policy_p, &self->as->config.policies.admin);
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	// Invoke operation.
	Py_BEGIN_ALLOW_THREADS
	aerospike_set_quotas(self->as, &err, admin_policy_p, role, read_quota,
						 write_quota);
	Py_END_ALLOW_THREADS

CLEANUP:
	if (err.code != AEROSPIKE_OK) {
		PyObject *py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyObject *exception_type = raise_exception(&err);
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return NULL;
	}

	return PyLong_FromLong(0);
}

/**
 *******************************************************************************************************
 * Drop a role in the Aerospike DB.
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns an integer status. 0(Zero) is success value.
 * In case of error,appropriate exceptions will be raised.
 *******************************************************************************************************
 */
PyObject *AerospikeClient_Admin_Drop_Role(AerospikeClient *self, PyObject *args,
										  PyObject *kwds)
{
	// Initialize error
	as_error err;
	as_error_init(&err);

	// Python Function Arguments
	PyObject *py_policy = NULL;
	PyObject *py_role = NULL;

	as_policy_admin admin_policy;
	as_policy_admin *admin_policy_p = NULL;

	// Python Function Keyword Arguments
	static char *kwlist[] = {"role", "policy", NULL};

	// Python Function Argument Parsing
	if (PyArg_ParseTupleAndKeywords(args, kwds, "O|O:admin_drop_role", kwlist,
									&py_role, &py_policy) == false) {
		return NULL;
	}

	if (!self || !self->as) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
		goto CLEANUP;
	}

	if (!self->is_conn_16) {
		as_error_update(&err, AEROSPIKE_ERR_CLUSTER,
						"No connection to aerospike cluster");
		goto CLEANUP;
	}

	pyobject_to_policy_admin(self, &err, py_policy, &admin_policy,
							 &admin_policy_p, &self->as->config.policies.admin);
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	char *role = NULL;
	if (PyUnicode_Check(py_role)) {
		role = PyString_AsString(py_role);
	}
	else {
		as_error_update(&err, AEROSPIKE_ERR_PARAM,
						"Role name should be a string");
		goto CLEANUP;
	}

	// Invoke operation
	Py_BEGIN_ALLOW_THREADS
	aerospike_drop_role(self->as, &err, admin_policy_p, role);
	Py_END_ALLOW_THREADS

CLEANUP:
	if (err.code != AEROSPIKE_OK) {
		PyObject *py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyObject *exception_type = raise_exception(&err);
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return NULL;
	}

	return PyLong_FromLong(0);
}

/**
 *******************************************************************************************************
 * Add privileges to a role in the Aerospike DB.
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns an integer status. 0(Zero) is success value.
 * In case of error,appropriate exceptions will be raised.
 *******************************************************************************************************
 */
PyObject *AerospikeClient_Admin_Grant_Privileges(AerospikeClient *self,
												 PyObject *args, PyObject *kwds)
{
	// Initialize error
	as_error err;
	as_error_init(&err);

	// Python Function Arguments
	PyObject *py_policy = NULL;
	PyObject *py_role = NULL;
	PyObject *py_privileges = NULL;

	as_policy_admin admin_policy;
	as_policy_admin *admin_policy_p = NULL;

	// Python Function Keyword Arguments
	static char *kwlist[] = {"role", "privileges", "policy", NULL};

	// Python Function Argument Parsing
	if (PyArg_ParseTupleAndKeywords(args, kwds, "OO|O:admin_grant_privileges",
									kwlist, &py_role, &py_privileges,
									&py_policy) == false) {
		return NULL;
	}

	// Aerospike Operation Arguments
	int privileges_size = 0;
	as_privilege **privileges = NULL;

	if (!self || !self->as) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
		goto CLEANUP;
	}

	if (!self->is_conn_16) {
		as_error_update(&err, AEROSPIKE_ERR_CLUSTER,
						"No connection to aerospike cluster");
		goto CLEANUP;
	}

	// Convert python object to an array of privileges
	if (!PyList_Check(py_privileges)) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM,
						"Privileges should be a list");
		goto CLEANUP;
	}

	privileges_size = PyList_Size(py_privileges);
	privileges =
		(as_privilege **)cf_malloc(sizeof(as_privilege *) * privileges_size);
	for (int i = 0; i < privileges_size; ++i) {
		privileges[i] = (as_privilege *)cf_malloc(sizeof(as_privilege));
	}

	pyobject_to_as_privileges(&err, py_privileges, privileges, privileges_size);

	pyobject_to_policy_admin(self, &err, py_policy, &admin_policy,
							 &admin_policy_p, &self->as->config.policies.admin);
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	char *role = NULL;
	if (PyUnicode_Check(py_role)) {
		role = PyString_AsString(py_role);
	}
	else {
		as_error_update(&err, AEROSPIKE_ERR_PARAM,
						"Role name should be a string");
		goto CLEANUP;
	}

	// Invoke operation
	Py_BEGIN_ALLOW_THREADS
	aerospike_grant_privileges(self->as, &err, admin_policy_p, role, privileges,
							   privileges_size);
	Py_END_ALLOW_THREADS

CLEANUP:
	if (privileges != NULL) {
		for (int i = 0; i < privileges_size; i++) {
			if (privileges[i])
				cf_free(privileges[i]);
		}

		cf_free(privileges);
	}

	if (err.code != AEROSPIKE_OK) {
		PyObject *py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyObject *exception_type = raise_exception(&err);
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return NULL;
	}

	return PyLong_FromLong(0);
}

/**
 *******************************************************************************************************
 * Revoke privileges to a role in the Aerospike DB.
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns an integer status. 0(Zero) is success value.
 * In case of error,appropriate exceptions will be raised.
 *******************************************************************************************************
 */
PyObject *AerospikeClient_Admin_Revoke_Privileges(AerospikeClient *self,
												  PyObject *args,
												  PyObject *kwds)
{
	// Initialize error
	as_error err;
	as_error_init(&err);

	// Python Function Arguments
	PyObject *py_policy = NULL;
	PyObject *py_role = NULL;
	PyObject *py_privileges = NULL;

	as_policy_admin admin_policy;
	as_policy_admin *admin_policy_p = NULL;

	// Python Function Keyword Arguments
	static char *kwlist[] = {"role", "privileges", "policy", NULL};

	// Python Function Argument Parsing
	if (PyArg_ParseTupleAndKeywords(args, kwds, "OO|O:admin_revoke_privileges",
									kwlist, &py_role, &py_privileges,
									&py_policy) == false) {
		return NULL;
	}

	// Aerospike Operation Arguments
	int privileges_size = 0;
	as_privilege **privileges = NULL;

	if (!self || !self->as) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
		goto CLEANUP;
	}

	if (!self->is_conn_16) {
		as_error_update(&err, AEROSPIKE_ERR_CLUSTER,
						"No connection to aerospike cluster");
		goto CLEANUP;
	}

	// Convert python object to an array of privileges
	if (!PyList_Check(py_privileges)) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM,
						"Privileges should be a list");
		goto CLEANUP;
	}

	privileges_size = PyList_Size(py_privileges);
	privileges =
		(as_privilege **)cf_malloc(sizeof(as_privilege *) * privileges_size);
	for (int i = 0; i < privileges_size; ++i) {
		privileges[i] = (as_privilege *)cf_malloc(sizeof(as_privilege));
	}

	pyobject_to_as_privileges(&err, py_privileges, privileges, privileges_size);

	pyobject_to_policy_admin(self, &err, py_policy, &admin_policy,
							 &admin_policy_p, &self->as->config.policies.admin);
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	char *role = NULL;
	if (PyUnicode_Check(py_role)) {
		role = PyString_AsString(py_role);
	}
	else {
		as_error_update(&err, AEROSPIKE_ERR_PARAM,
						"Role name should be a string");
		goto CLEANUP;
	}

	// Invoke operation
	Py_BEGIN_ALLOW_THREADS
	aerospike_revoke_privileges(self->as, &err, admin_policy_p, role,
								privileges, privileges_size);
	Py_END_ALLOW_THREADS

CLEANUP:
	if (privileges != NULL) {
		for (int i = 0; i < privileges_size; i++) {
			if (privileges[i])
				cf_free(privileges[i]);
		}

		cf_free(privileges);
	}

	if (err.code != AEROSPIKE_OK) {
		PyObject *py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyObject *exception_type = raise_exception(&err);
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return NULL;
	}

	return PyLong_FromLong(0);
}

/**
 *******************************************************************************************************
 * Query a role in the Aerospike DB.
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns data of a particular role on success.
 * In case of error,appropriate exceptions will be raised.
 *******************************************************************************************************
 */
PyObject *AerospikeClient_Admin_Query_Role(AerospikeClient *self,
										   PyObject *args, PyObject *kwds)
{
	// Initialize error
	as_error err;
	as_error_init(&err);

	// Python Function Arguments
	PyObject *py_policy = NULL;
	PyObject *py_role = NULL;
	PyObject *py_ret_role = NULL;

	as_policy_admin admin_policy;
	as_policy_admin *admin_policy_p = NULL;

	as_role *ret_role = NULL;

	// Python Function Keyword Arguments
	static char *kwlist[] = {"role", "policy", NULL};

	// Python Function Argument Parsing
	if (PyArg_ParseTupleAndKeywords(args, kwds, "O|O:admin_query_role", kwlist,
									&py_role, &py_policy) == false) {
		return NULL;
	}

	if (!self || !self->as) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
		goto CLEANUP;
	}

	if (!self->is_conn_16) {
		as_error_update(&err, AEROSPIKE_ERR_CLUSTER,
						"No connection to aerospike cluster");
		goto CLEANUP;
	}

	pyobject_to_policy_admin(self, &err, py_policy, &admin_policy,
							 &admin_policy_p, &self->as->config.policies.admin);
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	char *role = NULL;
	if (PyUnicode_Check(py_role)) {
		role = PyString_AsString(py_role);
	}
	else {
		as_error_update(&err, AEROSPIKE_ERR_PARAM,
						"Role name should be a string");
		goto CLEANUP;
	}

	// Invoke operation
	Py_BEGIN_ALLOW_THREADS
	aerospike_query_role(self->as, &err, admin_policy_p, role, &ret_role);
	Py_END_ALLOW_THREADS
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	as_role_to_pyobject_old(&err, ret_role, &py_ret_role);

CLEANUP:

	if (ret_role != NULL) {
		as_role_destroy(ret_role);
	}

	if (err.code != AEROSPIKE_OK) {
		PyObject *py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyObject *exception_type = raise_exception(&err);
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return NULL;
	}

	return py_ret_role;
}

/**
 *******************************************************************************************************
 * Query all roles in the Aerospike DB.
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns data of all roles on success.
 * In case of error,appropriate exceptions will be raised.
 *******************************************************************************************************
 */
PyObject *AerospikeClient_Admin_Query_Roles(AerospikeClient *self,
											PyObject *args, PyObject *kwds)
{
	// Initialize error
	as_error err;
	as_error_init(&err);

	// Python Function Arguments
	PyObject *py_policy = NULL;
	PyObject *py_ret_role = NULL;

	as_policy_admin admin_policy;
	as_policy_admin *admin_policy_p = NULL;

	as_role **ret_role = NULL;
	int ret_role_size = 0;

	// Python Function Keyword Arguments
	static char *kwlist[] = {"policy", NULL};

	// Python Function Argument Parsing
	if (PyArg_ParseTupleAndKeywords(args, kwds, "|O:admin_query_roles", kwlist,
									&py_policy) == false) {
		return NULL;
	}

	if (!self || !self->as) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
		goto CLEANUP;
	}

	if (!self->is_conn_16) {
		as_error_update(&err, AEROSPIKE_ERR_CLUSTER,
						"No connection to aerospike cluster");
		goto CLEANUP;
	}

	pyobject_to_policy_admin(self, &err, py_policy, &admin_policy,
							 &admin_policy_p, &self->as->config.policies.admin);
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	// Invoke operation
	Py_BEGIN_ALLOW_THREADS
	aerospike_query_roles(self->as, &err, admin_policy_p, &ret_role,
						  &ret_role_size);
	Py_END_ALLOW_THREADS
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	as_role_array_to_pyobject_old(&err, ret_role, &py_ret_role, ret_role_size);

CLEANUP:

	if (ret_role) {
		as_roles_destroy(ret_role, ret_role_size);
	}

	if (err.code != AEROSPIKE_OK) {
		PyObject *py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyObject *exception_type = raise_exception(&err);
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return NULL;
	}

	return py_ret_role;
}

/**
 *******************************************************************************************************
 * Query a role in the Aerospike DB.
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns data of a particular role on success.
 * In case of error,appropriate exceptions will be raised.
 *******************************************************************************************************
 */
PyObject *AerospikeClient_Admin_Get_Role(AerospikeClient *self, PyObject *args,
										 PyObject *kwds)
{
	// Initialize error
	as_error err;
	as_error_init(&err);

	// Python Function Arguments
	PyObject *py_policy = NULL;
	PyObject *py_role = NULL;
	PyObject *py_ret_role = NULL;

	as_policy_admin admin_policy;
	as_policy_admin *admin_policy_p = NULL;

	as_role *ret_role = NULL;

	// Python Function Keyword Arguments
	static char *kwlist[] = {"role", "policy", NULL};

	// Python Function Argument Parsing
	if (PyArg_ParseTupleAndKeywords(args, kwds, "O|O:admin_get_role", kwlist,
									&py_role, &py_policy) == false) {
		return NULL;
	}

	if (!self || !self->as) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
		goto CLEANUP;
	}

	if (!self->is_conn_16) {
		as_error_update(&err, AEROSPIKE_ERR_CLUSTER,
						"No connection to aerospike cluster");
		goto CLEANUP;
	}

	pyobject_to_policy_admin(self, &err, py_policy, &admin_policy,
							 &admin_policy_p, &self->as->config.policies.admin);
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	char *role = NULL;
	if (PyUnicode_Check(py_role)) {
		role = PyString_AsString(py_role);
	}
	else {
		as_error_update(&err, AEROSPIKE_ERR_PARAM,
						"Role name should be a string");
		goto CLEANUP;
	}

	// Invoke operation
	Py_BEGIN_ALLOW_THREADS
	aerospike_query_role(self->as, &err, admin_policy_p, role, &ret_role);
	Py_END_ALLOW_THREADS
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	py_ret_role = PyDict_New();
	if (py_ret_role == NULL) {
		as_error_update(&err, AEROSPIKE_ERR_CLIENT,
						"Failed to create py_ret_role.");
		goto CLEANUP;
	}

	as_role_to_pyobject(&err, ret_role, py_ret_role);

CLEANUP:

	if (ret_role) {
		as_role_destroy(ret_role);
	}

	if (err.code != AEROSPIKE_OK) {
		PyObject *py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyObject *exception_type = raise_exception(&err);
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return NULL;
	}

	return py_ret_role;
}

/**
 *******************************************************************************************************
 * Query all roles in the Aerospike DB.
 *
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns data of all roles on success.
 * In case of error,appropriate exceptions will be raised.
 *******************************************************************************************************
 */
PyObject *AerospikeClient_Admin_Get_Roles(AerospikeClient *self, PyObject *args,
										  PyObject *kwds)
{
	// Initialize error
	as_error err;
	as_error_init(&err);

	// Python Function Arguments
	PyObject *py_policy = NULL;
	PyObject *py_ret_role = NULL;

	as_policy_admin admin_policy;
	as_policy_admin *admin_policy_p = NULL;

	as_role **ret_role = NULL;
	int ret_role_size = 0;

	// Python Function Keyword Arguments
	static char *kwlist[] = {"policy", NULL};

	// Python Function Argument Parsing
	if (PyArg_ParseTupleAndKeywords(args, kwds, "|O:admin_get_roles", kwlist,
									&py_policy) == false) {
		return NULL;
	}

	if (!self || !self->as) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
		goto CLEANUP;
	}

	if (!self->is_conn_16) {
		as_error_update(&err, AEROSPIKE_ERR_CLUSTER,
						"No connection to aerospike cluster");
		goto CLEANUP;
	}

	pyobject_to_policy_admin(self, &err, py_policy, &admin_policy,
							 &admin_policy_p, &self->as->config.policies.admin);
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	// Invoke operation
	Py_BEGIN_ALLOW_THREADS
	aerospike_query_roles(self->as, &err, admin_policy_p, &ret_role,
						  &ret_role_size);
	Py_END_ALLOW_THREADS
	if (err.code != AEROSPIKE_OK) {
		goto CLEANUP;
	}

	as_role_array_to_pyobject(&err, ret_role, &py_ret_role, ret_role_size);

CLEANUP:

	if (ret_role) {
		as_roles_destroy(ret_role, ret_role_size);
	}

	if (err.code != AEROSPIKE_OK) {
		PyObject *py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyObject *exception_type = raise_exception(&err);
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return NULL;
	}

	return py_ret_role;
}