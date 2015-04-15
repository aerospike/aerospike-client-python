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

#include <aerospike/aerospike.h>
#include <aerospike/as_admin.h>
#include <aerospike/as_config.h>
#include <aerospike/as_error.h>
#include <aerospike/as_policy.h>

#include "admin.h"
#include "conversions.h"
#include "exceptions.h"
#include "policy.h"

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
PyObject * AerospikeClient_Admin_Create_User(AerospikeClient * self, PyObject *args, PyObject * kwds)
{
	// Initialize error
	as_error err;
	as_error_init(&err);

	// Python Function Arguments
	PyObject * py_policy = NULL;
	PyObject * py_user = NULL;
	PyObject * py_password = NULL;
	PyObject * py_roles = NULL;
	PyObject * py_roles_size = NULL;

	as_policy_admin admin_policy;
	as_policy_admin *admin_policy_p = NULL;

	// Python Function Keyword Arguments
	static char * kwlist[] = {"policy", "user", "password", "roles", "roles_size", NULL};

	// Python Function Argument Parsing
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "OOOOO:admin_create_user", kwlist,
				&py_policy, &py_user, &py_password, &py_roles, &py_roles_size) == false ) {
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
		as_error_update(&err, AEROSPIKE_ERR_CLUSTER, "No connection to aerospike cluster");
		goto CLEANUP;
	}

	// Convert python object to an array of roles
	roles_size = (int) PyInt_AsLong(py_roles_size);
	roles = alloca(sizeof(char *) * roles_size);
	for (int i = 0; i < roles_size; i++) {
		roles[i] = cf_malloc(sizeof(char) * AS_ROLE_SIZE);
		memset(roles[i], 0, sizeof(char) * AS_ROLE_SIZE);
	}

	pyobject_to_strArray(&err, py_roles, roles);
	if ( err.code != AEROSPIKE_OK ) {
		goto CLEANUP;
	}

	// Convert python objects to username and password strings
	if ( !PyString_Check(py_user) ) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Username should be a string");
		goto CLEANUP;
	}

	user = PyString_AsString(py_user);

	if( !PyString_Check(py_password) ) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Password should be a string");
		goto CLEANUP;
	}

	password = PyString_AsString(py_password);

	// Convert python object to policy_admin
	pyobject_to_policy_admin( &err, py_policy, &admin_policy, &admin_policy_p,
			&self->as->config.policies.admin);
	if ( err.code != AEROSPIKE_OK ) {
		goto CLEANUP;
	}

	// Invoke operation
	aerospike_create_user(self->as, &err, admin_policy_p, user, password, (const char**)roles, roles_size);
	if(err.code != AEROSPIKE_OK) {
		as_error_update(&err, err.code, err.message);
		goto CLEANUP;
	}

CLEANUP:
	for(int i = 0; i < roles_size; i++) {
		if( roles[i] != NULL)
			cf_free(roles[i]);
	}

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
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
PyObject * AerospikeClient_Admin_Drop_User( AerospikeClient *self, PyObject *args, PyObject *kwds )
{
	// Initialize error
	as_error err;
	as_error_init(&err);

	// Python Function Arguments
	PyObject * py_policy = NULL;
	PyObject * py_user = NULL;

	as_policy_admin admin_policy;
	as_policy_admin *admin_policy_p = NULL;

	// Python Function Keyword Arguments
	static char * kwlist[] = {"policy", "user", NULL};

	// Python Function Argument Parsing
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "OO:admin_drop_user", kwlist,
				&py_policy, &py_user) == false ) {
		return NULL;
	}

	if (!self || !self->as) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
		goto CLEANUP;
	}

	if (!self->is_conn_16) {
		as_error_update(&err, AEROSPIKE_ERR_CLUSTER, "No connection to aerospike cluster");
		goto CLEANUP;
	}

	// Aerospike Operation Arguments
	char *user = NULL;

	// Convert python object to policy_admin
	pyobject_to_policy_admin(&err, py_policy, &admin_policy, &admin_policy_p,
			&self->as->config.policies.admin);
	if ( err.code != AEROSPIKE_OK ) {
		goto CLEANUP;
	}

	// Convert python object to username string
	if ( !PyString_Check(py_user) ) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Username should be a string");
		goto CLEANUP;
	}

	user = PyString_AsString(py_user);

	//Invoke operation
	aerospike_drop_user(self->as, &err, admin_policy_p, user);
	if(err.code != AEROSPIKE_OK) {
		as_error_update(&err, err.code, err.message);
		goto CLEANUP;
	}

CLEANUP:

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
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
PyObject * AerospikeClient_Admin_Set_Password( AerospikeClient *self, PyObject *args, PyObject *kwds )
{
	// Initialize error
	as_error err;
	as_error_init(&err);

	// Python Function Arguments
	PyObject * py_policy = NULL;
	PyObject * py_user = NULL;
	PyObject * py_password = NULL;

	as_policy_admin admin_policy;
	as_policy_admin *admin_policy_p = NULL;

	// Python Function Keyword Arguments
	static char * kwlist[] = {"policy", "user", "password", NULL};

	// Python Function Argument Parsing
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "OOO:admin_set_password", kwlist,
				&py_policy, &py_user, &py_password) == false ) {
		return NULL;
	}

	if (!self || !self->as) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
		goto CLEANUP;
	}

	if (!self->is_conn_16) {
		as_error_update(&err, AEROSPIKE_ERR_CLUSTER, "No connection to aerospike cluster");
		goto CLEANUP;
	}

	// Aerospike Operation Arguments
	char *user = NULL, *password = NULL;

	// Convert python object to policy_admin
	pyobject_to_policy_admin(&err, py_policy, &admin_policy, &admin_policy_p,
			&self->as->config.policies.admin);
	if ( err.code != AEROSPIKE_OK ) {
		goto CLEANUP;
	}

	// Convert python objects into username and password strings
	if ( !PyString_Check(py_user) ) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Username should be a string");
		goto CLEANUP;
	}

	user = PyString_AsString(py_user);

	if ( !PyString_Check(py_password) ) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Password should be a string");
		goto CLEANUP;
	}

	password = PyString_AsString(py_password);

	// Invoke operation
	aerospike_set_password( self->as, &err, admin_policy_p, user, password );
	if(err.code != AEROSPIKE_OK) {
		as_error_update(&err, err.code, err.message);
		goto CLEANUP;
	}

CLEANUP:

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
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
PyObject * AerospikeClient_Admin_Change_Password( AerospikeClient *self, PyObject *args, PyObject *kwds )
{
	// Initialize error
	as_error err;
	as_error_init(&err);

	// Python Function Arguments
	PyObject * py_policy = NULL;
	PyObject * py_user = NULL;
	PyObject * py_password = NULL;

	as_policy_admin admin_policy;
	as_policy_admin *admin_policy_p = NULL;

	// Python Function Keyword Arguments
	static char * kwlist[] = {"policy", "user", "password", NULL};

	// Python Function Argument Parsing
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "OOO:admin_change_password", kwlist,
				&py_policy, &py_user, &py_password) == false ) {
		return NULL;
	}

	if (!self || !self->as) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
		goto CLEANUP;
	}

	if (!self->is_conn_16) {
		as_error_update(&err, AEROSPIKE_ERR_CLUSTER, "No connection to aerospike cluster");
		goto CLEANUP;
	}

	// Aerospike Operation Arguments
	char *user = NULL, *password = NULL;

	// Convert python object to policy_admin
	pyobject_to_policy_admin(&err, py_policy, &admin_policy, &admin_policy_p,
			&self->as->config.policies.admin);
	if ( err.code != AEROSPIKE_OK ) {
		goto CLEANUP;
	}

	// Convert python objects into username and password strings
	if ( !PyString_Check(py_user) ) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Username should be a string");
		goto CLEANUP;
	}

	user = PyString_AsString(py_user);

	if ( !PyString_Check(py_password) ) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Password should be a string");
		goto CLEANUP;
	}

	password = PyString_AsString(py_password);

	// Invoke operation
	aerospike_change_password( self->as, &err, admin_policy_p, user, password );
	if(err.code != AEROSPIKE_OK) {
		as_error_update(&err, err.code, err.message);
		goto CLEANUP;
	}

CLEANUP:

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
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
PyObject * AerospikeClient_Admin_Grant_Roles( AerospikeClient *self, PyObject *args, PyObject *kwds )
{
	// Initialize error
	as_error err;
	as_error_init(&err);

	// Python Function Arguments
	PyObject * py_policy = NULL;
	PyObject * py_user = NULL;
	PyObject * py_roles = NULL;
	PyObject * py_roles_size = NULL;

	as_policy_admin admin_policy;
	as_policy_admin *admin_policy_p = NULL;

	// Python Function Keyword Arguments
	static char * kwlist[] = {"policy", "user", "roles", "roles_size", NULL};

	// Python Function Argument Parsing
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "OOOO:admin_grant_roles", kwlist,
				&py_policy, &py_user, &py_roles, &py_roles_size) == false ) {
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
		as_error_update(&err, AEROSPIKE_ERR_CLUSTER, "No connection to aerospike cluster");
		goto CLEANUP;
	}

	// Convert python object to array of roles
	roles_size = (int) PyInt_AsLong(py_roles_size);
	roles = alloca(sizeof(char *) * roles_size);
	for (int i = 0; i < roles_size; i++) {
		roles[i] = cf_malloc(sizeof(char) * AS_ROLE_SIZE);
		memset(roles[i], 0, sizeof(char) * AS_ROLE_SIZE);
	}

	pyobject_to_strArray(&err, py_roles, roles);
	if ( err.code != AEROSPIKE_OK ) {
		goto CLEANUP;
	}

	// Convert python object into username string
	if ( !PyString_Check(py_user) ) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Username should be a string");
		goto CLEANUP;
	}

	user = PyString_AsString(py_user);

	// Convert python object to policy_admin
	pyobject_to_policy_admin(&err, py_policy, &admin_policy, &admin_policy_p,
			&self->as->config.policies.admin);
	if ( err.code != AEROSPIKE_OK ) {
		goto CLEANUP;
	}

	// Invoke operation
	aerospike_grant_roles(self->as, &err, admin_policy_p, user, (const char**)roles, roles_size);
	if(err.code != AEROSPIKE_OK) {
		as_error_update(&err, err.code, err.message);
		goto CLEANUP;
	}

CLEANUP:
	for (int i = 0; i < roles_size; i++) {
		if(roles[i] != NULL)
			cf_free(roles[i]);
	}

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
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
PyObject * AerospikeClient_Admin_Revoke_Roles( AerospikeClient *self, PyObject *args, PyObject *kwds )
{
	// Initialize error
	as_error err;
	as_error_init(&err);

	// Python Function Arguments
	PyObject * py_policy = NULL;
	PyObject * py_user = NULL;
	PyObject * py_roles = NULL;
	PyObject * py_roles_size = NULL;

	as_policy_admin admin_policy;
	as_policy_admin *admin_policy_p = NULL;

	// Python Function Keyword Arguments
	static char * kwlist[] = {"policy", "user", "roles", "roles_size", NULL};

	// Python Function Argument Parsing
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "OOOO:admin_revoke_roles", kwlist,
				&py_policy, &py_user, &py_roles, &py_roles_size) == false ) {
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
		as_error_update(&err, AEROSPIKE_ERR_CLUSTER, "No connection to aerospike cluster");
		goto CLEANUP;
	}

	// Convert python object to array of roles
	roles_size = (int) PyInt_AsLong(py_roles_size);
	roles = alloca(sizeof(char *) * roles_size);
	for (int i = 0; i < roles_size; i++) {
		roles[i] = cf_malloc(sizeof(char) * AS_ROLE_SIZE);
		memset(roles[i], 0, sizeof(char) * AS_ROLE_SIZE);
	}

	pyobject_to_strArray(&err, py_roles, roles);
	if ( err.code != AEROSPIKE_OK ) {
		goto CLEANUP;
	}

	if ( py_policy == Py_None ) {
		py_policy = PyDict_New();
	}

	// Convert python object to username string
	if ( !PyString_Check(py_user) ) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Username should be a string");
		goto CLEANUP;
	}

	user = PyString_AsString(py_user);

	// Convert python object to policy_admin
	pyobject_to_policy_admin(&err, py_policy, &admin_policy, &admin_policy_p,
			&self->as->config.policies.admin);
	if ( err.code != AEROSPIKE_OK ) {
		goto CLEANUP;
	}

	// Invoke operation
	aerospike_revoke_roles(self->as, &err, admin_policy_p, user, (const char**)roles, roles_size);
	if(err.code != AEROSPIKE_OK) {
		as_error_update(&err, err.code, err.message);
		goto CLEANUP;
	}

CLEANUP:
	for (int i = 0; i < roles_size; i++) {
		if(roles[i] != NULL)
			cf_free(roles[i]);
	}

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
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
PyObject * AerospikeClient_Admin_Query_User( AerospikeClient * self, PyObject * args, PyObject *kwds )
{
	// Initialize error
	as_error err;
	as_error_init(&err);

	// Python Function Arguments
	PyObject * py_policy = NULL;
	PyObject * py_user_name = NULL;

	// Python Function Result
	PyObject * py_user = NULL;

	as_policy_admin admin_policy;
	as_policy_admin *admin_policy_p = NULL;

	// Python Function Keyword Arguments
	static char * kwlist[] = {"policy", "user", NULL};

	// Python Function Argument Parsing
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "OO:admin_query_user", kwlist, &py_policy, &py_user_name) == false ) {
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
		as_error_update(&err, AEROSPIKE_ERR_CLUSTER, "No connection to aerospike cluster");
		goto CLEANUP;
	}

	// Convert python object to policy_admin
	pyobject_to_policy_admin(&err, py_policy, &admin_policy, &admin_policy_p,
			&self->as->config.policies.admin);
	if ( err.code != AEROSPIKE_OK ) {
		goto CLEANUP;
	}

	// Convert python object to username string
	if ( !PyString_Check(py_user_name) ) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Username should be a string");
		goto CLEANUP;
	}

	user_name = PyString_AsString(py_user_name);

	// Invoke operation
	aerospike_query_user(self->as, &err, admin_policy_p, user_name, &user);
	if(err.code != AEROSPIKE_OK) {
		as_error_update(&err, err.code, err.message);
		goto CLEANUP;
	}

	// Convert returned as_user struct to python object
	as_user_to_pyobject(&err, user, &py_user);
	if(err.code != AEROSPIKE_OK) {
		as_error_update(&err, err.code, err.message);
		goto CLEANUP;
	}

CLEANUP:
	if( user != NULL )
		as_user_destroy(user);

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
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
PyObject * AerospikeClient_Admin_Query_Users( AerospikeClient * self, PyObject * args, PyObject *kwds )
{
	// Initialize error
	as_error err;
	as_error_init(&err);

	// Python Function Arguments
	PyObject * py_policy = NULL;

	// Python Function Result
	PyObject * py_users = NULL;

	as_policy_admin admin_policy;
	as_policy_admin *admin_policy_p = NULL;

	// Python Function Keyword Arguments
	static char * kwlist[] = {"policy", NULL};

	// Python Function Argument Parsing
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "O:admin_query_users", kwlist, &py_policy) == false ) {
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
		as_error_update(&err, AEROSPIKE_ERR_CLUSTER, "No connection to aerospike cluster");
		goto CLEANUP;
	}

	// Convert python object to policy_admin
	pyobject_to_policy_admin(&err, py_policy, &admin_policy, &admin_policy_p,
			&self->as->config.policies.admin);
	if ( err.code != AEROSPIKE_OK ) {
		goto CLEANUP;
	}

	// Invoke operation
	aerospike_query_users(self->as, &err, admin_policy_p, &users, &users_size);
	if(err.code != AEROSPIKE_OK) {
		as_error_update(&err, err.code, err.message);
		goto CLEANUP;
	}

	// Convert returned array of as_user structs into python object;
	as_user_array_to_pyobject(&err, users, &py_users, users_size);
	if(err.code != AEROSPIKE_OK) {
		as_error_update(&err, err.code, err.message);
		goto CLEANUP;
	}

CLEANUP:
	as_users_destroy(users, users_size);

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyObject *exception_type = raise_exception(&err);
		PyErr_SetObject(exception_type, py_err);
		Py_DECREF(py_err);
		return NULL;
	}

	return py_users;
}
