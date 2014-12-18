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
#include "policy.h"

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
	int success = -1;
	int roles_size = 0;
	char **roles = NULL;
	char *user = NULL, *password = NULL;

	if (!self || !self->as) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
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
	pyobject_to_policy_admin( &err, py_policy, &admin_policy, &admin_policy_p);
	if ( err.code != AEROSPIKE_OK ) {
		goto CLEANUP;
	}

	// Invoke operation
	success = aerospike_create_user(self->as, admin_policy_p, user, password, (const char**)roles, roles_size);
	if ( success ) {
		as_error_update(&err, success, "aerospike create user failed");
	}

CLEANUP:
	for(int i = 0; i < roles_size; i++) {
		if( roles[i] != NULL)
			cf_free(roles[i]);
	}

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		Py_DECREF(py_err);
		return NULL;
	}

	return PyLong_FromLong(0);
}


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

	// Aerospike Operation Arguments
	int success = -1;
	char *user = NULL;

	// Convert python object to policy_admin
	pyobject_to_policy_admin(&err, py_policy, &admin_policy, &admin_policy_p);
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
	success = aerospike_drop_user(self->as, admin_policy_p, user);
	if ( success ) {
		as_error_update(&err, success, "aerospike drop user failed");
	}

CLEANUP:

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		Py_DECREF(py_err);
		return NULL;
	}

	return PyLong_FromLong(0);
}


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

	// Aerospike Operation Arguments
	int success = -1;
	char *user = NULL, *password = NULL;

	// Convert python object to policy_admin
	pyobject_to_policy_admin(&err, py_policy, &admin_policy, &admin_policy_p);
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
	success = aerospike_set_password( self->as, admin_policy_p, user, password );
	if ( success ) {
		as_error_update(&err, success, "aerospike set password failed");
	}

CLEANUP:

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		Py_DECREF(py_err);
		return NULL;
	}

	return PyLong_FromLong(0);
}


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

	// Aerospike Operation Arguments
	int success = -1;
	char *user = NULL, *password = NULL;

	// Convert python object to policy_admin
	pyobject_to_policy_admin(&err, py_policy, &admin_policy, &admin_policy_p);
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
	success = aerospike_change_password( self->as, admin_policy_p, user, password );
	if ( success ) {
		as_error_update(&err, success, "aerospike change password failed");
	}

CLEANUP:

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		Py_DECREF(py_err);
		return NULL;
	}

	return PyLong_FromLong(0);
}


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

	// Aerospike Operation Result
	int success = -1;

	if (!self || !self->as) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
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
	pyobject_to_policy_admin(&err, py_policy, &admin_policy, &admin_policy_p);
	if ( err.code != AEROSPIKE_OK ) {
		goto CLEANUP;
	}

	// Invoke operation
	success = aerospike_grant_roles(self->as, admin_policy_p, user, (const char**)roles, roles_size);
	if ( success ) {
		as_error_update(&err, success, "aerospike grant roles failed");
	}

CLEANUP:
	for (int i = 0; i < roles_size; i++) {
		if(roles[i] != NULL)
			cf_free(roles[i]);
	}

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		Py_DECREF(py_err);
		return NULL;
	}

	return PyLong_FromLong(0);
}


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

	// Aerospike Operation Result
	int success = -1;

	if (!self || !self->as) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
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
	pyobject_to_policy_admin(&err, py_policy, &admin_policy, &admin_policy_p);
	if ( err.code != AEROSPIKE_OK ) {
		goto CLEANUP;
	}

	// Invoke operation
	success = aerospike_revoke_roles(self->as, admin_policy_p, user, (const char**)roles, roles_size);
	if ( success ) {
		as_error_update(&err, success, "aerospike revoke roles failed");
	}

CLEANUP:
	for (int i = 0; i < roles_size; i++) {
		if(roles[i] != NULL)
			cf_free(roles[i]);
	}

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		Py_DECREF(py_err);
		return NULL;
	}

	return PyLong_FromLong(0);
}


PyObject * AerospikeClient_Admin_Replace_Roles( AerospikeClient *self, PyObject *args, PyObject *kwds )
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
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "OOOO:admin_replace_roles", kwlist,
				&py_policy, &py_user, &py_roles, &py_roles_size) == false ) {
		return NULL;
	}

	// Aerospike Operation Arguments
	char *user = NULL;
	int roles_size = 0;
	char **roles = NULL;

	// Aerospike Operation Result
	int success = -1;

	if (!self || !self->as) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
		goto CLEANUP;
	}

	// Convert python object to array of roles
	roles_size = (int) PyInt_AsLong(py_roles_size);
	roles = alloca(sizeof(char *) * roles_size);
	for ( int i = 0; i < roles_size; i++ ) {
		roles[i] = cf_malloc(sizeof(char) * AS_ROLE_SIZE);
		memset(roles[i], 0, sizeof(char) * AS_ROLE_SIZE);
	}

	pyobject_to_strArray(&err, py_roles, roles);
	if ( err.code != AEROSPIKE_OK ) {
		goto CLEANUP;
	}

	// Convert python object to username string
	if ( !PyString_Check(py_user) ) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Username should be a string");
		goto CLEANUP;
	}

	user = PyString_AsString(py_user);

	// Convert python object to policy_admin
	pyobject_to_policy_admin(&err, py_policy, &admin_policy, &admin_policy_p);
	if ( err.code != AEROSPIKE_OK ) {
		goto CLEANUP;
	}

	// Invoke operation
	success = aerospike_replace_roles(self->as, admin_policy_p, user, (const char**)roles, roles_size);
	if ( success ) {
		as_error_update(&err, success, "aerospike replace roles failed");
	}

CLEANUP:
	for (int i = 0; i < roles_size; i++) {
		if(roles[i] != NULL)
			cf_free(roles[i]);
	}

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		Py_DECREF(py_err);
		return NULL;
	}

	return PyLong_FromLong(0);
}

PyObject * AerospikeClient_Admin_Query_User( AerospikeClient * self, PyObject * args, PyObject *kwds )
{
	// Initialize error
	as_error err;
	as_error_init(&err);

	// Python Function Arguments
	PyObject * py_policy = NULL;
	PyObject * py_user = NULL;

	// Python Function Result
	PyObject * py_user_roles = NULL;

	as_policy_admin admin_policy;
	as_policy_admin *admin_policy_p = NULL;

	// Python Function Keyword Arguments
	static char * kwlist[] = {"policy", "user", NULL};

	// Python Function Argument Parsing
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "OO:admin_query_user", kwlist, &py_policy, &py_user) == false ) {
		return NULL;
	}

	// Aerospike Operation Arguments
	char *user = NULL;
	as_user_roles *user_roles = NULL;

	// Aerospike Operation Result
	int success = -1;

	if (!self || !self->as) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
		goto CLEANUP;
	}

	// Convert python object to policy_admin
	pyobject_to_policy_admin(&err, py_policy, &admin_policy, &admin_policy_p);
	if ( err.code != AEROSPIKE_OK ) {
		goto CLEANUP;
	}

	// Convert python object to username string
	if ( !PyString_Check(py_user) ) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Username should be a string");
		goto CLEANUP;
	}

	user = PyString_AsString(py_user);

	// Invoke operation
	success = aerospike_query_user(self->as, admin_policy_p, user, &user_roles);
	if ( success ) {
		as_error_update(&err, success, "aerospike query user failed");
		goto CLEANUP;
	}

	// Convert returned as_user_roles struct to python object
	as_user_roles_to_pyobject(&err, user_roles, &py_user_roles);
	if ( err.code != AEROSPIKE_OK ) {
		goto CLEANUP;
	}

CLEANUP:
	if( user_roles != NULL )
		as_user_roles_destroy(user_roles);

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		Py_DECREF(py_err);
		return NULL;
	}

	return py_user_roles;
}

PyObject * AerospikeClient_Admin_Query_Users( AerospikeClient * self, PyObject * args, PyObject *kwds )
{
	// Initialize error
	as_error err;
	as_error_init(&err);

	// Python Function Arguments
	PyObject * py_policy = NULL;

	// Python Function Result
	PyObject * py_user_roles = NULL;

	as_policy_admin admin_policy;
	as_policy_admin *admin_policy_p = NULL;

	// Python Function Keyword Arguments
	static char * kwlist[] = {"policy", NULL};

	// Python Function Argument Parsing
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "O:admin_query_users", kwlist, &py_policy) == false ) {
		return NULL;
	}

	// Aerospike Operation Arguments
	int users = 0;
	as_user_roles **user_roles = NULL;

	// Aerospike Operation Result
	int success = -1;

	if (!self || !self->as) {
		as_error_update(&err, AEROSPIKE_ERR_PARAM, "Invalid aerospike object");
		goto CLEANUP;
	}

	// Convert python object to policy_admin
	pyobject_to_policy_admin(&err, py_policy, &admin_policy, &admin_policy_p);
	if ( err.code != AEROSPIKE_OK ) {
		goto CLEANUP;
	}

	// Invoke operation
	success = aerospike_query_users(self->as, admin_policy_p, &user_roles, &users);
	if ( success ) {
		as_error_update(&err, success, "aerospike query users failed");
		goto CLEANUP;
	}

	// Convert returned array of as_user_roles structs into python object;
	as_user_roles_array_to_pyobject(&err, user_roles, &py_user_roles, users);
	if ( err.code != AEROSPIKE_OK ) {
		goto CLEANUP;
	}

CLEANUP:
	as_user_roles_destroy_array(user_roles, users);

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		Py_DECREF(py_err);
		return NULL;
	}

	return py_user_roles;
}
