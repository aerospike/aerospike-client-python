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
#include <structmember.h>
#include <stdbool.h>

#include <aerospike/aerospike.h>
#include <aerospike/as_admin.h>
#include <aerospike/as_config.h>
#include <aerospike/as_error.h>
#include <aerospike/as_policy.h>

#include "admin.h"
#include "conversions.h"
#include "policy.h"

PyObject * AerospikeClient_create_user( AerospikeClient * self, PyObject *args )
{
	int success = -1;
	as_error err;
	as_error_init(&err);
	
	PyObject * py_policy = NULL;
	PyObject * py_user = NULL;
	PyObject * py_password = NULL;
	PyObject * py_roles = NULL;
	PyObject * py_roles_size = NULL;

	if ( PyArg_ParseTuple(args, "OOOOO:admin_create_user", 
	&py_policy, &py_user, &py_password, &py_roles, &py_roles_size) == false ) {
		return PyLong_FromLong(-1);
	}
	
	char *user, *password;
	if( PyString_Check(py_user) ) {
		user = PyString_AsString(py_user);		
	}
	else {
		return PyLong_FromLong(-1);
	}

	if( PyString_Check(py_password) ) {
		password = PyString_AsString(py_password);		
	}
	else {
		return PyLong_FromLong(-1);
	}

	int roles_size = (int)PyInt_AsLong(py_roles_size); 
	
	char* roles[roles_size];
	for(int i = 0; i < roles_size; i++) {
		roles[i] = cf_malloc(sizeof(char) * AS_ROLE_SIZE);
	}
 
	pyobject_to_strArray(&err, py_roles, roles);
	if ( err.code != AEROSPIKE_OK ) {
		goto CLEANUP;
	}
	
	as_policy_admin *policy, policy_struct;
	pyobject_to_policy_admin( &err, py_policy, &policy_struct, &policy );
	if ( err.code != AEROSPIKE_OK ) {
		goto CLEANUP;
	}

	success = aerospike_create_user(self->as, policy, user, password, (const char**)roles, roles_size);
	
CLEANUP:
	for(int i = 0; i < roles_size; i++) {
		if( roles[i] != NULL)
			cf_free(roles[i]);
	}

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		return PyLong_FromLong(-1);
	}

	if(!success) {
		return PyLong_FromLong(0);
	}
	else {	
		return PyLong_FromLong(-1);
	}
}


PyObject * AerospikeClient_drop_user( AerospikeClient *self, PyObject *args ) 
{
	as_error err;
	as_error_init(&err);

	PyObject * py_policy = NULL;
	PyObject * py_user = NULL;
	
	if ( PyArg_ParseTuple(args, "OO:admin_drop_user", &py_policy, &py_user) == false ) {
		return PyLong_FromLong(-1);
	}
 
	as_policy_admin *policy, policy_struct;
	pyobject_to_policy_admin(&err, py_policy, &policy_struct, &policy);
	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		return PyLong_FromLong(-1);
	}
	
	char *user;
	if( PyString_Check(py_user) ) {
		user = PyString_AsString(py_user);		
	}
	else {
		return PyLong_FromLong(-1);
	}

	int success = -1;
	success = aerospike_drop_user(self->as, policy, user);
	if(!success) {
		return PyLong_FromLong(0);	
	}
	else {
		return PyLong_FromLong(-1);
	}
}


PyObject * AerospikeClient_set_password( AerospikeClient *self, PyObject *args )
{
	as_error err;
	as_error_init(&err);

	PyObject * py_policy = NULL;
	PyObject * py_user = NULL;
	PyObject * py_password = NULL;
	
	if ( PyArg_ParseTuple( args, "OOO:admin_set_password", &py_policy, &py_user, &py_password) == false ) {
		return PyLong_FromLong(-1);
	}

	as_policy_admin *policy, policy_struct;
	pyobject_to_policy_admin(&err, py_policy, &policy_struct, &policy);
	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		return PyLong_FromLong(-1);
	}
	
	char *user;
	if( PyString_Check(py_user) ) {
		user = PyString_AsString(py_user);		
	}
	else {
		return PyLong_FromLong(-1);
	}

	char *password;
	if( PyString_Check(py_password) ) {
		password = PyString_AsString(py_password);		
	}
	else {
		return PyLong_FromLong(-1);
	}

	int success = -1;
	success = aerospike_set_password( self->as, policy, user, password );
	if(!success) {
		return PyLong_FromLong(0);	
	}
	else {
		return PyLong_FromLong(-1);
	}
}


PyObject * AerospikeClient_change_password( AerospikeClient *self, PyObject *args )
{
	as_error err;
	as_error_init(&err);

	PyObject * py_policy = NULL;
	PyObject * py_user = NULL;
	PyObject * py_password = NULL;
	
	if ( PyArg_ParseTuple(args,"OOO:admin_change_password", 
		&py_policy, &py_user, &py_password) == false ) {
		return PyLong_FromLong(-1);
	}

	as_policy_admin *policy, policy_struct;
	pyobject_to_policy_admin(&err, py_policy, &policy_struct, &policy);
	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		return PyLong_FromLong(-1);
	}
	
	char *user;
	if( PyString_Check(py_user) ) {
		user = PyString_AsString(py_user);		
	}
	else {
		return PyLong_FromLong(-1);
	}

	char *password;
	if( PyString_Check(py_password) ) {
		password = PyString_AsString(py_password);		
	}
	else {
		return PyLong_FromLong(-1);
	}

	int success = -1;
	success = aerospike_change_password( self->as, policy, user, password );
	if(!success) {
		return PyLong_FromLong(0);	
	}
	else {
		return PyLong_FromLong(-1);
	}
}


PyObject * AerospikeClient_grant_roles( AerospikeClient *self, PyObject *args )
{
	int success = -1;
	as_error err;
	as_error_init(&err);

	PyObject * py_policy = NULL;
	PyObject * py_user = NULL;
	PyObject * py_roles = NULL;
	PyObject * py_roles_size = NULL;

	if ( PyArg_ParseTuple(args, "OOOO:admin_grant_roles", 
		&py_policy, &py_user, &py_roles, &py_roles_size) == false ) {
		return PyLong_FromLong(-1);
	}

	char *user;
	if( PyString_Check(py_user) ) {
		user = PyString_AsString(py_user);		
	}
	else {
		return PyLong_FromLong(-1);
	}

	int roles_size = (int)PyInt_AsLong(py_roles_size); 
	
	char *roles[roles_size];
	for(int i = 0; i < roles_size; i++) {
		roles[i] = cf_malloc(sizeof(char) * AS_ROLE_SIZE);
	}

	pyobject_to_strArray(&err, py_roles, roles);
	if ( err.code != AEROSPIKE_OK ) {
		goto CLEANUP;
	}

	as_policy_admin *policy, policy_struct;
	pyobject_to_policy_admin(&err, py_policy, &policy_struct, &policy);
	if ( err.code != AEROSPIKE_OK ) {
		goto CLEANUP;
	}

	success = aerospike_grant_roles(self->as, policy, user, (const char**)roles, roles_size);

CLEANUP:
	for(int i = 0; i < roles_size; i++) {
		if(roles[i] != NULL)
			cf_free(roles[i]);
	}

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		return PyLong_FromLong(-1);
	}

	if(!success) {
		return PyLong_FromLong(0);
	}
	else {
		return PyLong_FromLong(-1);
	}
}


PyObject * AerospikeClient_revoke_roles( AerospikeClient *self, PyObject *args )
{
	int success = -1;
	as_error err;
	as_error_init(&err);

	PyObject * py_policy = NULL;
	PyObject * py_user = NULL;
	PyObject * py_roles = NULL;
	PyObject * py_roles_size = NULL;

	if ( PyArg_ParseTuple(args, "OOOO:admin_revoke_roles", 
		&py_policy, &py_user, &py_roles, &py_roles_size) == false ) {
		return PyLong_FromLong(-1);
	}

	char *user;
	if( PyString_Check(py_user) ) {
		user = PyString_AsString(py_user);		
	}
	else {
		return PyLong_FromLong(-1);
	}

	int roles_size = (int)PyInt_AsLong(py_roles_size); 
	
	char *roles[roles_size];
	for(int i = 0; i < roles_size; i++) {
		roles[i] = cf_malloc(sizeof(char) * AS_ROLE_SIZE);
	}

	pyobject_to_strArray(&err, py_roles, roles);
	if ( err.code != AEROSPIKE_OK ) {
		goto CLEANUP;
	}
	
	as_policy_admin *policy, policy_struct;
	pyobject_to_policy_admin(&err, py_policy, &policy_struct, &policy);
	if ( err.code != AEROSPIKE_OK ) {
		goto CLEANUP;
	}

	success = aerospike_revoke_roles(self->as, policy, user, (const char**)roles, roles_size);

CLEANUP:
	for(int i = 0; i < roles_size; i++) {
		if(roles[i] != NULL)
			cf_free(roles[i]);
	}

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		return PyLong_FromLong(-1);
	}

	if(!success) {
		return PyLong_FromLong(0);
	}
	else {
		return PyLong_FromLong(-1);
	}
}


PyObject * AerospikeClient_replace_roles( AerospikeClient *self, PyObject *args )
{
	int success = -1;
	as_error err;
	as_error_init(&err);

	PyObject * py_policy = NULL;
	PyObject * py_user = NULL;
	PyObject * py_roles = NULL;
	PyObject * py_roles_size = NULL;

	if ( PyArg_ParseTuple(args, "OOOO:admin_replace_roles", 
		&py_policy, &py_user, &py_roles, &py_roles_size) == false ) {
		return PyLong_FromLong(-1);
	}

	char *user;
	if( PyString_Check(py_user) ) {
		user = PyString_AsString(py_user);		
	}
	else {
		return PyLong_FromLong(-1);
	}

	int roles_size = (int)PyInt_AsLong(py_roles_size); 
	
	char *roles[roles_size];
	for( int i = 0; i < roles_size; i++ ) {
		roles[i] = cf_malloc(sizeof(char) * AS_ROLE_SIZE);
	}

	pyobject_to_strArray(&err, py_roles, roles);
	if ( err.code != AEROSPIKE_OK ) {
		goto CLEANUP;
	}

	as_policy_admin *policy, policy_struct;
	pyobject_to_policy_admin(&err, py_policy, &policy_struct, &policy);
	if ( err.code != AEROSPIKE_OK ) {
		goto CLEANUP;
	}

	success = aerospike_replace_roles(self->as, policy, user, (const char**)roles, roles_size);
	
CLEANUP:
	for(int i = 0; i < roles_size; i++) {
		if(roles[i] != NULL)
			cf_free(roles[i]);
	}
	
	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		return PyLong_FromLong(-1);
	}

	if(!success) {
		return PyLong_FromLong(0);
	}
	else {
		return PyLong_FromLong(-1);
	}
}

PyObject * AerospikeClient_query_user( AerospikeClient * self, PyObject * args )
{
	as_error err;
	as_error_init(&err);

	PyObject * py_policy = NULL;
	PyObject * py_user = NULL;

	if ( PyArg_ParseTuple(args, "OO:admin_query_user", &py_policy, &py_user) == false ) {
		return PyLong_FromLong(-1);
	}

	as_policy_admin *policy, policy_struct;
	pyobject_to_policy_admin(&err, py_policy, &policy_struct, &policy);
	if ( err.code != AEROSPIKE_OK ) {
		goto CLEANUP;
	}

	char *user;
	if( PyString_Check(py_user) ) {
		user = PyString_AsString(py_user);		
	}
	else {
		return PyLong_FromLong(-1);
	}

	as_user_roles *user_roles = NULL;
	int success = -1;
	success = aerospike_query_user(self->as, policy, user, &user_roles);
	if(success) {
		return PyLong_FromLong(-1);
	}

	PyObject *py_user_roles;
	as_user_roles_to_pyobject(&err, user_roles, &py_user_roles);

	if( !PyList_Check(py_user_roles)) {
		if( user_roles != NULL )
			as_user_roles_destroy(user_roles);

		return PyLong_FromLong(-1);
	}

CLEANUP:
	if( user_roles != NULL )
		as_user_roles_destroy(user_roles);
	
	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		return PyLong_FromLong(-1);
	}

	return py_user_roles;
}

PyObject * AerospikeClient_query_users( AerospikeClient * self, PyObject * args )
{
	as_error err;
	as_error_init(&err);

	PyObject * py_policy = NULL;
	
	if ( PyArg_ParseTuple(args, "O:admin_query_users", &py_policy) == false ) {
		return PyLong_FromLong(-1);
	}

	as_policy_admin *policy, policy_struct;
	pyobject_to_policy_admin(&err, py_policy, &policy_struct, &policy);
	if ( err.code != AEROSPIKE_OK ) {
		goto CLEANUP;
	}
	
	int users;
	as_user_roles **user_roles;
	int success = -1;
	success = aerospike_query_users(self->as, policy, &user_roles, &users);
	if(success) {	
		return PyLong_FromLong(-1);
	}

	PyObject *py_user_roles;
	as_user_roles_array_to_pyobject(&err, user_roles, &py_user_roles, users);
	if( err.code != AEROSPIKE_OK ) {
		as_user_roles_destroy_array(user_roles, users);
		goto CLEANUP;
	}

CLEANUP:
	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		return PyLong_FromLong(-1);
	}
	
	as_user_roles_destroy_array(user_roles, users);

	return py_user_roles;
}


