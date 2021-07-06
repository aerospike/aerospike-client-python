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

#pragma once

#include <Python.h>
#include <stdbool.h>

#include "types.h"
#include "macros.h"

#define TRACE() printf("%s:%d\n", __FILE__, __LINE__)

/*******************************************************************************
 * ADMIN OPERATIONS
 ******************************************************************************/
/**
 * Create a new user in the database.
 *
 *		client.admin_create_user(policy, user, password, roles, roles_size)
 *
 */
PyObject *AerospikeClient_Admin_Create_User(AerospikeClient *self,
											PyObject *args, PyObject *kwds);

/**
 * Drop an existing user from the database.
 *
 *		client.admin_drop_user(policy, user)
 *
 */
PyObject *AerospikeClient_Admin_Drop_User(AerospikeClient *self, PyObject *args,
										  PyObject *kwds);

/**
 * Set the password for an existing user.
 *
 *		client.admin_set_password(policy, user, password)
 *
 */
PyObject *AerospikeClient_Admin_Set_Password(AerospikeClient *self,
											 PyObject *args, PyObject *kwds);

/**
 * Change the password for an existing user.
 *
 *		client.admin_change_password(policy, user, password)
 *
 */
PyObject *AerospikeClient_Admin_Change_Password(AerospikeClient *self,
												PyObject *args, PyObject *kwds);

/**
 * Grant security roles to an existing user.
 *
 *		client.admin_grant_roles(policy, user, roles, roles_size)
 *
 */
PyObject *AerospikeClient_Admin_Grant_Roles(AerospikeClient *self,
											PyObject *args, PyObject *kwds);

/**
 * Revoke the roles specified from an existing user.
 *
 *		client.admin_revoke_roles(policy, user, roles, roles_size)
 *
 */
PyObject *AerospikeClient_Admin_Revoke_Roles(AerospikeClient *self,
											 PyObject *args, PyObject *kwds);

/**
 * Retrieve the roles of an existing user.
 *
 *		client.admin_query_user(policy, user)
 *
 */
PyObject *AerospikeClient_Admin_Query_User(AerospikeClient *self,
										   PyObject *args, PyObject *kwds);

/**
 * Retrieve the info of an existing user.
 *
 *		client.admin_query_user_info(policy, user)
 *
 */
PyObject *AerospikeClient_Admin_Query_User_Info(AerospikeClient *self,
												PyObject *args, PyObject *kwds);

/**
 * Retrieve the roles for all existing users in the database.
 *
 *		client.admin_query_users(policy)
 *
 */
PyObject *AerospikeClient_Admin_Query_Users(AerospikeClient *self,
											PyObject *args, PyObject *kwds);

/**
 * Retrieve the info for all existing users in the database.
 *
 *		client.admin_query_users_info(policy)
 *
 */
PyObject *AerospikeClient_Admin_Query_Users_Info(AerospikeClient *self,
												 PyObject *args,
												 PyObject *kwds);

/**
 * Create a new role in the database.
 *
 *		client.admin_create_role(role, privileges, ns, set, policy)
 *
 */
PyObject *AerospikeClient_Admin_Create_Role(AerospikeClient *self,
											PyObject *args, PyObject *kwds);
/**
 * Drop a user defined role in the database.
 *
 *		client.admin_drop_role(role, policy)
 *
 */
PyObject *AerospikeClient_Admin_Drop_Role(AerospikeClient *self, PyObject *args,
										  PyObject *kwds);
/**
 * Grant privileges to a user defined role in the database.
 *
 *		client.admin_grant_privileges(role, privileges, ns, set, policy)
 *
 */
PyObject *AerospikeClient_Admin_Grant_Privileges(AerospikeClient *self,
												 PyObject *args,
												 PyObject *kwds);
/**
 * Revoke privileges from a user defined role in the database.
 *
 *		client.admin_revoke_privileges(role, privileges, ns, set, policy)
 *
 */
PyObject *AerospikeClient_Admin_Revoke_Privileges(AerospikeClient *self,
												  PyObject *args,
												  PyObject *kwds);
/**
 * Query a user defined role in the database.
 *
 *		client.admin_query_role(role, policy)
 *
 */
PyObject *AerospikeClient_Admin_Query_Role(AerospikeClient *self,
										   PyObject *args, PyObject *kwds);
/**
 * Query all user defined roles in the database.
 *
 *		client.admin_query_roles(policy)
 *
 */
PyObject *AerospikeClient_Admin_Query_Roles(AerospikeClient *self,
											PyObject *args, PyObject *kwds);
/**
 * Get a user defined role in the database.
 *
 *		client.admin_get_role(role, policy)
 *
 */
PyObject *AerospikeClient_Admin_Get_Role(AerospikeClient *self, PyObject *args,
										 PyObject *kwds);
/**
 * Get all user defined roles in the database.
 *
 *		client.admin_get_roles(policy)
 *
 */
PyObject *AerospikeClient_Admin_Get_Roles(AerospikeClient *self, PyObject *args,
										  PyObject *kwds);
/**
 * Set read and write quotas for a user defined role in the database.
 *
 *		client.admin_set_quotas(role, read_quota, write_quota, policy)
 *
 */
PyObject *AerospikeClient_Admin_Set_Quotas(AerospikeClient *self,
										   PyObject *args, PyObject *kwds);
/**
 * Set IP whitelist for a user defined role in the database.
 *
 *		client.admin_set_whitelist(role, whitelist, policy)
 *
 */
PyObject *AerospikeClient_Admin_Set_Whitelist(AerospikeClient *self,
											  PyObject *args, PyObject *kwds);