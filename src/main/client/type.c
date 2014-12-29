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
#include <aerospike/as_config.h>
#include <aerospike/as_error.h>
#include <aerospike/as_policy.h>

#include "admin.h"
#include "client.h"
#include "policy.h"

/*******************************************************************************
 * PYTHON TYPE METHODS
 ******************************************************************************/

static PyMethodDef AerospikeClient_Type_Methods[] = {

	// CONNECTION OPERATIONS

	{"connect",
		(PyCFunction) AerospikeClient_Connect, METH_VARARGS | METH_KEYWORDS,
		"Opens connection(s) to the cluster."},
	{"close",
		(PyCFunction) AerospikeClient_Close, METH_VARARGS | METH_KEYWORDS,
		"Close the connection(s) to the cluster."},

	// ADMIN OPERATIONS

	{"admin_create_user",
		(PyCFunction) AerospikeClient_Admin_Create_User, METH_VARARGS | METH_KEYWORDS,
		"Create a new user."},
	{"admin_drop_user",	(PyCFunction) AerospikeClient_Admin_Drop_User, METH_VARARGS | METH_KEYWORDS,
		"Drop a user."},
	{"admin_set_password",
		(PyCFunction) AerospikeClient_Admin_Set_Password,	METH_VARARGS | METH_KEYWORDS,
		"Set password"},
	{"admin_change_password",
		(PyCFunction) AerospikeClient_Admin_Change_Password, METH_VARARGS | METH_KEYWORDS,
		"Change password."},
	{"admin_grant_roles",
		(PyCFunction) AerospikeClient_Admin_Grant_Roles, METH_VARARGS | METH_KEYWORDS,
		"Grant Roles."},
	{"admin_revoke_roles",
		(PyCFunction) AerospikeClient_Admin_Revoke_Roles,	METH_VARARGS | METH_KEYWORDS,
		"Revoke roles"},
	{"admin_replace_roles",
		(PyCFunction) AerospikeClient_Admin_Replace_Roles, METH_VARARGS | METH_KEYWORDS,
		"Replace roles."},
	{"admin_query_user",
		(PyCFunction) AerospikeClient_Admin_Query_User, METH_VARARGS | METH_KEYWORDS,
		"Query a user for roles."},
	{"admin_query_users",	(PyCFunction) AerospikeClient_Admin_Query_Users, METH_VARARGS | METH_KEYWORDS,
		"Query all users for roles."},

	// KVS OPERATIONS

	{"exists",
		(PyCFunction) AerospikeClient_Exists, METH_VARARGS | METH_KEYWORDS,
		"Check the existence of a record in the database."},
	{"get",
		(PyCFunction) AerospikeClient_Get, METH_VARARGS | METH_KEYWORDS,
		"Read a record from the database."},
	{"select",
		(PyCFunction) AerospikeClient_Select, METH_VARARGS | METH_KEYWORDS,
		"Project specific bins of a record from the database."},
	{"put",
		(PyCFunction) AerospikeClient_Put, METH_VARARGS | METH_KEYWORDS,
		"Write a record into the database."},
	{"remove",
		(PyCFunction) AerospikeClient_Remove, METH_VARARGS | METH_KEYWORDS,
		"Remove a record from the database."},
	{"apply",
		(PyCFunction) AerospikeClient_Apply, METH_VARARGS | METH_KEYWORDS,
		"Apply a UDF on a record in the database."},
    {"append",
        (PyCFunction) AerospikeClient_Append, METH_VARARGS | METH_KEYWORDS,
        "Appends a string to the string value in a bin"},
	{"prepend",
		(PyCFunction) AerospikeClient_Prepend, METH_VARARGS | METH_KEYWORDS,
		"Prepend a record to the database"},
	{"touch",
		(PyCFunction) AerospikeClient_Touch, METH_VARARGS | METH_KEYWORDS,
		"Touch a record in the database"},
	{"increment",
		(PyCFunction) AerospikeClient_Increment, METH_VARARGS | METH_KEYWORDS,
		"Increments a numeric value in a bin"},
	{"operate",
		(PyCFunction) AerospikeClient_Operate, METH_VARARGS | METH_KEYWORDS,
		"Performs operate operation"},

	// Deprecated key-based API

	{"key",
		(PyCFunction) AerospikeClient_Key, METH_VARARGS | METH_KEYWORDS,
		"**[DEPRECATED]** Create a new Key object for performing key operations."},

	// QUERY OPERATIONS

	{"query",
		(PyCFunction) AerospikeClient_Query, METH_VARARGS | METH_KEYWORDS,
		"Create a new Query object for peforming queries."},

	// SCAN OPERATIONS

	{"scan",
		(PyCFunction) AerospikeClient_Scan, METH_VARARGS | METH_KEYWORDS,
		"Create a new Scan object for performing scans."},

	// INFO OPERATIONS
	{"info",
		(PyCFunction) AerospikeClient_Info, METH_VARARGS | METH_KEYWORDS,
		"Send an info request to the cluster."},
	{"info_node",
		(PyCFunction) AerospikeClient_InfoNode, METH_VARARGS | METH_KEYWORDS,
		"Send an info request to the cluster."},
	{"get_nodes",
		(PyCFunction) AerospikeClient_GetNodes, METH_VARARGS | METH_KEYWORDS,
		"Gets information about the nodes of the cluster."},

	// UDF OPERATIONS

	{"udf_put",
		(PyCFunction)AerospikeClient_UDF_Put,	METH_VARARGS | METH_KEYWORDS,
		"Registers a UDF"},
	{"udf_remove",
		(PyCFunction)AerospikeClient_UDF_Remove, METH_VARARGS | METH_KEYWORDS,
		"De-registers a UDF"},
	{"udf_list",
		(PyCFunction)AerospikeClient_UDF_List, METH_VARARGS | METH_KEYWORDS,
		"Lists the UDFs"},

	// SECONDARY INDEX OPERATONS

	{"index_integer_create",
		(PyCFunction)AerospikeClient_Index_Integer_Create, METH_VARARGS | METH_KEYWORDS,
		"Creates a secondary integer index"},
	{"index_string_create",
		(PyCFunction)AerospikeClient_Index_String_Create,	METH_VARARGS | METH_KEYWORDS,
		"Creates a secondary string index"},
	{"index_remove",
		(PyCFunction)AerospikeClient_Index_Remove, METH_VARARGS | METH_KEYWORDS,
		"Remove a secondary index"},

	// BATCH OPERATIONS
	{"get_many",
		(PyCFunction)AerospikeClient_Get_Many, METH_VARARGS | METH_KEYWORDS,
		"Get many records at a time."},
	{"exists_many",
		(PyCFunction)AerospikeClient_Exists_Many, METH_VARARGS | METH_KEYWORDS,
		"Get many records at a time."},

	{NULL}
};

/*******************************************************************************
 * PYTHON TYPE HOOKS
 ******************************************************************************/

static PyObject * AerospikeClient_Type_New(PyTypeObject * type, PyObject * args, PyObject * kwds)
{
	AerospikeClient * self = NULL;

	self = (AerospikeClient *) type->tp_alloc(type, 0);

	if ( self == NULL ) {
		return NULL;
	}

	return (PyObject *) self;
}

static int AerospikeClient_Type_Init(AerospikeClient * self, PyObject * args, PyObject * kwds)
{
	PyObject * py_config = NULL;

	static char * kwlist[] = {"config", NULL};

	if ( PyArg_ParseTupleAndKeywords(args, kwds, "O:client", kwlist, &py_config) == false ) {
		return -1;
	}

	if ( ! PyDict_Check(py_config) ) {
		return -1;
	}

	as_config config;
	as_config_init(&config);

	bool lua_system_path = FALSE;
	bool lua_user_path = FALSE;

	PyObject * py_lua = PyDict_GetItemString(py_config, "lua");
	if ( py_lua && PyDict_Check(py_lua) ) {

		PyObject * py_lua_system_path = PyDict_GetItemString(py_lua, "system_path");
		if ( py_lua_system_path && PyString_Check(py_lua_system_path) ) {
			lua_system_path = TRUE;
			memcpy(config.lua.system_path, PyString_AsString(py_lua_system_path), AS_CONFIG_PATH_MAX_LEN);
		}

		PyObject * py_lua_user_path = PyDict_GetItemString(py_lua, "user_path");
		if ( py_lua_user_path && PyString_Check(py_lua_user_path) ) {
			lua_user_path = TRUE;
			memcpy(config.lua.user_path, PyString_AsString(py_lua_user_path), AS_CONFIG_PATH_MAX_LEN);
		}

	}

	if ( ! lua_system_path ) {

		PyObject * py_prefix = PySys_GetObject("prefix");
		if ( py_prefix && PyString_Check(py_prefix) ) {
			char * prefix = PyString_AsString(py_prefix);
			size_t prefix_len = strlen(prefix);

			char system_path[AS_CONFIG_PATH_MAX_LEN] = {0};
			memcpy(system_path, prefix, strlen(prefix));
			memcpy(system_path + prefix_len, "/aerospike/lua", AS_CONFIG_PATH_MAX_LEN - prefix_len);
			system_path[prefix_len + strlen("/aerospike/lua")] = '\0';

			struct stat info;

			if( stat( system_path, &info ) == 0 && (info.st_mode & S_IFDIR) ) {
				memcpy(config.lua.system_path, system_path, AS_CONFIG_PATH_MAX_LEN);
			}
			else {
				memcpy(system_path + prefix_len, "/local/aerospike/lua", AS_CONFIG_PATH_MAX_LEN - prefix_len);
				system_path[prefix_len + strlen("/local/aerospike/lua")] = '\0';

				if( stat( system_path, &info ) == 0 && (info.st_mode & S_IFDIR) ) {
					memcpy(config.lua.system_path, system_path, AS_CONFIG_PATH_MAX_LEN);
				}
				else {
					config.lua.system_path[0] = '\0';
				}
			}
		}
	}

	if ( ! lua_user_path ) {
		memcpy(config.lua.user_path, ".", AS_CONFIG_PATH_MAX_LEN);
	}

	PyObject * py_hosts = PyDict_GetItemString(py_config, "hosts");
	if ( py_hosts && PyList_Check(py_hosts) ) {
		int size = (int) PyList_Size(py_hosts);
		for ( int i = 0; i < size && i < AS_CONFIG_HOSTS_SIZE; i++ ) {
			PyObject * py_host = PyList_GetItem(py_hosts, i);
			if ( PyTuple_Check(py_host) && PyTuple_Size(py_host) == 2 ) {
				PyObject * py_addr = PyTuple_GetItem(py_host,0);
				PyObject * py_port = PyTuple_GetItem(py_host,1);
				if ( PyString_Check(py_addr) ) {
					char * addr = PyString_AsString(py_addr);
					config.hosts[i].addr = addr;
				}
				if ( PyInt_Check(py_port) ) {
					config.hosts[i].port = (uint16_t) PyInt_AsLong(py_port);
				}
				else if ( PyLong_Check(py_port) ) {
					config.hosts[i].port = (uint16_t) PyLong_AsLong(py_port);
				}
			}
			else if ( PyString_Check(py_host) ) {
				char * addr = PyString_AsString(py_host);
				config.hosts[i].addr = addr;
				config.hosts[i].port = 3000;
			}
		}
	}


	as_policies_init(&config.policies);

	self->as = aerospike_new(&config);

	return 0;
}

static void AerospikeClient_Type_Dealloc(PyObject * self)
{
	self->ob_type->tp_free((PyObject *) self);
}

/*******************************************************************************
 * PYTHON TYPE DESCRIPTOR
 ******************************************************************************/

static PyTypeObject AerospikeClient_Type = {
	PyObject_HEAD_INIT(NULL)

		.ob_size			= 0,
	.tp_name			= "aerospike.Client",
	.tp_basicsize		= sizeof(AerospikeClient),
	.tp_itemsize		= 0,
	.tp_dealloc			= (destructor) AerospikeClient_Type_Dealloc,
	.tp_print			= 0,
	.tp_getattr			= 0,
	.tp_setattr			= 0,
	.tp_compare			= 0,
	.tp_repr			= 0,
	.tp_as_number		= 0,
	.tp_as_sequence		= 0,
	.tp_as_mapping		= 0,
	.tp_hash			= 0,
	.tp_call			= 0,
	.tp_str				= 0,
	.tp_getattro		= 0,
	.tp_setattro		= 0,
	.tp_as_buffer		= 0,
	.tp_flags			= Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
	.tp_doc				=
		"The Client class manages the connections and trasactions against\n"
		"an Aerospike cluster.\n",
	.tp_traverse		= 0,
	.tp_clear			= 0,
	.tp_richcompare		= 0,
	.tp_weaklistoffset	= 0,
	.tp_iter			= 0,
	.tp_iternext		= 0,
	.tp_methods			= AerospikeClient_Type_Methods,
	.tp_members			= 0,
	.tp_getset			= 0,
	.tp_base			= 0,
	.tp_dict			= 0,
	.tp_descr_get		= 0,
	.tp_descr_set		= 0,
	.tp_dictoffset		= 0,
	.tp_init			= (initproc) AerospikeClient_Type_Init,
	.tp_alloc			= 0,
	.tp_new				= AerospikeClient_Type_New
};

/*******************************************************************************
 * PUBLIC FUNCTIONS
 ******************************************************************************/

PyTypeObject * AerospikeClient_Ready()
{
	return PyType_Ready(&AerospikeClient_Type) == 0 ? &AerospikeClient_Type : NULL;
}

AerospikeClient * AerospikeClient_New(PyObject * parent, PyObject * args, PyObject * kwds)
{
	AerospikeClient * self = (AerospikeClient *) AerospikeClient_Type.tp_new(&AerospikeClient_Type, args, kwds);
	if ( AerospikeClient_Type.tp_init((PyObject *) self, args, kwds) == 0 ){
		// Initialize connection flag
		return self;
	}
	else {
		as_error err;
		as_error_init(&err);
		as_error_update(&err, AEROSPIKE_ERR, "Parameters are incorrect");
		PyObject * py_err = NULL;
		error_to_pyobject( &err, &py_err);
		PyErr_SetObject( PyExc_Exception, py_err);
		Py_DECREF(py_err);
		return NULL;
	}
}
