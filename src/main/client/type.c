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
#include <structmember.h>
#include <stdbool.h>
#include <unistd.h>

#include <aerospike/aerospike.h>
#include <aerospike/as_config.h>
#include <aerospike/as_error.h>
#include <aerospike/as_policy.h>

#include "admin.h"
#include "client.h"
#include "policy.h"
#include "conversions.h"
#include "exceptions.h"
#include "tls_config.h"
#include "policy_config.h"

static int set_rack_aware_config(as_config *conf, PyObject *config_dict);
static int set_use_services_alternate(as_config *conf, PyObject *config_dict);

enum {
	INIT_SUCCESS,
	INIT_NO_CONFIG_ERR,
	INIT_CONFIG_TYPE_ERR,
	INIT_LUA_USER_ERR,
	INIT_LUA_SYS_ERR,
	INIT_HOST_TYPE_ERR,
	INIT_EMPTY_HOSTS_ERR,
	INIT_INVALID_ADRR_ERR,
	INIT_SERIALIZE_ERR,
	INIT_DESERIALIZE_ERR,
	INIT_COMPRESSION_ERR,
	INIT_POLICY_PARAM_ERR,
	INIT_INVALID_AUTHMODE_ERR
};

/*******************************************************************************
 * PYTHON DOC METHODS
 ******************************************************************************/

PyDoc_STRVAR(connect_doc, "connect([username, password])\n\
\n\
Connect to the cluster. The optional username and password only apply when \
connecting to the Enterprise Edition of Aerospike.");

PyDoc_STRVAR(exists_doc, "exists(key[, policy]) -> (key, meta)\n\
\n\
Check if a record with a given key exists in the cluster and return the record \
as a tuple() consisting of key and meta. If the record does not exist the meta data will be None.");

PyDoc_STRVAR(get_doc, "get(key[, policy]) -> (key, meta, bins)\n\
\n\
Read a record with a given key, and return the record as a tuple() consisting of key, meta and bins.");

PyDoc_STRVAR(get_async_doc,
			 "get_async(get_callback, key[, policy]) -> (key, meta, bins)\n\
\n\
Read a record asynchronously with a given key, and return the record as a tuple() consisting of key, meta and bins.");

PyDoc_STRVAR(select_doc, "select(key, bins[, policy]) -> (key, meta, bins)\n\
\n\
Read a record with a given key, and return the record as a tuple() consisting of key, meta and bins, \
with the specified bins projected. \
Prior to Aerospike server 3.6.0, if a selected bin does not exist its value will be None. \
Starting with 3.6.0, if a bin does not exist it will not be present in the returned Record Tuple.");

PyDoc_STRVAR(put_doc, "put(key, bins[, meta[, policy[, serializer]]])\n\
\n\
Write a record with a given key to the cluster.");

PyDoc_STRVAR(put_async_doc, "put(key, bins[, meta[, policy[, serializer]]])\n\
\n\
Write a record asynchronously with a given key to the cluster.");

PyDoc_STRVAR(remove_doc, "remove(key[, policy])\n\
\n\
Remove a record matching the key from the cluster.");

PyDoc_STRVAR(apply_doc, "apply(key, module, function, args[, policy])\n\
\n\
Apply a registered (see udf_put()) record UDF to a particular record.");

PyDoc_STRVAR(remove_bin_doc, "remove_bin(key, list[, meta[, policy]])\n\
\n\
Remove a list of bins from a record with a given key. \
Equivalent to setting those bins to aerospike.null() with a put().");

PyDoc_STRVAR(append_doc, "append(key, bin, val[, meta[, policy]])\n\
\n\
Append the string val to the string value in bin.");

PyDoc_STRVAR(prepend_doc, "prepend(key, bin, val[, meta[, policy]])\n\
\n\
Prepend the string value in bin with the string val.");

PyDoc_STRVAR(touch_doc, "touch(key[, val=0[, meta[, policy]]])\n\
\n\
Touch the given record, resetting its time-to-live and incrementing its generation.");

PyDoc_STRVAR(increment_doc, "increment(key, bin, offset[, meta[, policy]])\n\
\n\
Increment the integer value in bin by the integer val.");

PyDoc_STRVAR(operate_doc,
			 "operate(key, list[, meta[, policy]]) -> (key, meta, bins)\n\
\n\
Perform multiple bin operations on a record with a given key, In Aerospike server versions prior to 3.6.0, \
non-existent bins being read will have a None value. \
Starting with 3.6.0 non-existent bins will not be present in the returned Record Tuple. \
The returned record tuple will only contain one entry per bin, \
even if multiple operations were performed on the bin.");

PyDoc_STRVAR(
	operate_ordered_doc,
	"operate_ordered(key, list[, meta[, policy]]) -> (key, meta, bins)\n\
\n\
Perform multiple bin operations on a record with the results being returned as a list of (bin-name, result) tuples. \
The order of the elements in the list will correspond to the order of the operations from the input parameters.");

PyDoc_STRVAR(list_append_doc, "list_append(key, bin, val[, meta[, policy]])\n\
\n\
Append a single element to a list value in bin.");

PyDoc_STRVAR(list_extend_doc, "list_extend(key, bin, items[, meta[, policy]])\n\
\n\
Extend the list value in bin with the given items.");

PyDoc_STRVAR(list_insert_doc,
			 "list_insert(key, bin, index, val[, meta[, policy]])\n\
\n\
Insert an element at the specified index of a list value in bin.");

PyDoc_STRVAR(list_insert_items_doc,
			 "list_insert_items(key, bin, index, items[, meta[, policy]])\n\
\n\
Insert the items at the specified index of a list value in bin.");

PyDoc_STRVAR(list_pop_doc,
			 "list_pop(key, bin, index[, meta[, policy]]) -> val\n\
\n\
Remove and get back a list element at a given index of a list value in bin.");

PyDoc_STRVAR(list_pop_range_doc,
			 "list_pop_range(key, bin, index, count[, meta[, policy]]) -> val\n\
\n\
Remove and get back list elements at a given index of a list value in bin.");

PyDoc_STRVAR(list_remove_doc, "list_remove(key, bin, index[, meta[, policy]])\n\
\n\
Remove a list element at a given index of a list value in bin.");

PyDoc_STRVAR(list_remove_range_doc,
			 "list_remove_range(key, bin, index, count[, meta[, policy]])\n\
\n\
Remove list elements at a given index of a list value in bin.");

PyDoc_STRVAR(list_clear_doc, "list_clear(key, bin[, meta[, policy]])\n\
\n\
Remove all the elements from a list value in bin.");

PyDoc_STRVAR(list_set_doc, "list_set(key, bin, index, val[, meta[, policy]])\n\
\n\
Set list element val at the specified index of a list value in bin.");

PyDoc_STRVAR(list_get_doc,
			 "list_get(key, bin, index[, meta[, policy]]) -> val\n\
\n\
Get the list element at the specified index of a list value in bin.");

PyDoc_STRVAR(list_get_range_doc,
			 "list_get_range(key, bin, index, count[, meta[, policy]]) -> val\n\
\n\
Get the list of count elements starting at a specified index of a list value in bin.");

PyDoc_STRVAR(list_trim_doc,
			 "list_trim(key, bin, index, count[, meta[, policy]]) -> val\n\
\n\
Remove elements from the list which are not within the range starting at the given index plus count.");

PyDoc_STRVAR(list_size_doc, "list_size(key, bin[, meta[, policy]]) -> count\n\
\n\
Count the number of elements in the list value in bin.");

PyDoc_STRVAR(map_set_policy_doc, "map_set_policy(key, bin, map_policy)\n\
\n\
Set the map policy for the given bin.");

PyDoc_STRVAR(map_put_doc,
			 "map_put(key, bin, map_key, val[, map_policy[, meta[, policy]]])\n\
\n\
Add the given map_key/value pair to the map record specified by key and bin.");

PyDoc_STRVAR(map_put_items_doc,
			 "map_put_items(key, bin, items[, map_policy[, meta[, policy]]])\n\
\n\
Add the given items dict of key/value pairs to the map record specified by key and bin.");

PyDoc_STRVAR(
	map_increment_doc,
	"map_increment(key, bin, map_key, incr[, map_policy[, meta[, policy]]])\n\
\n\
Increment the value of the map entry by given incr. Map entry is specified by key, bin and map_key.");

PyDoc_STRVAR(
	map_decrement_doc,
	"map_decrement(key, bin, map_key, decr[, map_policy[, meta[, policy]]])\n\
\n\
Decrement the value of the map entry by given decr. Map entry is specified by key, bin and map_key.");

PyDoc_STRVAR(map_size_doc, "map_size(key, bin[, meta[, policy]]) -> count\n\
\n\
Return the size of the map specified by key and bin.");

PyDoc_STRVAR(map_clear_doc, "map_clear(key, bin[, meta[, policy]])\n\
\n\
Remove all entries from the map specified by key and bin.");

PyDoc_STRVAR(
	map_remove_by_key_doc,
	"map_remove_by_key(key, bin, map_key, return_type[, meta[, policy]])\n\
\n\
Remove and optionally return first map entry from the map specified by key and bin which matches given map_key.");

PyDoc_STRVAR(
	map_remove_by_key_list_doc,
	"map_remove_by_key_list(key, bin, list, return_type[, meta[, policy]][, meta[, policy]])\n\
\n\
Remove and optionally return map entries from the map specified by key and bin \
which have keys that match the given list of keys.");

PyDoc_STRVAR(
	map_remove_by_key_range_doc,
	"map_remove_by_key_range(key, bin, map_key, range, return_type[, meta[, policy]])\n\
\n\
Remove and optionally return map entries from the map specified by key and bin identified \
by the key range (map_key inclusive, range exclusive).");

PyDoc_STRVAR(
	map_remove_by_value_doc,
	"map_remove_by_value(key, bin, val, return_type[, meta[, policy]])\n\
\n\
Remove and optionally return map entries from the map specified by key and bin which \
have a value matching val parameter.");

PyDoc_STRVAR(
	map_remove_by_value_list_doc,
	"map_remove_by_value_list(key, bin, list, return_type[, meta[, policy]])\n\
\n\
Remove and optionally return map entries from the map specified by key and bin which \
have a value matching the list of values.");

PyDoc_STRVAR(
	map_remove_by_value_range_doc,
	"map_remove_by_value_range(key, bin, val, range, return_type[, meta[, policy]])\n\
\n\
Remove and optionally return map entries from the map specified by key and bin identified \
by the value range (val inclusive, range exclusive).");

PyDoc_STRVAR(
	map_remove_by_index_doc,
	"map_remove_by_index(key, bin, index, return_type[, meta[, policy]])\n\
\n\
Remove and optionally return the map entry from the map specified by key and bin at the given index location.");

PyDoc_STRVAR(
	map_remove_by_index_range_doc,
	"map_remove_by_index_range(key, bin, index, range, return_type[, meta[, policy]])\n\
\n\
Remove and optionally return the map entries from the map specified by key and bin starting at \
the given index location and removing range number of items.");

PyDoc_STRVAR(
	map_remove_by_rank_doc,
	"map_remove_by_rank(key, bin, rank, return_type[, meta[, policy]])\n\
\n\
Remove and optionally return the map entry from the map specified by key and bin \
with a value that has the given rank.");

PyDoc_STRVAR(
	map_remove_by_rank_range_doc,
	"map_remove_by_rank_range(key, bin, rank, range, return_type[, meta[, policy]])\n\
\n\
Remove and optionally return the map entries from the map specified by key and bin which \
have a value rank starting at rank and removing range number of items.");

PyDoc_STRVAR(
	map_get_by_key_doc,
	"map_get_by_key(key, bin, map_key, return_type[, meta[, policy]])\n\
\n\
Return map entry from the map specified by key and bin which has a key that matches the given map_key.");

PyDoc_STRVAR(
	map_get_by_key_range_doc,
	"map_get_by_key_range(key, bin, map_key, range, return_type[, meta[, policy]])\n\
\n\
Return map entries from the map specified by key and bin identified by the key range \
(map_key inclusive, range exclusive).");

PyDoc_STRVAR(map_get_by_value_doc,
			 "map_get_by_value(key, bin, val, return_type[, meta[, policy]])\n\
\n\
Return map entries from the map specified by key and bin which have a value matching val parameter.");

PyDoc_STRVAR(
	map_get_by_value_range_doc,
	"map_get_by_value_range(key, bin, val, range, return_type[, meta[, policy]])\n\
\n\
Return map entries from the map specified by key and bin identified by the value \
range (val inclusive, range exclusive).");

PyDoc_STRVAR(
	map_get_by_value_list_doc,
	"map_get_by_value_range(key, bin, value_list, return_type[, meta[, policy]])\n\
\n\
Return map entries from the map specified by key and bin which contain a value matching one \
of the values in the provided value_list.\n\
Requires Aerospike Server versions >= 3.16.0.1");

PyDoc_STRVAR(
	map_get_by_key_list_doc,
	"map_get_by_value_range(key, bin, key_list, return_type[, meta[, policy]])\n\
\n\
Return map entries from the map specified by key and bin for keys matching those \
in the provided key_list.\n\
Requires Aerospike Server versions >= 3.16.0.1");

PyDoc_STRVAR(
	map_get_by_index_doc,
	"map_get_by_index(key, bin, index, return_type[, meta[, policy]])\n\
\n\
Return the map entry from the map specified by key and bin at the given index location.");

PyDoc_STRVAR(
	map_get_by_index_range_doc,
	"map_get_by_index_range(key, bin, index, range, return_type[, meta[, policy]])\n\
\n\
Return the map entries from the map specified by key and bin starting at the given index \
location and removing range number of items.");

PyDoc_STRVAR(map_get_by_rank_doc,
			 "map_get_by_rank(key, bin, rank, return_type[, meta[, policy]])\n\
\n\
Return the map entry from the map specified by key and bin with a value that has the given rank.");

PyDoc_STRVAR(
	map_get_by_rank_range_doc,
	"map_get_by_rank_range(key, bin, rank, range, return_type[, meta[, policy]])\n\
\n\
Return the map entries from the map specified by key and bin which have a value rank starting \
at rank and removing range number of items.");

PyDoc_STRVAR(query_doc, "query(namespace[, set]) -> Query\n\
\n\
Return a `aerospike.Query` object to be used for executing queries over a specified set \
(which can be omitted or None) in a namespace. \
A query with a None set returns records which are not in any named set. \
This is different than the meaning of a None set in a scan.");

PyDoc_STRVAR(
	query_apply_doc,
	"query_apply(ns, set, predicate, module, function[, args[, policy]]) -> int\n\
\n\
Initiate a background query and apply a record UDF to each record matched by the query.");

PyDoc_STRVAR(job_info_doc, "job_info(job_id, module[, policy]) -> dict\n\
\n\
Return the status of a job running in the background.");

PyDoc_STRVAR(scan_doc, "scan(namespace[, set]) -> Scan\n\
\n\
Return a `aerospike.Scan` object to be used for executing scans over a specified set \
(which can be omitted or None) in a namespace. A scan with a None set returns all the records in the namespace.");

PyDoc_STRVAR(
	scan_apply_doc,
	"scan_apply(ns, set, module, function[, args[, policy[, options,[ block]]]]) -> int\n\
\n\
Initiate a background scan and apply a record UDF to each record matched by the scan.");

PyDoc_STRVAR(scan_info_doc, "scan_info(scan_id) -> dict\n\
\n\
Return the status of a scan running in the background.");

PyDoc_STRVAR(info_doc, "info(command[, hosts[, policy]]) -> {}\n\
\n\
Send an info command to multiple nodes specified in a hosts list.");

PyDoc_STRVAR(
	set_xdr_filter_doc,
	"set_xdr_filter(data_center, namespace, expression_filter[, policy]) -> {}\n\
\n\
Set cluster xdr filter.");

PyDoc_STRVAR(info_all_doc, "info_all(command[, policy]]) -> {}\n\
\n\
Send an info *command* to all nodes in the cluster to which the client is connected.\n\
If any of the individual requests fail, this will raise an exception.");

PyDoc_STRVAR(info_single_node_doc,
			 "info_single_node(command, host[, policy]) -> str\n\
\n\
Send an info command to a single node specified by host.");

PyDoc_STRVAR(info_random_node_doc,
			 "info_random_node(command, [policy]) -> str\n\
\n\
Send an info command to a single random node.");

PyDoc_STRVAR(info_node_doc, "info_node(command, host[, policy]) -> str\n\
\n\
DEPRECATED: Please user info_single_node() instead.\n\
Send an info command to a single node specified by host.");

PyDoc_STRVAR(get_nodes_doc, "get_nodes() -> []\n\
\n\
Return the list of hosts present in a connected cluster.");

PyDoc_STRVAR(get_node_names_doc, "get_node_names() -> []\n\
\n\
Return the list of hosts, including node names, present in a connected cluster.");

PyDoc_STRVAR(udf_put_doc, "udf_put(filename[, udf_type[, policy]])\n\
\n\
Register a UDF module with the cluster.");

PyDoc_STRVAR(udf_remove_doc, "udf_remove(module[, policy])\n\
\n\
Remove a  previously registered UDF module from the cluster.");

PyDoc_STRVAR(udf_list_doc, "udf_list([policy]) -> []\n\
\n\
Return the list of UDF modules registered with the cluster.");

PyDoc_STRVAR(udf_get_doc, "udf_get(module[, language[, policy]]) -> str\n\
\n\
Return the content of a UDF module which is registered with the cluster.");

PyDoc_STRVAR(index_integer_create_doc,
			 "index_integer_create(ns, set, bin, index_name[, policy])\n\
\n\
Create an integer index with index_name on the bin in the specified ns, set.");

PyDoc_STRVAR(index_string_create_doc,
			 "index_string_create(ns, set, bin, index_name[, policy])\n\
\n\
Create a string index with index_name on the bin in the specified ns, set.");

PyDoc_STRVAR(index_remove_doc, "index_remove(ns, index_name[, policy])\n\
\n\
Remove the index with index_name from the namespace.");

PyDoc_STRVAR(
	index_list_create_doc,
	"index_list_create(ns, set, bin, index_datatype, index_name[, policy])\n\
\n\
Create an index named index_name for numeric, string or GeoJSON values (as defined by index_datatype) \
on records of the specified ns, set whose bin is a list.");

PyDoc_STRVAR(
	index_map_keys_create_doc,
	"index_map_keys_create(ns, set, bin, index_datatype, index_name[, policy])\n\
\n\
Create an index named index_name for numeric, string or GeoJSON values (as defined by index_datatype) \
on records of the specified ns, set whose bin is a map. The index will include the keys of the map.");

PyDoc_STRVAR(
	index_map_values_create_doc,
	"index_map_values_create(ns, set, bin, index_datatype, index_name[, policy])\n\
\n\
Create an index named index_name for numeric, string or GeoJSON values (as defined by index_datatype) \
on records of the specified ns, set whose bin is a map. The index will include the values of the map.");

PyDoc_STRVAR(index_geo2dsphere_create_doc,
			 "index_geo2dsphere_create(ns, set, bin, index_name[, policy])\n\
\n\
Create a geospatial 2D spherical index with index_name on the bin in the specified ns, set.");

PyDoc_STRVAR(get_many_doc, "get_many(keys[, policy]) -> [ (key, meta, bins)]\n\
\n\
Batch-read multiple records with applying list of opearagtions and returns them as a list. \
Any record that does not exist will have a None value for metadata and status in the record tuple.");

PyDoc_STRVAR(batch_get_ops_doc, "batch_get_ops((list_of_keys, list_of_ops, meta, policy)) -> [ ((, list_of_keys, list_of_ops, meta, policy))]\n\
\n\
Batch-read multiple records, and return them as a list. \
Any record that does not exist will have a None value for metadata and bins in the record tuple.");

PyDoc_STRVAR(select_many_doc,
			 "select_many(keys, bins[, policy]) -> [(key, meta, bins)]\n\
\n\
Batch-read multiple records, and return them as a list. \
Any record that does not exist will have a None value for metadata and bins in the record tuple. \
The bins will be filtered as specified.");

PyDoc_STRVAR(exists_many_doc, "exists_many(keys[, policy]) -> [ (key, meta)]\n\
\n\
Batch-read metadata for multiple keys, and return it as a list. \
Any record that does not exist will have a None value for metadata in the result tuple.");

PyDoc_STRVAR(get_key_digest_doc, "get_key_digest(ns, set, key) -> bytearray\n\
\n\
Calculate the digest of a particular key. See: Key Tuple.");

PyDoc_STRVAR(get_key_partition_id_doc,
			 "get_key_partition_id(ns, set, key) -> int\n\
\n\
Gets the partition ID of given key. See: Key Tuple.");

PyDoc_STRVAR(truncate_doc, "truncate(namespace, set, nanos[, policy])\n\
\n\
Remove records in specified namespace/set efficiently. \
This method is many orders of magnitude faster than deleting records one at a time. \
Works with Aerospike Server versions >= 3.12.\n\
\n\
This asynchronous server call may return before the truncation is complete. \
The user can still write new records after the server returns because new records will have \
last update times greater than the truncate cutoff (set at the time of truncate call)");

/*******************************************************************************
 * PYTHON TYPE METHODS
 ******************************************************************************/

static PyMethodDef AerospikeClient_Type_Methods[] = {

	// CONNECTION OPERATIONS

	{"connect", (PyCFunction)AerospikeClient_Connect,
	 METH_VARARGS | METH_KEYWORDS, connect_doc},
	{"close", (PyCFunction)AerospikeClient_Close, METH_VARARGS | METH_KEYWORDS,
	 "Close the connection(s) to the cluster."},
	{"is_connected", (PyCFunction)AerospikeClient_is_connected,
	 METH_VARARGS | METH_KEYWORDS, "Checks current connection state."},
	{"shm_key", (PyCFunction)AerospikeClient_shm_key,
	 METH_VARARGS | METH_KEYWORDS, "Get the shm key of the cluster"},

	// ADMIN OPERATIONS

	{"admin_create_user", (PyCFunction)AerospikeClient_Admin_Create_User,
	 METH_VARARGS | METH_KEYWORDS, "Create a new user."},
	{"admin_drop_user", (PyCFunction)AerospikeClient_Admin_Drop_User,
	 METH_VARARGS | METH_KEYWORDS, "Drop a user."},
	{"admin_set_password", (PyCFunction)AerospikeClient_Admin_Set_Password,
	 METH_VARARGS | METH_KEYWORDS, "Set password"},
	{"admin_change_password",
	 (PyCFunction)AerospikeClient_Admin_Change_Password,
	 METH_VARARGS | METH_KEYWORDS, "Change password."},
	{"admin_grant_roles", (PyCFunction)AerospikeClient_Admin_Grant_Roles,
	 METH_VARARGS | METH_KEYWORDS, "Grant Roles."},
	{"admin_revoke_roles", (PyCFunction)AerospikeClient_Admin_Revoke_Roles,
	 METH_VARARGS | METH_KEYWORDS, "Revoke roles"},
	{"admin_query_user", (PyCFunction)AerospikeClient_Admin_Query_User,
	 METH_VARARGS | METH_KEYWORDS, "Query a user for roles."},
	{"admin_query_user_info",
	 (PyCFunction)AerospikeClient_Admin_Query_User_Info,
	 METH_VARARGS | METH_KEYWORDS,
	 "Query a user for read/write info, connections-in-use and roles."},
	{"admin_query_users", (PyCFunction)AerospikeClient_Admin_Query_Users,
	 METH_VARARGS | METH_KEYWORDS, "Query all users for roles."},
	{"admin_query_users_info",
	 (PyCFunction)AerospikeClient_Admin_Query_Users_Info,
	 METH_VARARGS | METH_KEYWORDS,
	 "Query all users for read/write info, connections-in-use and roles."},
	{"admin_create_role", (PyCFunction)AerospikeClient_Admin_Create_Role,
	 METH_VARARGS | METH_KEYWORDS, "Create a new role."},
	{"admin_drop_role", (PyCFunction)AerospikeClient_Admin_Drop_Role,
	 METH_VARARGS | METH_KEYWORDS, "Drop a new role."},
	{"admin_grant_privileges",
	 (PyCFunction)AerospikeClient_Admin_Grant_Privileges,
	 METH_VARARGS | METH_KEYWORDS, "Grant privileges to a user defined role."},
	{"admin_revoke_privileges",
	 (PyCFunction)AerospikeClient_Admin_Revoke_Privileges,
	 METH_VARARGS | METH_KEYWORDS,
	 "Revoke privileges from a user defined role."},
	{"admin_query_role", (PyCFunction)AerospikeClient_Admin_Query_Role,
	 METH_VARARGS | METH_KEYWORDS, "DEPRECATED Query a user defined role."},
	{"admin_query_roles", (PyCFunction)AerospikeClient_Admin_Query_Roles,
	 METH_VARARGS | METH_KEYWORDS, "DEPRECATED Query all user defined roles."},
	{"admin_get_role", (PyCFunction)AerospikeClient_Admin_Get_Role,
	 METH_VARARGS | METH_KEYWORDS, "Get a user defined role."},
	{"admin_get_roles", (PyCFunction)AerospikeClient_Admin_Get_Roles,
	 METH_VARARGS | METH_KEYWORDS, "Get all user defined roles."},
	{"admin_set_quotas", (PyCFunction)AerospikeClient_Admin_Set_Quotas,
	 METH_VARARGS | METH_KEYWORDS,
	 "Set read and write quotas for a user defined role."},
	{"admin_set_whitelist", (PyCFunction)AerospikeClient_Admin_Set_Whitelist,
	 METH_VARARGS | METH_KEYWORDS, "Set IP whitelist for a user defined role."},

	// KVS OPERATIONS

	{"exists", (PyCFunction)AerospikeClient_Exists,
	 METH_VARARGS | METH_KEYWORDS, exists_doc},
	{"get", (PyCFunction)AerospikeClient_Get, METH_VARARGS | METH_KEYWORDS,
	 get_doc},
	{"get_async", (PyCFunction)AerospikeClient_Get_Async,
	 METH_VARARGS | METH_KEYWORDS, get_async_doc},
	{"select", (PyCFunction)AerospikeClient_Select,
	 METH_VARARGS | METH_KEYWORDS, select_doc},
	{"put", (PyCFunction)AerospikeClient_Put, METH_VARARGS | METH_KEYWORDS,
	 put_doc},
	{"put_async", (PyCFunction)AerospikeClient_Put_Async,
	 METH_VARARGS | METH_KEYWORDS, put_async_doc},
	{"get_key_partition_id", (PyCFunction)AerospikeClient_Get_Key_PartitionID,
	 METH_VARARGS | METH_KEYWORDS, get_key_partition_id_doc},
	{"remove", (PyCFunction)AerospikeClient_Remove,
	 METH_VARARGS | METH_KEYWORDS, remove_doc},
	{"apply", (PyCFunction)AerospikeClient_Apply, METH_VARARGS | METH_KEYWORDS,
	 apply_doc},
	{"remove_bin", (PyCFunction)AerospikeClient_RemoveBin,
	 METH_VARARGS | METH_KEYWORDS, remove_bin_doc},
	{"append", (PyCFunction)AerospikeClient_Append,
	 METH_VARARGS | METH_KEYWORDS, append_doc},
	{"prepend", (PyCFunction)AerospikeClient_Prepend,
	 METH_VARARGS | METH_KEYWORDS, prepend_doc},
	{"touch", (PyCFunction)AerospikeClient_Touch, METH_VARARGS | METH_KEYWORDS,
	 touch_doc},
	{"increment", (PyCFunction)AerospikeClient_Increment,
	 METH_VARARGS | METH_KEYWORDS, increment_doc},
	{"operate", (PyCFunction)AerospikeClient_Operate,
	 METH_VARARGS | METH_KEYWORDS, operate_doc},
	{"operate_ordered", (PyCFunction)AerospikeClient_OperateOrdered,
	 METH_VARARGS | METH_KEYWORDS, operate_ordered_doc},

	// LIST OPERATIONS

	{"list_append", (PyCFunction)AerospikeClient_ListAppend,
	 METH_VARARGS | METH_KEYWORDS, list_append_doc},
	{"list_extend", (PyCFunction)AerospikeClient_ListExtend,
	 METH_VARARGS | METH_KEYWORDS, list_extend_doc},
	{"list_insert", (PyCFunction)AerospikeClient_ListInsert,
	 METH_VARARGS | METH_KEYWORDS, list_insert_doc},
	{"list_insert_items", (PyCFunction)AerospikeClient_ListInsertItems,
	 METH_VARARGS | METH_KEYWORDS, list_insert_items_doc},
	{"list_pop", (PyCFunction)AerospikeClient_ListPop,
	 METH_VARARGS | METH_KEYWORDS, list_pop_doc},
	{"list_pop_range", (PyCFunction)AerospikeClient_ListPopRange,
	 METH_VARARGS | METH_KEYWORDS, list_pop_range_doc},
	{"list_remove", (PyCFunction)AerospikeClient_ListRemove,
	 METH_VARARGS | METH_KEYWORDS, list_remove_doc},
	{"list_remove_range", (PyCFunction)AerospikeClient_ListRemoveRange,
	 METH_VARARGS | METH_KEYWORDS, list_remove_range_doc},
	{"list_clear", (PyCFunction)AerospikeClient_ListClear,
	 METH_VARARGS | METH_KEYWORDS, list_clear_doc},
	{"list_set", (PyCFunction)AerospikeClient_ListSet,
	 METH_VARARGS | METH_KEYWORDS, list_set_doc},
	{"list_get", (PyCFunction)AerospikeClient_ListGet,
	 METH_VARARGS | METH_KEYWORDS, list_get_doc},
	{"list_get_range", (PyCFunction)AerospikeClient_ListGetRange,
	 METH_VARARGS | METH_KEYWORDS, list_get_range_doc},
	{"list_trim", (PyCFunction)AerospikeClient_ListTrim,
	 METH_VARARGS | METH_KEYWORDS, list_trim_doc},
	{"list_size", (PyCFunction)AerospikeClient_ListSize,
	 METH_VARARGS | METH_KEYWORDS, list_size_doc},

	// MAP OPERATIONS

	{"map_set_policy", (PyCFunction)AerospikeClient_MapSetPolicy,
	 METH_VARARGS | METH_KEYWORDS, map_set_policy_doc},
	{"map_put", (PyCFunction)AerospikeClient_MapPut,
	 METH_VARARGS | METH_KEYWORDS, map_put_doc},
	{"map_put_items", (PyCFunction)AerospikeClient_MapPutItems,
	 METH_VARARGS | METH_KEYWORDS, map_put_items_doc},
	{"map_increment", (PyCFunction)AerospikeClient_MapIncrement,
	 METH_VARARGS | METH_KEYWORDS, map_increment_doc},
	{"map_decrement", (PyCFunction)AerospikeClient_MapDecrement,
	 METH_VARARGS | METH_KEYWORDS, map_decrement_doc},
	{"map_size", (PyCFunction)AerospikeClient_MapSize,
	 METH_VARARGS | METH_KEYWORDS, map_size_doc},
	{"map_clear", (PyCFunction)AerospikeClient_MapClear,
	 METH_VARARGS | METH_KEYWORDS, map_clear_doc},
	{"map_remove_by_key", (PyCFunction)AerospikeClient_MapRemoveByKey,
	 METH_VARARGS | METH_KEYWORDS, map_remove_by_key_doc},
	{"map_remove_by_key_list", (PyCFunction)AerospikeClient_MapRemoveByKeyList,
	 METH_VARARGS | METH_KEYWORDS, map_remove_by_key_list_doc},
	{"map_remove_by_key_range",
	 (PyCFunction)AerospikeClient_MapRemoveByKeyRange,
	 METH_VARARGS | METH_KEYWORDS, map_remove_by_key_range_doc},
	{"map_remove_by_value", (PyCFunction)AerospikeClient_MapRemoveByValue,
	 METH_VARARGS | METH_KEYWORDS, map_remove_by_value_doc},
	{"map_remove_by_value_list",
	 (PyCFunction)AerospikeClient_MapRemoveByValueList,
	 METH_VARARGS | METH_KEYWORDS, map_remove_by_value_list_doc},
	{"map_remove_by_value_range",
	 (PyCFunction)AerospikeClient_MapRemoveByValueRange,
	 METH_VARARGS | METH_KEYWORDS, map_remove_by_value_range_doc},
	{"map_remove_by_index", (PyCFunction)AerospikeClient_MapRemoveByIndex,
	 METH_VARARGS | METH_KEYWORDS, map_remove_by_index_doc},
	{"map_remove_by_index_range",
	 (PyCFunction)AerospikeClient_MapRemoveByIndexRange,
	 METH_VARARGS | METH_KEYWORDS, map_remove_by_index_range_doc},
	{"map_remove_by_rank", (PyCFunction)AerospikeClient_MapRemoveByRank,
	 METH_VARARGS | METH_KEYWORDS, map_remove_by_rank_doc},
	{"map_remove_by_rank_range",
	 (PyCFunction)AerospikeClient_MapRemoveByRankRange,
	 METH_VARARGS | METH_KEYWORDS, map_remove_by_rank_range_doc},
	{"map_get_by_key", (PyCFunction)AerospikeClient_MapGetByKey,
	 METH_VARARGS | METH_KEYWORDS, map_get_by_key_doc},
	{"map_get_by_key_range", (PyCFunction)AerospikeClient_MapGetByKeyRange,
	 METH_VARARGS | METH_KEYWORDS, map_get_by_key_range_doc},
	{"map_get_by_key_list", (PyCFunction)AerospikeClient_MapGetByKeyList,
	 METH_VARARGS | METH_KEYWORDS, map_get_by_key_list_doc},
	{"map_get_by_value", (PyCFunction)AerospikeClient_MapGetByValue,
	 METH_VARARGS | METH_KEYWORDS, map_get_by_value_doc},
	{"map_get_by_value_range", (PyCFunction)AerospikeClient_MapGetByValueRange,
	 METH_VARARGS | METH_KEYWORDS, map_get_by_value_range_doc},
	{"map_get_by_value_list", (PyCFunction)AerospikeClient_MapGetByValueList,
	 METH_VARARGS | METH_KEYWORDS, map_get_by_value_list_doc},
	{"map_get_by_index", (PyCFunction)AerospikeClient_MapGetByIndex,
	 METH_VARARGS | METH_KEYWORDS, map_get_by_index_doc},
	{"map_get_by_index_range", (PyCFunction)AerospikeClient_MapGetByIndexRange,
	 METH_VARARGS | METH_KEYWORDS, map_get_by_index_range_doc},
	{"map_get_by_rank", (PyCFunction)AerospikeClient_MapGetByRank,
	 METH_VARARGS | METH_KEYWORDS, map_get_by_rank_doc},
	{"map_get_by_rank_range", (PyCFunction)AerospikeClient_MapGetByRankRange,
	 METH_VARARGS | METH_KEYWORDS, map_get_by_rank_range_doc},

	// QUERY OPERATIONS

	{"query", (PyCFunction)AerospikeClient_Query, METH_VARARGS | METH_KEYWORDS,
	 query_doc},
	{"query_apply", (PyCFunction)AerospikeClient_QueryApply,
	 METH_VARARGS | METH_KEYWORDS, query_apply_doc},
	{"job_info", (PyCFunction)AerospikeClient_JobInfo,
	 METH_VARARGS | METH_KEYWORDS, job_info_doc},

	// SCAN OPERATIONS

	{"scan", (PyCFunction)AerospikeClient_Scan, METH_VARARGS | METH_KEYWORDS,
	 scan_doc},
	{"scan_apply", (PyCFunction)AerospikeClient_ScanApply,
	 METH_VARARGS | METH_KEYWORDS, scan_apply_doc},

	{"scan_info", (PyCFunction)AerospikeClient_ScanInfo,
	 METH_VARARGS | METH_KEYWORDS, scan_info_doc},

	// INFO OPERATIONS

	{"info", (PyCFunction)AerospikeClient_Info, METH_VARARGS | METH_KEYWORDS,
	 info_doc},
	{"set_xdr_filter", (PyCFunction)AerospikeClient_SetXDRFilter,
	 METH_VARARGS | METH_KEYWORDS, set_xdr_filter_doc},
	{"info_all", (PyCFunction)AerospikeClient_InfoAll,
	 METH_VARARGS | METH_KEYWORDS, info_all_doc},
	{"info_single_node", (PyCFunction)AerospikeClient_InfoSingleNode,
	 METH_VARARGS | METH_KEYWORDS, info_single_node_doc},
	{"info_random_node", (PyCFunction)AerospikeClient_InfoRandomNode,
	 METH_VARARGS | METH_KEYWORDS, info_random_node_doc},
	{"info_node", // DEPRECATED
	 (PyCFunction)AerospikeClient_InfoNode, METH_VARARGS | METH_KEYWORDS,
	 info_node_doc},
	{"get_nodes", (PyCFunction)AerospikeClient_GetNodes,
	 METH_VARARGS | METH_KEYWORDS, get_nodes_doc},
	{"get_node_names", (PyCFunction)AerospikeClient_GetNodeNames,
	 METH_VARARGS | METH_KEYWORDS, get_node_names_doc},
	// UDF OPERATIONS

	{"udf_put", (PyCFunction)AerospikeClient_UDF_Put,
	 METH_VARARGS | METH_KEYWORDS, udf_put_doc},
	{"udf_remove", (PyCFunction)AerospikeClient_UDF_Remove,
	 METH_VARARGS | METH_KEYWORDS, udf_remove_doc},
	{"udf_list", (PyCFunction)AerospikeClient_UDF_List,
	 METH_VARARGS | METH_KEYWORDS, udf_list_doc},
	{"udf_get", (PyCFunction)AerospikeClient_UDF_Get_UDF,
	 METH_VARARGS | METH_KEYWORDS, udf_get_doc},

	// SECONDARY INDEX OPERATONS

	{"index_integer_create", (PyCFunction)AerospikeClient_Index_Integer_Create,
	 METH_VARARGS | METH_KEYWORDS, index_integer_create_doc},
	{"index_string_create", (PyCFunction)AerospikeClient_Index_String_Create,
	 METH_VARARGS | METH_KEYWORDS, index_string_create_doc},
	{"index_remove", (PyCFunction)AerospikeClient_Index_Remove,
	 METH_VARARGS | METH_KEYWORDS, index_remove_doc},
	{"index_list_create", (PyCFunction)AerospikeClient_Index_List_Create,
	 METH_VARARGS | METH_KEYWORDS, index_list_create_doc},
	{"index_map_keys_create",
	 (PyCFunction)AerospikeClient_Index_Map_Keys_Create,
	 METH_VARARGS | METH_KEYWORDS, index_map_keys_create_doc},
	{"index_map_values_create",
	 (PyCFunction)AerospikeClient_Index_Map_Values_Create,
	 METH_VARARGS | METH_KEYWORDS, index_map_values_create_doc},
	{"index_geo2dsphere_create",
	 (PyCFunction)AerospikeClient_Index_2dsphere_Create,
	 METH_VARARGS | METH_KEYWORDS, index_geo2dsphere_create_doc},

	// BATCH OPERATIONS

	{"get_many", (PyCFunction)AerospikeClient_Get_Many,
	 METH_VARARGS | METH_KEYWORDS, get_many_doc},
	{"batch_get_ops", (PyCFunction)AerospikeClient_Batch_GetOps,
	 METH_VARARGS | METH_KEYWORDS, batch_get_ops_doc},
	{"select_many", (PyCFunction)AerospikeClient_Select_Many,
	 METH_VARARGS | METH_KEYWORDS, select_many_doc},
	{"exists_many", (PyCFunction)AerospikeClient_Exists_Many,
	 METH_VARARGS | METH_KEYWORDS, exists_many_doc},
	{"get_key_digest", (PyCFunction)AerospikeClient_Get_Key_Digest,
	 METH_VARARGS | METH_KEYWORDS, get_key_digest_doc},

	// TRUNCATE OPERATIONS
	{"truncate", (PyCFunction)AerospikeClient_Truncate,
	 METH_VARARGS | METH_KEYWORDS, truncate_doc},

	{NULL}};

/*******************************************************************************
 * PYTHON TYPE HOOKS
 ******************************************************************************/

static PyObject *AerospikeClient_Type_New(PyTypeObject *type, PyObject *args,
										  PyObject *kwds)
{
	AerospikeClient *self = NULL;

	self = (AerospikeClient *)type->tp_alloc(type, 0);

	return (PyObject *)self;
}

static int AerospikeClient_Type_Init(AerospikeClient *self, PyObject *args,
									 PyObject *kwds)
{
	PyObject *py_config = NULL;
	int error_code = 0;
	as_error constructor_err;
	as_error_init(&constructor_err);
	static char *kwlist[] = {"config", NULL};

	self->has_connected = false;
	self->use_shared_connection = false;
	self->as = NULL;
	self->send_bool_as = SEND_BOOL_AS_PY_BYTES;

	if (PyArg_ParseTupleAndKeywords(args, kwds, "O:client", kwlist,
									&py_config) == false) {
		error_code = INIT_NO_CONFIG_ERR;
		goto CONSTRUCTOR_ERROR;
	}

	if (!PyDict_Check(py_config)) {
		error_code = INIT_CONFIG_TYPE_ERR;
		goto CONSTRUCTOR_ERROR;
	}

	as_config config;
	as_config_init(&config);

	bool lua_user_path = false;

	PyObject *py_lua = PyDict_GetItemString(py_config, "lua");
	if (py_lua && PyDict_Check(py_lua)) {

		PyObject *py_lua_user_path = PyDict_GetItemString(py_lua, "user_path");
		if (py_lua_user_path && PyString_Check(py_lua_user_path)) {
			lua_user_path = true;
			if (strnlen(PyString_AsString(py_lua_user_path),
						AS_CONFIG_PATH_MAX_SIZE) > AS_CONFIG_PATH_MAX_LEN) {
				error_code = INIT_LUA_USER_ERR;
				goto CONSTRUCTOR_ERROR;
			}
			strcpy(config.lua.user_path, PyString_AsString(py_lua_user_path));
		}
	}

	if (!lua_user_path) {
		strcpy(config.lua.user_path, ".");
	}
	else {
		struct stat info;
		if (stat(config.lua.user_path, &info) != 0 ||
			!(info.st_mode & S_IFDIR)) {
			strcpy(config.lua.user_path, ".");
		}
	}

	PyObject *py_tls = PyDict_GetItemString(py_config, "tls");
	if (py_tls && PyDict_Check(py_tls)) {
		setup_tls_config(&config, py_tls);
	}

	PyObject *py_hosts = PyDict_GetItemString(py_config, "hosts");
	if (py_hosts && PyList_Check(py_hosts)) {
		int size = (int)PyList_Size(py_hosts);
		if (!size) {
			error_code = INIT_EMPTY_HOSTS_ERR;
			goto CONSTRUCTOR_ERROR;
		}
		for (int i = 0; i < size; i++) {
			char *addr = NULL;
			char *tls_name = NULL;
			uint16_t port = 3000;
			PyObject *py_host = PyList_GetItem(py_hosts, i);
			PyObject *py_addr, *py_port, *py_tls_name;

			if (PyTuple_Check(py_host) && PyTuple_Size(py_host) >= 2 &&
				PyTuple_Size(py_host) <= 3) {

				py_addr = PyTuple_GetItem(py_host, 0);
				if (PyString_Check(py_addr)) {
					addr = strdup(PyString_AsString(py_addr));
				}
				else if (PyUnicode_Check(py_addr)) {
					PyObject *py_ustr = PyUnicode_AsUTF8String(py_addr);
					addr = strdup(PyBytes_AsString(py_ustr));
					Py_DECREF(py_ustr);
				}
				py_port = PyTuple_GetItem(py_host, 1);
				if (PyInt_Check(py_port) || PyLong_Check(py_port)) {
					port = (uint16_t)PyLong_AsLong(py_port);
				}
				else {
					port = 0;
				}
				// Set TLS Name if provided
				if (PyTuple_Size(py_host) == 3) {
					py_tls_name = PyTuple_GetItem(py_host, 2);
					if (PyString_Check(py_tls_name)) {
						tls_name = strdup(PyString_AsString(py_tls_name));
					}
					else if (PyUnicode_Check(py_tls_name)) {
						PyObject *py_ustr = PyUnicode_AsUTF8String(py_tls_name);
						tls_name = strdup(PyBytes_AsString(py_ustr));
						Py_DECREF(py_ustr);
					}
				}
			}
			else if (PyString_Check(py_host)) {
				addr = strdup(strtok(PyString_AsString(py_host), ":"));
				addr = strtok(addr, ":");
				char *temp = strtok(NULL, ":");
				if (NULL != temp) {
					port = (uint16_t)atoi(temp);
				}
			}
			if (addr) {
				if (tls_name) {
					as_config_tls_add_host(&config, addr, tls_name, port);
					free(tls_name);
				}
				else {
					as_config_add_host(&config, addr, port);
				}
				free(addr);
			}
			else {
				error_code = INIT_INVALID_ADRR_ERR;
				goto CONSTRUCTOR_ERROR;
			}
		}
	}
	else {
		error_code = INIT_HOST_TYPE_ERR;
		goto CONSTRUCTOR_ERROR;
	}

	PyObject *py_shm = PyDict_GetItemString(py_config, "shm");
	if (py_shm && PyDict_Check(py_shm)) {

		config.use_shm = true;

		// This does not match documentation (wrong name and location in dict),
		//  but leave it for now for customers who may be using it
		PyObject *py_shm_max_nodes =
			PyDict_GetItemString(py_shm, "shm_max_nodes");
		if (py_shm_max_nodes && PyInt_Check(py_shm_max_nodes)) {
			config.shm_max_nodes = PyInt_AsLong(py_shm_max_nodes);
		}
		py_shm_max_nodes = PyDict_GetItemString(py_shm, "max_nodes");
		if (py_shm_max_nodes && PyInt_Check(py_shm_max_nodes)) {
			config.shm_max_nodes = PyInt_AsLong(py_shm_max_nodes);
		}

		// This does not match documentation (wrong name and location in dict),
		//  but leave it for now for customers who may be using it
		PyObject *py_shm_max_namespaces =
			PyDict_GetItemString(py_shm, "shm_max_namespaces");
		if (py_shm_max_namespaces && PyInt_Check(py_shm_max_namespaces)) {
			config.shm_max_namespaces = PyInt_AsLong(py_shm_max_namespaces);
		}
		py_shm_max_namespaces = PyDict_GetItemString(py_shm, "max_namespaces");
		if (py_shm_max_namespaces && PyInt_Check(py_shm_max_namespaces)) {
			config.shm_max_namespaces = PyInt_AsLong(py_shm_max_namespaces);
		}

		// This does not match documentation (wrong name and location in dict),
		//  but leave it for now for customers who may be using it
		PyObject *py_shm_takeover_threshold_sec =
			PyDict_GetItemString(py_shm, "shm_takeover_threshold_sec");
		if (py_shm_takeover_threshold_sec &&
			PyInt_Check(py_shm_takeover_threshold_sec)) {
			config.shm_takeover_threshold_sec =
				PyInt_AsLong(py_shm_takeover_threshold_sec);
		}
		py_shm_takeover_threshold_sec =
			PyDict_GetItemString(py_shm, "takeover_threshold_sec");
		if (py_shm_takeover_threshold_sec &&
			PyInt_Check(py_shm_takeover_threshold_sec)) {
			config.shm_takeover_threshold_sec =
				PyInt_AsLong(py_shm_takeover_threshold_sec);
		}

		PyObject *py_shm_cluster_key = PyDict_GetItemString(py_shm, "shm_key");
		if (py_shm_cluster_key && PyInt_Check(py_shm_cluster_key)) {
			user_shm_key = true;
			config.shm_key = PyInt_AsLong(py_shm_cluster_key);
		}
	}

	self->is_client_put_serializer = false;
	self->user_serializer_call_info.callback = NULL;
	self->user_deserializer_call_info.callback = NULL;
	PyObject *py_serializer_option =
		PyDict_GetItemString(py_config, "serialization");
	if (py_serializer_option && PyTuple_Check(py_serializer_option)) {
		PyObject *py_serializer = PyTuple_GetItem(py_serializer_option, 0);
		if (py_serializer && py_serializer != Py_None) {
			if (!PyCallable_Check(py_serializer)) {
				error_code = INIT_SERIALIZE_ERR;
				goto CONSTRUCTOR_ERROR;
			}
			memset(&self->user_serializer_call_info, 0,
				   sizeof(self->user_serializer_call_info));
			self->user_serializer_call_info.callback = py_serializer;
		}
		PyObject *py_deserializer = PyTuple_GetItem(py_serializer_option, 1);
		if (py_deserializer && py_deserializer != Py_None) {
			if (!PyCallable_Check(py_deserializer)) {
				error_code = INIT_DESERIALIZE_ERR;
				goto CONSTRUCTOR_ERROR;
			}
			memset(&self->user_deserializer_call_info, 0,
				   sizeof(self->user_deserializer_call_info));
			self->user_deserializer_call_info.callback = py_deserializer;
		}
	}

	as_policies_init(&config.policies);
	//Set default value of use_batch_direct

	PyObject *py_policies = PyDict_GetItemString(py_config, "policies");
	if (py_policies && PyDict_Check(py_policies)) {
		//global defaults setting
		PyObject *py_key_policy = PyDict_GetItemString(py_policies, "key");
		if (py_key_policy && PyInt_Check(py_key_policy)) {
			long long_key_policy = PyInt_AsLong(py_key_policy);
			config.policies.read.key = long_key_policy;
			config.policies.write.key = long_key_policy;
			config.policies.apply.key = long_key_policy;
			config.policies.operate.key = long_key_policy;
			config.policies.remove.key = long_key_policy;
		}

		/* This was the name of the policy pre 3.0.0, keep for legacy reasons
		 * It is the same as total_timeout
		 */
		PyObject *py_timeout = PyDict_GetItemString(py_policies, "timeout");
		if (py_timeout && PyInt_Check(py_timeout)) {
			long long_timeout = PyInt_AsLong(py_timeout);

			config.policies.write.base.total_timeout = long_timeout;
			config.policies.read.base.total_timeout = long_timeout;
			config.policies.apply.base.total_timeout = long_timeout;
			config.policies.operate.base.total_timeout = long_timeout;
			config.policies.query.base.total_timeout = long_timeout;
			config.policies.scan.base.total_timeout = long_timeout;
			config.policies.remove.base.total_timeout = long_timeout;
			config.policies.batch.base.total_timeout = long_timeout;
		}

		PyObject *py_sock_timeout =
			PyDict_GetItemString(py_policies, "socket_timeout");
		if (py_sock_timeout && PyInt_Check(py_sock_timeout)) {
			long long_timeout = PyInt_AsLong(py_sock_timeout);

			config.policies.write.base.socket_timeout = long_timeout;
			config.policies.read.base.socket_timeout = long_timeout;
			config.policies.apply.base.socket_timeout = long_timeout;
			config.policies.operate.base.socket_timeout = long_timeout;
			config.policies.query.base.socket_timeout = long_timeout;
			config.policies.scan.base.socket_timeout = long_timeout;
			config.policies.remove.base.socket_timeout = long_timeout;
			config.policies.batch.base.socket_timeout = long_timeout;
		}

		PyObject *py_total_timeout =
			PyDict_GetItemString(py_policies, "total_timeout");
		if (py_total_timeout && PyInt_Check(py_total_timeout)) {
			long long_total_timeout = PyInt_AsLong(py_total_timeout);

			config.policies.write.base.total_timeout = long_total_timeout;
			config.policies.read.base.total_timeout = long_total_timeout;
			config.policies.apply.base.total_timeout = long_total_timeout;
			config.policies.operate.base.total_timeout = long_total_timeout;
			config.policies.query.base.total_timeout = long_total_timeout;
			config.policies.scan.base.total_timeout = long_total_timeout;
			config.policies.remove.base.total_timeout = long_total_timeout;
			config.policies.batch.base.total_timeout = long_total_timeout;
		}

		PyObject *py_max_retry =
			PyDict_GetItemString(py_policies, "max_retries");
		if (py_max_retry && PyInt_Check(py_max_retry)) {
			long long_max_retries = PyInt_AsLong(py_max_retry);
			config.policies.write.base.max_retries = long_max_retries;
			config.policies.read.base.max_retries = long_max_retries;
			config.policies.apply.base.max_retries = long_max_retries;
			config.policies.operate.base.max_retries = long_max_retries;
			config.policies.query.base.max_retries = long_max_retries;
			config.policies.scan.base.max_retries = long_max_retries;
			config.policies.remove.base.max_retries = long_max_retries;
			config.policies.batch.base.max_retries = long_max_retries;
		}

		PyObject *py_exists = PyDict_GetItemString(py_policies, "exists");
		if (py_exists && PyInt_Check(py_exists)) {
			long long_exists = PyInt_AsLong(py_exists);
			config.policies.write.exists = long_exists;
		}

		PyObject *py_replica = PyDict_GetItemString(py_policies, "replica");
		if (py_replica && PyInt_Check(py_replica)) {
			long long_replica = PyInt_AsLong(py_replica);
			config.policies.read.replica = long_replica;
			config.policies.write.replica = long_replica;
			config.policies.apply.replica = long_replica;
			config.policies.operate.replica = long_replica;
			config.policies.remove.replica = long_replica;
		}

		PyObject *py_ap_read_mode =
			PyDict_GetItemString(py_policies, "read_mode_ap");
		if (py_ap_read_mode && PyInt_Check(py_ap_read_mode)) {
			as_policy_read_mode_ap ap_read_mode =
				(as_policy_read_mode_ap)PyInt_AsLong(py_ap_read_mode);
			config.policies.read.read_mode_ap = ap_read_mode;
			config.policies.operate.read_mode_ap = ap_read_mode;
			config.policies.batch.read_mode_ap = ap_read_mode;
		}

		PyObject *py_sc_read_mode =
			PyDict_GetItemString(py_policies, "read_mode_sc");
		if (py_sc_read_mode && PyInt_Check(py_sc_read_mode)) {
			as_policy_read_mode_sc sc_read_mode =
				(as_policy_read_mode_sc)PyInt_AsLong(py_sc_read_mode);
			config.policies.read.read_mode_sc = sc_read_mode;
			config.policies.operate.read_mode_sc = sc_read_mode;
			config.policies.batch.read_mode_sc = sc_read_mode;
		}

		PyObject *py_commit_level =
			PyDict_GetItemString(py_policies, "commit_level");
		if (py_commit_level && PyInt_Check(py_commit_level)) {
			long long_commit_level = PyInt_AsLong(py_commit_level);
			config.policies.write.commit_level = long_commit_level;
			config.policies.apply.commit_level = long_commit_level;
			config.policies.operate.commit_level = long_commit_level;
			config.policies.remove.commit_level = long_commit_level;
		}

		// This does not match documentation (should not be in policies),
		//  but leave it for now for customers who may be using it
		PyObject *py_max_threads =
			PyDict_GetItemString(py_policies, "max_threads");
		if (py_max_threads &&
			(PyInt_Check(py_max_threads) || PyLong_Check(py_max_threads))) {
			config.max_conns_per_node = PyInt_AsLong(py_max_threads);
		}

		// This does not match documentation (should not be in policies),
		//  but leave it for now for customers who may be using it
		PyObject *py_thread_pool_size =
			PyDict_GetItemString(py_policies, "thread_pool_size");
		if (py_thread_pool_size && (PyInt_Check(py_thread_pool_size) ||
									PyLong_Check(py_thread_pool_size))) {
			config.thread_pool_size = PyInt_AsLong(py_thread_pool_size);
		}

		/*
		 * Generation policy is removed from constructor.
		 */

		/*
		 * Set individual policy groups, and base policies for each
		 * Set the individual policy groups new in 3.0
		 * */

		if (set_subpolicies(&config, py_policies) != AEROSPIKE_OK) {
			error_code = INIT_POLICY_PARAM_ERR;
			goto CONSTRUCTOR_ERROR;
		}

		PyObject *py_login_timeout =
			PyDict_GetItemString(py_policies, "login_timeout_ms");
		if (py_login_timeout && PyInt_Check(py_login_timeout)) {
			config.login_timeout_ms = PyInt_AsLong(py_login_timeout);
		}

		PyObject *py_auth_mode = PyDict_GetItemString(py_policies, "auth_mode");
		if (py_auth_mode) {
			if (PyInt_Check(py_auth_mode)) {
				long auth_mode = PyInt_AsLong(py_auth_mode);
				if ((long)AS_AUTH_INTERNAL == auth_mode ||
					(long)AS_AUTH_EXTERNAL == auth_mode ||
					(long)AS_AUTH_EXTERNAL_INSECURE == auth_mode ||
					(long)AS_AUTH_PKI == auth_mode ) {
					config.auth_mode = auth_mode;
				} else {
					error_code = INIT_INVALID_AUTHMODE_ERR;
					goto CONSTRUCTOR_ERROR;
				}
			} else {
				//it may come like auth_mode = None, for those non-integer cases, treat them as non-set
				//error_code = INIT_INVALID_AUTHMODE_ERR;
				//goto CONSTRUCTOR_ERROR;
			}
		}
	}

	// thread_pool_size
	PyObject *py_thread_pool_size =
		PyDict_GetItemString(py_config, "thread_pool_size");
	if (py_thread_pool_size && PyInt_Check(py_thread_pool_size)) {
		config.thread_pool_size = PyInt_AsLong(py_thread_pool_size);
	}

	// max_threads (backward compatibility)
	PyObject *py_max_threads = PyDict_GetItemString(py_config, "max_threads");
	if (py_max_threads &&
		(PyInt_Check(py_max_threads) || PyLong_Check(py_max_threads))) {
		config.max_conns_per_node = PyInt_AsLong(py_max_threads);
	}

	// max_conns_per_node
	PyObject *py_max_conns =
		PyDict_GetItemString(py_config, "max_conns_per_node");
	if (py_max_conns &&
		(PyInt_Check(py_max_conns) || PyLong_Check(py_max_conns))) {
		config.max_conns_per_node = PyInt_AsLong(py_max_conns);
	}

	//conn_timeout_ms
	PyObject *py_connect_timeout =
		PyDict_GetItemString(py_config, "connect_timeout");
	if (py_connect_timeout && PyInt_Check(py_connect_timeout)) {
		config.conn_timeout_ms = PyInt_AsLong(py_connect_timeout);
	}

	//Whether to utilize shared connection
	PyObject *py_share_connect =
		PyDict_GetItemString(py_config, "use_shared_connection");
	if (py_share_connect) {
		self->use_shared_connection = PyObject_IsTrue(py_share_connect);
	}

	PyObject *py_send_bool_as = PyDict_GetItemString(py_config, "send_bool_as");
	if (py_send_bool_as != NULL && PyLong_Check(py_send_bool_as)) {
		int send_bool_as_temp = PyLong_AsLong(py_send_bool_as);
		if (send_bool_as_temp >= SEND_BOOL_AS_PY_BYTES &&
			send_bool_as_temp <= SEND_BOOL_AS_AS_BOOL) {
			self->send_bool_as = send_bool_as_temp;
		}
	}

	//compression_threshold
	PyObject *py_compression_threshold =
		PyDict_GetItemString(py_config, "compression_threshold");
	if (py_compression_threshold && PyInt_Check(py_compression_threshold)) {
		int compression_value = PyInt_AsLong(py_compression_threshold);
		if (compression_value >= 0) {
			config.policies.write.compression_threshold = compression_value;
		}
		else {
			error_code = INIT_COMPRESSION_ERR;
			goto CONSTRUCTOR_ERROR;
		}
	}

	PyObject *py_tend_interval =
		PyDict_GetItemString(py_config, "tend_interval");
	if (py_tend_interval && PyInt_Check(py_tend_interval)) {
		config.tender_interval = PyInt_AsLong(py_tend_interval);
	}

	PyObject *py_cluster_name = PyDict_GetItemString(py_config, "cluster_name");
	if (py_cluster_name && PyString_Check(py_cluster_name)) {
		as_config_set_cluster_name(&config,
								   strdup(PyString_AsString(py_cluster_name)));
	}

	//strict_types check
	self->strict_types = true;
	PyObject *py_strict_types = PyDict_GetItemString(py_config, "strict_types");
	if (py_strict_types && PyBool_Check(py_strict_types)) {
		if (Py_False == py_strict_types) {
			self->strict_types = false;
		}
	}

	if (set_rack_aware_config(&config, py_config) != INIT_SUCCESS) {
		error_code = INIT_POLICY_PARAM_ERR;
		goto CONSTRUCTOR_ERROR;
	}
	if (set_use_services_alternate(&config, py_config) != INIT_SUCCESS) {
		error_code = INIT_POLICY_PARAM_ERR;
		goto CONSTRUCTOR_ERROR;
	}

	PyObject *py_max_socket_idle = NULL;
	py_max_socket_idle = PyDict_GetItemString(py_config, "max_socket_idle");
	if (py_max_socket_idle && PyInt_Check(py_max_socket_idle)) {
		long max_socket_idle = PyInt_AsLong(py_max_socket_idle);
		if (max_socket_idle >= 0) {
			config.max_socket_idle = (uint32_t)max_socket_idle;
		}
	}
	self->as = aerospike_new(&config);

	return 0;

CONSTRUCTOR_ERROR:

	switch (error_code) {
	// 0 Is success
	case 0: {
		// Initialize connection flag
		return 0;
	}
	case INIT_NO_CONFIG_ERR: {
		as_error_update(&constructor_err, AEROSPIKE_ERR_PARAM,
						"No config argument");
		break;
	}
	case INIT_CONFIG_TYPE_ERR: {
		as_error_update(&constructor_err, AEROSPIKE_ERR_PARAM,
						"Config must be a dict");
		break;
	}
	case INIT_LUA_USER_ERR: {
		as_error_update(&constructor_err, AEROSPIKE_ERR_PARAM,
						"Lua user path too long");
		break;
	}
	case INIT_LUA_SYS_ERR: {
		as_error_update(&constructor_err, AEROSPIKE_ERR_PARAM,
						"Lua system path too long");
		break;
	}
	case INIT_HOST_TYPE_ERR: {
		as_error_update(&constructor_err, AEROSPIKE_ERR_PARAM,
						"Hosts must be a list");
		break;
	}
	case INIT_EMPTY_HOSTS_ERR: {
		as_error_update(&constructor_err, AEROSPIKE_ERR_PARAM,
						"Hosts must not be empty");
		break;
	}
	case INIT_INVALID_ADRR_ERR: {
		as_error_update(&constructor_err, AEROSPIKE_ERR_PARAM, "Invalid host");
		break;
	}
	case INIT_SERIALIZE_ERR: {
		as_error_update(&constructor_err, AEROSPIKE_ERR_PARAM,
						"Serializer must be callable");
		break;
	}
	case INIT_DESERIALIZE_ERR: {
		as_error_update(&constructor_err, AEROSPIKE_ERR_PARAM,
						"Deserializer must be callable");
		break;
	}
	case INIT_COMPRESSION_ERR: {
		as_error_update(&constructor_err, AEROSPIKE_ERR_PARAM,
						"Compression value must not be negative");
		break;
	}
	case INIT_POLICY_PARAM_ERR: {
		as_error_update(&constructor_err, AEROSPIKE_ERR_PARAM,
						"Invalid Policy setting value");
		break;
	}
	case INIT_INVALID_AUTHMODE_ERR: {
		as_error_update(&constructor_err, AEROSPIKE_ERR_PARAM,
						"Specify valid auth_mode");
		break;
	}
	default:
		// If a generic error was caught during init, use this message
		as_error_update(&constructor_err, AEROSPIKE_ERR_PARAM,
						"Invalid Parameters");
		break;
	}

	PyObject *py_err = NULL;
	error_to_pyobject(&constructor_err, &py_err);
	PyObject *exception_type = raise_exception(&constructor_err);
	PyErr_SetObject(exception_type, py_err);
	Py_DECREF(py_err);
	return -1;
}

static int set_rack_aware_config(as_config *conf, PyObject *config_dict)
{
	PyObject *py_config_value;
	long rack_id;
	py_config_value = PyDict_GetItemString(config_dict, "rack_aware");
	if (py_config_value) {
		if (PyBool_Check(py_config_value)) {
			conf->rack_aware = PyObject_IsTrue(py_config_value);
		}
		else {
			return INIT_POLICY_PARAM_ERR; // A non boolean was passed in as the value of rack_aware
		}
	}

	py_config_value = PyDict_GetItemString(config_dict, "rack_id");
	if (py_config_value) {
		if (PyLong_Check(py_config_value)) {
			rack_id = PyLong_AsLong(py_config_value);
		}
		else if (PyInt_Check(py_config_value)) {
			rack_id = PyInt_AsLong(py_config_value);
		}
		else {
			return INIT_POLICY_PARAM_ERR; // A non integer passed in.
		}
		if (rack_id == -1 && PyErr_Occurred()) {
			return INIT_POLICY_PARAM_ERR; // We had overflow.
		}

		if (rack_id > INT_MAX || rack_id < INT_MIN) {
			return INIT_POLICY_PARAM_ERR; // Magnitude too great for an integer in C.
		}
		conf->rack_id = (int)rack_id;
	}
	return INIT_SUCCESS;
}

static int set_use_services_alternate(as_config *conf, PyObject *config_dict)
{
	PyObject *py_config_value;
	py_config_value =
		PyDict_GetItemString(config_dict, "use_services_alternate");
	if (py_config_value) {
		if (PyBool_Check(py_config_value)) {
			conf->use_services_alternate = PyObject_IsTrue(py_config_value);
		}
		else {
			return INIT_POLICY_PARAM_ERR; // A non boolean was passed in as the value of use_services_alternate
		}
	}
	return INIT_SUCCESS;
}

static void AerospikeClient_Type_Dealloc(PyObject *self)
{

	as_error err;
	char *alias_to_search = NULL;
	PyObject *py_persistent_item = NULL;
	AerospikeGlobalHosts *global_host = NULL;
	AerospikeClient *client = (AerospikeClient *)self;

	// If the client has never connected
	// It is safe to destroy the aerospike structure
	if (client->as) {
		if (!client->has_connected) {
			aerospike_destroy(client->as);
		}
		else {

			// If the connection is possibly shared, use reference counted deletes
			if (client->use_shared_connection) {
				// If this client was still connected, deal with the global host object
				if (client->is_conn_16) {
					alias_to_search = return_search_string(client->as);
					py_persistent_item =
						PyDict_GetItemString(py_global_hosts, alias_to_search);
					if (py_persistent_item) {
						global_host =
							(AerospikeGlobalHosts *)py_persistent_item;
						// Only modify the global as object if the client points to it
						if (client->as == global_host->as) {
							close_aerospike_object(client->as, &err,
												   alias_to_search,
												   py_persistent_item, false);
						}
					}
				}
				// Connection is not shared, so it is safe to destroy the as object
			}
			else {
				if (client->is_conn_16) {
					aerospike_close(client->as, &err);
				}
				aerospike_destroy(client->as);
			}
		}
	}
	self->ob_type->tp_free((PyObject *)self);
}

/*******************************************************************************
 * PYTHON TYPE DESCRIPTOR
 ******************************************************************************/

static PyTypeObject AerospikeClient_Type = {
	PyVarObject_HEAD_INIT(NULL, 0) "aerospike.Client", // tp_name
	sizeof(AerospikeClient),						   // tp_basicsize
	0,												   // tp_itemsize
	(destructor)AerospikeClient_Type_Dealloc,
	// tp_dealloc
	0, // tp_print
	0, // tp_getattr
	0, // tp_setattr
	0, // tp_compare
	0, // tp_repr
	0, // tp_as_number
	0, // tp_as_sequence
	0, // tp_as_mapping
	0, // tp_hash
	0, // tp_call
	0, // tp_str
	0, // tp_getattro
	0, // tp_setattro
	0, // tp_as_buffer
	Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
	// tp_flags
	"The Client class manages the connections and trasactions against\n"
	"an Aerospike cluster.\n",
	// tp_doc
	0,							  // tp_traverse
	0,							  // tp_clear
	0,							  // tp_richcompare
	0,							  // tp_weaklistoffset
	0,							  // tp_iter
	0,							  // tp_iternext
	AerospikeClient_Type_Methods, // tp_methods
	0,							  // tp_members
	0,							  // tp_getset
	0,							  // tp_base
	0,							  // tp_dict
	0,							  // tp_descr_get
	0,							  // tp_descr_set
	0,							  // tp_dictoffset
	(initproc)AerospikeClient_Type_Init,
	// tp_init
	0,						  // tp_alloc
	AerospikeClient_Type_New, // tp_new
	0,						  // tp_free
	0,						  // tp_is_gc
	0						  // tp_bases
};

/*******************************************************************************
 * PUBLIC FUNCTIONS
 ******************************************************************************/

PyTypeObject *AerospikeClient_Ready()
{
	return PyType_Ready(&AerospikeClient_Type) == 0 ? &AerospikeClient_Type
													: NULL;
}

AerospikeClient *AerospikeClient_New(PyObject *parent, PyObject *args,
									 PyObject *kwds)
{
	AerospikeClient *self = (AerospikeClient *)AerospikeClient_Type.tp_new(
		&AerospikeClient_Type, args, kwds);
	as_error err;
	as_error_init(&err);
	int return_code = 0;
	return_code = AerospikeClient_Type.tp_init((PyObject *)self, args, kwds);

	switch (return_code) {
	// 0 Is success
	case 0: {
		// Initialize connection flag
		self->is_conn_16 = false;
		return self;
	}
	case -1: {
		if (PyErr_Occurred()) {
			return NULL;
		}
		break;
	}
	default: {
		if (PyErr_Occurred()) {
			return NULL;
		}
		break;
	}
	}

	PyObject *py_err = NULL;
	as_error_update(&err, AEROSPIKE_ERR_PARAM, "Failed to construct object");
	error_to_pyobject(&err, &py_err);
	PyObject *exception_type = raise_exception(&err);
	PyErr_SetObject(exception_type, py_err);
	Py_DECREF(py_err);
	return NULL;
}
