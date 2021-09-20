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
 * Macros for UDF operations.
 ******************************************************************************/

#define LUA_FILE_BUFFER_FRAME 512

/*******************************************************************************
 * CLIENT TYPE
 ******************************************************************************/

PyTypeObject *AerospikeClient_Ready(void);

/**
 * Create a new Aerospike client object and connect to the database.
 */
AerospikeClient *AerospikeClient_New(PyObject *self, PyObject *args,
									 PyObject *kwds);

/*******************************************************************************
 * CONNECTION OPERATIONS
 ******************************************************************************/

/**
 * Connect to the database.
 */
PyObject *AerospikeClient_Connect(AerospikeClient *self, PyObject *args,
								  PyObject *kwds);

/**
 * Close the connections to the database.
 */
PyObject *AerospikeClient_Close(AerospikeClient *self, PyObject *args,
								PyObject *kwds);

/**
 * Checks the connection to the database.
 */
PyObject *AerospikeClient_is_connected(AerospikeClient *self, PyObject *args,
									   PyObject *kwds);

/**
 * Get the shm_key to the cluster.
 */
PyObject *AerospikeClient_shm_key(AerospikeClient *self, PyObject *args,
								  PyObject *kwds);

/*******************************************************************************
 * KVS OPERATIONS
 ******************************************************************************/

/**
 * Apply a UDF on a record in the database.
 *
 *		client.apply((x,y,z), module, function, args...)
 *
 */
PyObject *AerospikeClient_Apply(AerospikeClient *self, PyObject *args,
								PyObject *kwds);

PyObject *AerospikeClient_Apply_Invoke(AerospikeClient *self, PyObject *py_key,
									   PyObject *py_module,
									   PyObject *py_function,
									   PyObject *py_arglist,
									   PyObject *py_policy);
/**
 * Check existence of a record in the database.
 *
 *		client.exists((x,y,z))
 *
 */
PyObject *AerospikeClient_Exists(AerospikeClient *self, PyObject *args,
								 PyObject *kwds);

PyObject *AerospikeClient_Exists_Invoke(AerospikeClient *self, PyObject *py_key,
										PyObject *py_policy);

/**
 * Read a record from the database.
 *
 *		client.get((x,y,z))
 *
 */
PyObject *AerospikeClient_Get(AerospikeClient *self, PyObject *args,
							  PyObject *kwds);

PyObject *AerospikeClient_Get_Invoke(AerospikeClient *self, PyObject *py_key,
									 PyObject *py_policy);

/**
 * Async Read a record from the database.
 *
 *		client.get((x,y,z))
 *
 */
PyObject *AerospikeClient_Get_Async(AerospikeClient *self, PyObject *args,
									PyObject *kwds);

/**
 * Project specific bins of a record from the database.
 *
 *		client.select((x,y,z), (bin1, bin2, bin3))
 *
 */
PyObject *AerospikeClient_Select(AerospikeClient *self, PyObject *args,
								 PyObject *kwds);

/**
 * Write a record in the database.
 *
 *		client.put((x,y,z), ...)
 *
 */
PyObject *AerospikeClient_Put(AerospikeClient *self, PyObject *args,
							  PyObject *kwds);

PyObject *AerospikeClient_Put_Invoke(AerospikeClient *self, PyObject *py_key,
									 PyObject *py_bins, PyObject *py_meta,
									 PyObject *py_policy,
									 long serializer_option);

/**
 * Write a record async in the database.
 *
 *		client.put((x,y,z), ...)
 *
 */
PyObject *AerospikeClient_Put_Async(AerospikeClient *self, PyObject *args,
									PyObject *kwds);

/**
 * Remove a record from the database.
 *
 *		client.remove((x,y,z))
 *
 */
PyObject *AerospikeClient_Remove(AerospikeClient *self, PyObject *args,
								 PyObject *kwds);

PyObject *AerospikeClient_Remove_Invoke(AerospikeClient *self, PyObject *py_key,
										PyObject *py_meta, PyObject *py_policy);

/**
 * Remove bin from the database.
 *
 *		client.removebin((x,y,z))
 *
 */
extern PyObject *AerospikeClient_RemoveBin(AerospikeClient *self,
										   PyObject *args, PyObject *kwds);
/**
 * Append a string to a string bin value of a record in the database.
 *
 *		client.append((x,y,z))
 *
 */
PyObject *AerospikeClient_Append(AerospikeClient *self, PyObject *args,
								 PyObject *kwds);
/**
 * Prepend a string to a string bin value of a record in the database.
 *
 *		client.prepend((x,y,z))
 *
 */
PyObject *AerospikeClient_Prepend(AerospikeClient *self, PyObject *args,
								  PyObject *kwds);
/**
 * Increment bin value of a record to the database.
 *
 *		client.increment((x,y,z))
 *
 */
PyObject *AerospikeClient_Increment(AerospikeClient *self, PyObject *args,
									PyObject *kwds);
/**
 * Touch a record in the database.
 *
 *		client.touch((x,y,z))
 *
 */
PyObject *AerospikeClient_Touch(AerospikeClient *self, PyObject *args,
								PyObject *kwds);
/**
 * Performs operate operations
 *
 *		client.operate((x,y,z))
 *
 */
PyObject *AerospikeClient_Operate(AerospikeClient *self, PyObject *args,
								  PyObject *kwds);
/**
 * Performs operate ordered operations
 *
 *		client.operate_ordered((x,y,z))
 *
 */
PyObject *AerospikeClient_OperateOrdered(AerospikeClient *self, PyObject *args,
										 PyObject *kwds);

/*******************************************************************************
 * LIST FUNCTIONS(CDT)
 ******************************************************************************/
/**
 * Append a single val to the list value in bin.
 *
 *		client.list_append((x,y,z))
 *
 */
PyObject *AerospikeClient_ListAppend(AerospikeClient *self, PyObject *args,
									 PyObject *kwds);

/**
 * Extend the list value in bin with the given items.
 *
 *		client.list_extend((x,y,z))
 *
 */
PyObject *AerospikeClient_ListExtend(AerospikeClient *self, PyObject *args,
									 PyObject *kwds);

/**
 * Insert val at the specified index of the list value in bin.
 *
 *		client.list_insert((x,y,z))
 *
 */
PyObject *AerospikeClient_ListInsert(AerospikeClient *self, PyObject *args,
									 PyObject *kwds);

/**
 * Insert the items at the specified index of a list value in bin.
 *
 *		client.list_insert_items((x,y,z))
 *
 */
PyObject *AerospikeClient_ListInsertItems(AerospikeClient *self, PyObject *args,
										  PyObject *kwds);

/**
 * Remove and get back a list element at a given index of a list value in bin.
 *
 *		client.list_pop((x,y,z))
 *
 */
PyObject *AerospikeClient_ListPop(AerospikeClient *self, PyObject *args,
								  PyObject *kwds);

/**
 * Remove and get back a list element at a given index of a list value in bin.
 *
 *		client.list_pop_range((x,y,z))
 *
 */
PyObject *AerospikeClient_ListPopRange(AerospikeClient *self, PyObject *args,
									   PyObject *kwds);

/**
 * Remove a list element at a given index of a list value in bin.
 *
 *		client.list_remove((x,y,z))
 *
 */
PyObject *AerospikeClient_ListRemove(AerospikeClient *self, PyObject *args,
									 PyObject *kwds);

/**
 * Remove list elements at a given index of a list value in bin.
 *
 *		client.list_remove_range((x,y,z))
 *
 */
PyObject *AerospikeClient_ListRemoveRange(AerospikeClient *self, PyObject *args,
										  PyObject *kwds);

/**
 * Remove all the elements from a list value in bin.
 *
 *		client.list_clear((x,y,z))
 *
 */
PyObject *AerospikeClient_ListClear(AerospikeClient *self, PyObject *args,
									PyObject *kwds);

/**
 * Set list element val at the specified index of a list value in bin.
 *
 *		client.list_set((x,y,z))
 *
 */
PyObject *AerospikeClient_ListSet(AerospikeClient *self, PyObject *args,
								  PyObject *kwds);

/**
 * Get the list element at the specified index of a list value in bin.
 *
 *		client.list_get((x,y,z))
 *
 */
PyObject *AerospikeClient_ListGet(AerospikeClient *self, PyObject *args,
								  PyObject *kwds);

/**
 * Get the list of count elements starting at a specified index of a list value in bin.
 *
 *		client.list_get_range((x,y,z))
 *
 */
PyObject *AerospikeClient_ListGetRange(AerospikeClient *self, PyObject *args,
									   PyObject *kwds);

/**
 * Remove elements from the list which are not within the range starting at the
 * given index plus count.
 *
 *		client.list_trim((x,y,z))
 *
 */
PyObject *AerospikeClient_ListTrim(AerospikeClient *self, PyObject *args,
								   PyObject *kwds);

/**
 * Count the elements of the list value in bin.
 *
 *		client.list_size((x,y,z))
 *
 */
PyObject *AerospikeClient_ListSize(AerospikeClient *self, PyObject *args,
								   PyObject *kwds);

/*******************************************************************************
 * MAP FUNCTIONS
 ******************************************************************************/
/**
 * Append a single val to the map value in bin.
 *
 *		client.map_put((x,y,z))
 *
 */

PyObject *AerospikeClient_MapSetPolicy(AerospikeClient *self, PyObject *args,
									   PyObject *kwds);
PyObject *AerospikeClient_MapPut(AerospikeClient *self, PyObject *args,
								 PyObject *kwds);
PyObject *AerospikeClient_MapPutItems(AerospikeClient *self, PyObject *args,
									  PyObject *kwds);
PyObject *AerospikeClient_MapIncrement(AerospikeClient *self, PyObject *args,
									   PyObject *kwds);
PyObject *AerospikeClient_MapDecrement(AerospikeClient *self, PyObject *args,
									   PyObject *kwds);
PyObject *AerospikeClient_MapSize(AerospikeClient *self, PyObject *args,
								  PyObject *kwds);
PyObject *AerospikeClient_MapClear(AerospikeClient *self, PyObject *args,
								   PyObject *kwds);

PyObject *AerospikeClient_MapRemoveByKey(AerospikeClient *self, PyObject *args,
										 PyObject *kwds);
PyObject *AerospikeClient_MapRemoveByKeyList(AerospikeClient *self,
											 PyObject *args, PyObject *kwds);
PyObject *AerospikeClient_MapRemoveByKeyRange(AerospikeClient *self,
											  PyObject *args, PyObject *kwds);

PyObject *AerospikeClient_MapRemoveByValue(AerospikeClient *self,
										   PyObject *args, PyObject *kwds);
PyObject *AerospikeClient_MapRemoveByValueList(AerospikeClient *self,
											   PyObject *args, PyObject *kwds);
PyObject *AerospikeClient_MapRemoveByValueRange(AerospikeClient *self,
												PyObject *args, PyObject *kwds);

PyObject *AerospikeClient_MapRemoveByIndex(AerospikeClient *self,
										   PyObject *args, PyObject *kwds);
PyObject *AerospikeClient_MapRemoveByIndexRange(AerospikeClient *self,
												PyObject *args, PyObject *kwds);

PyObject *AerospikeClient_MapRemoveByRank(AerospikeClient *self, PyObject *args,
										  PyObject *kwds);
PyObject *AerospikeClient_MapRemoveByRankRange(AerospikeClient *self,
											   PyObject *args, PyObject *kwds);

PyObject *AerospikeClient_MapGetByKey(AerospikeClient *self, PyObject *args,
									  PyObject *kwds);
PyObject *AerospikeClient_MapGetByKeyRange(AerospikeClient *self,
										   PyObject *args, PyObject *kwds);

PyObject *AerospikeClient_MapGetByValue(AerospikeClient *self, PyObject *args,
										PyObject *kwds);
PyObject *AerospikeClient_MapGetByValueRange(AerospikeClient *self,
											 PyObject *args, PyObject *kwds);
PyObject *AerospikeClient_MapGetByValueList(AerospikeClient *self,
											PyObject *args, PyObject *kwds);
PyObject *AerospikeClient_MapGetByKeyList(AerospikeClient *self, PyObject *args,
										  PyObject *kwds);

PyObject *AerospikeClient_MapGetByIndex(AerospikeClient *self, PyObject *args,
										PyObject *kwds);
PyObject *AerospikeClient_MapGetByIndexRange(AerospikeClient *self,
											 PyObject *args, PyObject *kwds);

PyObject *AerospikeClient_MapGetByRank(AerospikeClient *self, PyObject *args,
									   PyObject *kwds);
PyObject *AerospikeClient_MapGetByRankRange(AerospikeClient *self,
											PyObject *args, PyObject *kwds);

/*******************************************************************************
 * SCAN OPERATIONS
 ******************************************************************************/

/**
 * Performs a `scan` operation. This will initialize a scan object, which can
 * be used to scan records in specified namespace and/or set.
 *
 * A scan can be executed by calling `foreach`, which will call a callback
 * each result returned:
 *
 *		def each_result(record):
 *			print record
 *
 *		scan = client.scan(ns,set).foreach(each_result)
 *
 * Alternatively, you can use `results()` which will return a list containing
 * all the results.
 *
 *
 */
AerospikeScan *AerospikeClient_Scan(AerospikeClient *self, PyObject *args,
									PyObject *kwds);
PyObject *AerospikeClient_ScanApply(AerospikeClient *self, PyObject *args,
									PyObject *kwds);
PyObject *AerospikeClient_ScanInfo(AerospikeClient *self, PyObject *args,
								   PyObject *kwds);

/*******************************************************************************
 * QUERY OPERATIONS
 ******************************************************************************/

/**
 * Performs a `query` operation. This will initialize a query object, which
 * can be used to query records in specified namespace and/or set.
 *
 * A query can be executed by calling `foreach`, which will call a callback
 * each result returned:
 *
 *		def each_result(result):
 *			print result
 *
 *		scan = client.query(ns,set).where("a", between(1,100)).foreach(each_result)
 *
 * Alternatively, you can use `results()` which is a generator that will yield a
 * result for each iteration:
 *
 *		for result in client.query(ns,set).where("a", range(1,100)).results():
 *			print result
 *
 */
AerospikeQuery *AerospikeClient_Query(AerospikeClient *self, PyObject *args,
									  PyObject *kwds);
PyObject *AerospikeClient_QueryApply(AerospikeClient *self, PyObject *args,
									 PyObject *kwds);
PyObject *AerospikeClient_JobInfo(AerospikeClient *self, PyObject *args,
								  PyObject *kwds);

/*******************************************************************************
 * INFO OPERATIONS
 ******************************************************************************/

/**
 * Performs a `info` operation. This will invoke the info request against a single
 * node in the cluster. The return value is a string. If an error occurred on
 * the node, the Error will be an object containing details, otherwise it is
 * None. If the request was successful, then the Response will contain the
 * string response from the node, otherwise it is None.
 *
 *		client.info_single_node('statistics', 'BB9581F41290C00')
 *
 */
PyObject *AerospikeClient_InfoSingleNode(AerospikeClient *self, PyObject *args,
										 PyObject *kwds);

/**
 * Performs a `info` operation. This will invoke the info request against a random
 * node in the cluster. The return value is a string. If an error occurred on
 * the node, the Error will be an object containing details, otherwise it is
 * None. If the request was successful, then the Response will contain the
 * string response from the node, otherwise it is None.
 *
 *		client.info_random_node('statistics')
 *
 */
PyObject *AerospikeClient_InfoRandomNode(AerospikeClient *self, PyObject *args,
										 PyObject *kwds);

/**
 *  ----------------------------- DEPRECATED ---------------------------------
 * Please use info_single_node() instead.
 * Performs a `info` operation. This will invoke the info request against each
 * node in the cluster. The return value is a dict where the key is the node
 * name and the value is a tuple of (Error,Response). If an error occurred on
 * the node, the Error will be an object containing details, otherwise it is
 * None. If the request was successful, then the Response will contain the
 * string response from the node, otherwise it is None.
 *
 *		for node,(err,res) in client.info('statistics').items():
 *			if err == None:
 *				print "{0} - OK: {1}".format(record,res)
 *			else:
 *				print "{0} - ERR: {1}".format(record,err)
 *
 */
PyObject *AerospikeClient_InfoNode(AerospikeClient *self, PyObject *args,
								   PyObject *kwds);

/*******************************************************************************
 * UDF OPERATIONS
 ******************************************************************************/
/**
 * Registers a new UDF.
 *
 *		client.udf_put(policy, filename, udf_type)
 *
 */
PyObject *AerospikeClient_UDF_Put(AerospikeClient *self, PyObject *args,
								  PyObject *kwds);

/**
 * De-registers a UDF.
 *
 *		client.udf_remove(policy, filename)
 *
 */
PyObject *AerospikeClient_UDF_Remove(AerospikeClient *self, PyObject *args,
									 PyObject *kwds);

/**
 * Lists the UDFs
 *
 *		client.udf_list(policy)
 *
 */
PyObject *AerospikeClient_UDF_List(AerospikeClient *self, PyObject *args,
								   PyObject *kwds);

/**
 * Gets the registered UDFs
 *
 *		client.udf_get(module,policy,language)
 *
 */
PyObject *AerospikeClient_UDF_Get_UDF(AerospikeClient *self, PyObject *args,
									  PyObject *kwds);

/*******************************************************************************
 * SECONDARY INDEX OPERATIONS
 ******************************************************************************/
/**
 * Create secondary integer index
 *
 *		client.index_integer_create(namespace, set, bin, index_name, policy)
 *
 */
PyObject *AerospikeClient_Index_Integer_Create(AerospikeClient *self,
											   PyObject *args, PyObject *kwds);

/**
 * Create secondary string index
 *
 *		client.index_string_create(namespace, set, bin, index_name, policy)
 *
 */
PyObject *AerospikeClient_Index_String_Create(AerospikeClient *self,
											  PyObject *args, PyObject *kwds);

/**
 * Create secondary geospatial index
 *
 *		client.index_2dsphere_create(namespace, set, bin, index_name, policy)
 *
 */
PyObject *AerospikeClient_Index_2dsphere_Create(AerospikeClient *self,
												PyObject *args, PyObject *kwds);

/**
 * Remove secondary index
 *
 *		client.index_remove(policy, namespace, index_name)
 *
 */
PyObject *AerospikeClient_Index_Remove(AerospikeClient *self, PyObject *args,
									   PyObject *kwds);
/**
 * Create secondary list index
 *
 *		client.index_list_create(policy, namespace, set, bin, index_name)
 *
 */
PyObject *AerospikeClient_Index_List_Create(AerospikeClient *self,
											PyObject *args, PyObject *kwds);
/**
 * Create secondary map keys index
 *
 *		client.index_map_keys_create(policy, namespace, set, bin, index_name)
 *
 */
PyObject *AerospikeClient_Index_Map_Keys_Create(AerospikeClient *self,
												PyObject *args, PyObject *kwds);
/**
 * Create secondary map values index
 *
 *		client.index_map_values_create(policy, namespace, set, bin, index_name)
 *
 */
PyObject *AerospikeClient_Index_Map_Values_Create(AerospikeClient *self,
												  PyObject *args,
												  PyObject *kwds);

/*******************************************************************************
 * LOG OPERATIONS
 ******************************************************************************/
/**
 * Sets the log level
 *
 *		client.setLogLevel()
 *
 */
PyObject *AerospikeClient_Set_Log_Level(AerospikeClient *self, PyObject *args,
										PyObject *kwds);

/**
 * Sets the log handler
 *
 *		client.setLogHandler()
 *
 */
PyObject *AerospikeClient_Set_Log_Handler(AerospikeClient *self, PyObject *args,
										  PyObject *kwds);

/**
 * Get records in a batch
 *
 *		client.get_many([keys], policies)
 *
 */
PyObject *AerospikeClient_Get_Many(AerospikeClient *self, PyObject *args,
								   PyObject *kwds);

/**
 * Get records in a batch
 *
 *		client.batch_get_ops([keys], policies)
 *
 */
PyObject *AerospikeClient_Batch_GetOps(AerospikeClient *self, PyObject *args,
								   		PyObject *kwds);

/**
 * Filter bins from records in a batch
 *
 *		client.select_many([keys], [bins], policies)
 *
 */
PyObject *AerospikeClient_Select_Many(AerospikeClient *self, PyObject *args,
									  PyObject *kwds);

/**
 * Check existence of given keys
 *
 *		client.exists_many([keys], policies)
 *
 */
PyObject *AerospikeClient_Exists_Many(AerospikeClient *self, PyObject *args,
									  PyObject *kwds);

/**
* Perform xdr-set-filter info operation on the database.
*
* client.set_xdr_filter(data_center, namespace, expression_filter, policy)
*
*/
PyObject *AerospikeClient_SetXDRFilter(AerospikeClient *self, PyObject *args,
									   PyObject *kwds);

/**
 * Send an info request to the entire cluster
 * client.info_all("statistics", {}")
*/
PyObject *AerospikeClient_InfoAll(AerospikeClient *self, PyObject *args,
								  PyObject *kwds);

/**
* Perform info operation on the database.
*
* client.info((x,y,z))
*
*/
PyObject *AerospikeClient_Info(AerospikeClient *self, PyObject *args,
							   PyObject *kwds);

/**
* Perform get nodes operation on the database.
*
* client.get_nodes((x,y,z))
*
*/
PyObject *AerospikeClient_GetNodes(AerospikeClient *self, PyObject *args,
								   PyObject *kwds);
/**
* Perform get node name operation on the database.
*
* client.get_node_names((x,y,z))
*
*/
PyObject *AerospikeClient_GetNodeNames(AerospikeClient *self, PyObject *args,
									   PyObject *kwds);
/**
* Perforrm get key digest operation on the database.
*
* client.get_key_digest((x,y,z))
*
*/
PyObject *AerospikeClient_Get_Key_Digest(AerospikeClient *self, PyObject *args,
										 PyObject *kwds);
/**
* Perform get key's partition id from cluster.
*
* client.get_key_partition_id((x,y,z))
*
*/
PyObject *AerospikeClient_Get_Key_PartitionID(AerospikeClient *self,
											  PyObject *args, PyObject *kwds);
/**
 * Return search string for host port combination
 */
char *return_search_string(aerospike *as);
/**
 * Close the aerospike object depending on the global_hosts entries
 */
void close_aerospike_object(aerospike *as, as_error *err, char *alias_to_search,
							PyObject *py_persistent_item, bool do_destroy);
/**
 * Check type for 'operate' operation
 */
int check_type(AerospikeClient *self, PyObject *py_value, int op,
			   as_error *err);

/*******************************************************************************
 * TRUNCATE OPERATIONS
 ******************************************************************************/

PyObject *AerospikeClient_Truncate(AerospikeClient *self, PyObject *args,
								   PyObject *kwds);
