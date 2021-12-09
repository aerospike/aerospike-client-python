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

#include <aerospike/as_admin.h>
#include <aerospike/as_key.h>
#include <aerospike/as_error.h>
#include <aerospike/as_record.h>
#include <aerospike/as_udf.h>
#include <aerospike/as_operations.h>
#include <aerospike/aerospike_batch.h>
#include <aerospike/as_exp.h>
#include <aerospike/as_partition_filter.h>

#include "types.h"

#define CTX_KEY "ctx"

as_status as_udf_file_to_pyobject(as_error *err, as_udf_file *entry,
								  PyObject **py_file);

as_status as_udf_files_to_pyobject(as_error *err, as_udf_files *files,
								   PyObject **py_files);

as_status strArray_to_py_list(as_error *err, int num_elements, int element_size,
							  char str_array_ptr[][element_size],
							  PyObject *py_list);

as_status char_double_ptr_to_py_list(as_error *err, int num_elements,
									 int element_size, char **str_array_ptr,
									 PyObject *py_list);

as_status as_user_to_pyobject(as_error *err, as_user *user,
							  PyObject **py_as_user);

as_status as_user_info_to_pyobject(as_error *err, as_user *user,
								   PyObject **py_as_user);

as_status as_user_array_to_pyobject(as_error *err, as_user **users,
									PyObject **py_as_users, int users_size);

as_status as_user_info_array_to_pyobject(as_error *err, as_user **users,
										 PyObject **py_as_users,
										 int users_size);

as_status pyobject_to_strArray(as_error *err, PyObject *py_list, char **arr,
							   uint32_t max_len);

as_status pyobject_to_val(AerospikeClient *self, as_error *err,
						  PyObject *py_obj, as_val **val,
						  as_static_pool *static_pool, int serializer_type);

as_status pyobject_to_map(AerospikeClient *self, as_error *err,
						  PyObject *py_dict, as_map **map,
						  as_static_pool *static_pool, int serializer_type);

as_status pyobject_to_list(AerospikeClient *self, as_error *err,
						   PyObject *py_list, as_list **list,
						   as_static_pool *static_pool, int serializer_type);

as_status pyobject_to_key(as_error *err, PyObject *py_key, as_key *key);

as_status pyobject_to_index(AerospikeClient *self, as_error *err,
							PyObject *py_value, long *long_val);

as_status pyobject_to_record(AerospikeClient *self, as_error *err,
							 PyObject *py_rec, PyObject *py_meta,
							 as_record *rec, int serializer_option,
							 as_static_pool *static_pool);

as_status val_to_pyobject(AerospikeClient *self, as_error *err,
						  const as_val *val, PyObject **py_map);

as_status val_to_pyobject_cnvt_list_to_map(AerospikeClient *self, as_error *err,
										   const as_val *val,
										   PyObject **py_map);

as_status map_to_pyobject(AerospikeClient *self, as_error *err,
						  const as_map *map, PyObject **py_map);

as_status list_to_pyobject(AerospikeClient *self, as_error *err,
						   const as_list *list, PyObject **py_list);

as_status as_list_of_map_to_py_tuple_list(AerospikeClient *self, as_error *err,
										  const as_list *list,
										  PyObject **py_list);

as_status record_to_pyobject(AerospikeClient *self, as_error *err,
							 const as_record *rec, const as_key *key,
							 PyObject **obj);
							 
as_status record_to_resultpyobject(AerospikeClient *self, 
								as_error *err,
								const as_record *rec,
								PyObject **obj);

as_status operate_bins_to_pyobject(AerospikeClient *self, as_error *err,
								   const as_record *rec, PyObject **py_bins);

as_status record_to_pyobject_cnvt_list_to_map(AerospikeClient *self,
											  as_error *err,
											  const as_record *rec,
											  const as_key *key,
											  PyObject **obj);

as_status key_to_pyobject(as_error *err, const as_key *key, PyObject **obj);

as_status metadata_to_pyobject(as_error *err, const as_record *rec,
							   PyObject **obj);

as_status bins_to_pyobject(AerospikeClient *self, as_error *err,
						   const as_record *rec, PyObject **obj,
						   bool cnvt_list_to_map);

bool error_to_pyobject(const as_error *err, PyObject **obj);

as_status pyobject_to_astype_write(AerospikeClient *self, as_error *err,
								   PyObject *py_value, as_val **val,
								   as_static_pool *static_pool,
								   int serializer_type);

as_status as_privilege_to_pyobject(as_error *err, as_privilege privileges[],
								   PyObject *py_as_privilege,
								   int privilege_size);

as_status as_role_to_pyobject_old(as_error *err, as_role *role,
								  PyObject **py_as_role);

as_status as_role_to_pyobject(as_error *err, as_role *role,
							  PyObject *py_as_role);

as_status as_role_array_to_pyobject_old(as_error *err, as_role **roles,
										PyObject **py_as_roles, int roles_size);

as_status as_role_array_to_pyobject(as_error *err, as_role **roles,
									PyObject **py_as_roles, int roles_size);

as_status pyobject_to_as_privileges(as_error *err, PyObject *py_privileges,
									as_privilege **privileges,
									int privileges_size);

void initialize_bin_for_strictypes(AerospikeClient *self, as_error *err,
								   PyObject *py_value, as_binop *binop,
								   char *bin, as_static_pool *static_pool);

as_status bin_strict_type_checking(AerospikeClient *self, as_error *err,
								   PyObject *py_bin, char **bin);

as_status check_for_meta(PyObject *py_meta, as_operations *ops, as_error *err);

as_status as_batch_read_results_to_pyobject(as_error *err,
											AerospikeClient *client,
											const as_batch_read *results,
											uint32_t size,
											PyObject **py_records);

as_status batch_read_records_to_pyobject(AerospikeClient *self, as_error *err,
										 as_batch_read_records *records,
										 PyObject **py_recs);

as_status string_and_pyuni_from_pystring(PyObject *py_string,
										 PyObject **pyuni_r, char **c_str_ptr,
										 as_error *err);

as_status get_cdt_ctx(AerospikeClient *self, as_error *err, as_cdt_ctx *cdt_ctx,
					  PyObject *op_dict, bool *ctx_in_use,
					  as_static_pool *static_pool, int serializer_type);

as_status convert_predexp_list(PyObject *py_predexp_list,
							   as_predexp_list *predexp_list, as_error *err);

as_status convert_exp_list(AerospikeClient *self, PyObject *py_exp_list,
						   as_exp **exp_list, as_error *err);

as_status convert_partition_filter(AerospikeClient *self,
								   PyObject *py_partition_filter,
								   as_partition_filter *partition_filter,
								   as_partitions_status **ps,
								   as_error *err);

as_status get_int_from_py_int(as_error *err, PyObject *py_long,
							  int *int_pointer, const char *py_object_name);
