project('aerospike', 'c')

c_client_msgs = run_command('make', '-C', 'aerospike-client-c', check: true)
message(c_client_msgs.stdout())
message(c_client_msgs.stderr())

os = run_command('uname', check: true)
arch = run_command('uname', '-m', check: true)
target_folder = os.stdout().strip() + '-' + arch.stdout().strip()
c_client_target_folder = 'aerospike-client-c/target/' + target_folder

incdir = include_directories(
    c_client_target_folder + '/include',
    'src/include'
)

py = import('python').find_installation(pure: false)

c_compiler = meson.get_compiler('c')
lib_search_dir = meson.current_source_dir() + '/' + c_client_target_folder + '/lib/'
# message(' - ', lib_search_dir)
c_client_lib = c_compiler.find_library('aerospike', dirs: [lib_search_dir], static: true)
ssl_lib = c_compiler.find_library('ssl', static: true)
crypto_lib = c_compiler.find_library('crypto', static: true)
yaml_lib = c_compiler.find_library('yaml', static: true)

py.extension_module(
    'aerospike',
    'src/main/log.c',
    'src/main/transaction/type.c',
    'src/main/serializer.c',
    'src/main/config_provider/type.c',
    'src/main/predicates.c',
    'src/main/exception.c',
    'src/main/tls_config.c',
    'src/main/nullobject/type.c',
    'src/main/scan/execute_background.c',
    'src/main/scan/add_ops.c',
    'src/main/scan/foreach.c',
    'src/main/scan/apply.c',
    'src/main/scan/results.c',
    'src/main/scan/select.c',
    'src/main/scan/get_parts.c',
    'src/main/scan/type.c',
    'src/main/scan/paginate.c',
    'src/main/calc_digest.c',
    'src/main/conversions.c',
    'src/main/convert_expressions.c',
    'src/main/cdt_types/type.c',
    'src/main/policy.c',
    'src/main/policy_config.c',
    'src/main/key_ordered_dict/type.c',
    'src/main/global_hosts/type.c',
    'src/main/query/execute_background.c',
    'src/main/query/add_ops.c',
    'src/main/query/foreach.c',
    'src/main/query/where.c',
    'src/main/query/apply.c',
    'src/main/query/results.c',
    'src/main/query/select.c',
    'src/main/query/get_parts.c',
    'src/main/query/type.c',
    'src/main/query/paginate.c',
    'src/main/client/expression_operations.c',
    'src/main/client/batch_remove.c',
    'src/main/client/cdt_list_operate.c',
    'src/main/client/sec_index.c',
    'src/main/client/get_key_partition_id.c',
    'src/main/client/info.c',
    'src/main/client/remove_bin.c',
    'src/main/client/apply.c',
    'src/main/client/udf.c',
    'src/main/client/get_expression_base64.c',
    'src/main/client/metrics.c',
    'src/main/client/scan.c',
    'src/main/client/select.c',
    'src/main/client/admin.c',
    'src/main/client/batch_write.c',
    'src/main/client/get.c',
    'src/main/client/cdt_operation_utils.c',
    'src/main/client/truncate.c',
    'src/main/client/cdt_map_operate.c',
    'src/main/client/get_nodes.c',
    'src/main/client/bit_operate.c',
    'src/main/client/type.c',
    'src/main/client/query.c',
    'src/main/client/set_xdr_filter.c',
    'src/main/client/connect.c',
    'src/main/client/mrt.c',
    'src/main/client/info_single_node.c',
    'src/main/client/remove.c',
    'src/main/client/batch_read.c',
    'src/main/client/hll_operate.c',
    'src/main/client/batch_apply.c',
    'src/main/client/info_random_node.c',
    'src/main/client/operate.c',
    'src/main/client/put.c',
    'src/main/client/exists.c',
    'src/main/client/close.c',
    'src/main/client/batch_operate.c',
    'src/main/client/get_cdtctx_base64.c',
    'src/main/geospatial/loads.c',
    'src/main/geospatial/dumps.c',
    'src/main/geospatial/wrap.c',
    'src/main/geospatial/type.c',
    'src/main/geospatial/unwrap.c',
    'src/main/convert_partition_filter.c',
    'src/main/aerospike.c',
    install: true,
    dependencies: [
        c_client_lib,
        ssl_lib,
        crypto_lib,
        yaml_lib
    ],
    include_directories: incdir,
    c_args: ['-fno-strict-aliasing'],
    install_rpath: '$ORIGIN'
)
