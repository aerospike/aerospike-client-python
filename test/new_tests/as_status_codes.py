# Error Codes and messages for various exceptions

'''
Creates Classes so that we can access them like: cls.err_type.err_code
Roughly maps to: exception_types.h
'''


class AerospikeStatus(object):
    AEROSPIKE_OK = 0
    AEROSPIKE_SERVER_ERROR = -10
    AEROSPIKE_INVALID_HOST = -4
    AEROSPIKE_ERR_CLIENT = -1
    AEROSPIKE_ERR_PARAM = -2
    AEROSPIKE_ERR_RECORD_NOT_FOUND = 2
    AEROSPIKE_ERR_RECORD_GENERATION = 3
    AEROSPIKE_ERR_REQUEST_INVALID = 4
    AEROSPIKE_CLUSTER_ERROR = 11
    AEROSPIKE_ERR_BIN_INCOMPATIBLE_TYPE = 12
    AEROSPIKE_ERR_NAMESPACE_NOT_FOUND = 20
    AEROSPIKE_ERR_UDF = 100
    AEROSPIKE_ERR_INDEX_NOT_FOUND = 201
    LUA_FILE_NOT_FOUND = 1302
