/*
 *******************************************************************************************************
 * Static pool maintained to avoid runtime mallocs.
 * It comprises of following pools:
 * 1. Pool for Arraylist
 * 2. Pool for Hashmap
 * 3. Pool for Strings
 * 4. Pool for Integers
 * 5. Pool for Bytes
 *******************************************************************************************************
 */
#define AS_MAX_STORE_SIZE 4096

typedef struct bytes_static_pool {
	as_bytes bytes_pool[AS_MAX_STORE_SIZE];
	uint32_t current_bytes_id;
} as_static_pool;

#define BYTES_CNT(static_pool)                                                 \
	(((as_static_pool *)static_pool)->current_bytes_id)

#define BYTES_POOL(static_pool) ((as_static_pool *)static_pool)->bytes_pool

#define GET_BYTES_POOL(map_bytes, static_pool, err)                            \
	if (AS_MAX_STORE_SIZE > BYTES_CNT(static_pool)) {                          \
		map_bytes = &(BYTES_POOL(static_pool)[BYTES_CNT(static_pool)++]);      \
	}                                                                          \
	else {                                                                     \
		as_error_update(err, AEROSPIKE_ERR, "Cannot allocate as_bytes");       \
	}

#define POOL_DESTROY(static_pool)                                              \
	for (uint32_t iter = 0; iter < BYTES_CNT(static_pool); iter++) {           \
		as_bytes_destroy(&BYTES_POOL(static_pool)[iter]);                      \
	}
