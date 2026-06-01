#include <stdint.h>
#include <stdbool.h>
#include <aerospike/as_bytes.h>
#include <aerospike/as_error.h>
#include <citrusleaf/alloc.h>

typedef struct as_dynamic_pool;

/**
 * Fetches the address of the next as_byte in the pool.
 *
 * @param map_bytes Pointer to an as_bytes.
 * @param dynamic_pool Pointer to a dynamic pool.
 * @param err Pointer to an as_error
 */
static inline as_bytes *
as_dynamic_pool_get_as_bytes(as_dynamic_pool *dynamic_pool, as_error *err);

/**
 * Initializes the byte pool. Must be called anytime a dynamic pool is declared.
 *
 * @param dynamic_pool Pointer to a dynamic pool.
 */
#define BYTE_POOL_INIT_NULL(dynamic_pool)                                      \
    (dynamic_pool)->byte_group_table = NULL;                                   \
    (dynamic_pool)->allocate_buffers = false;
