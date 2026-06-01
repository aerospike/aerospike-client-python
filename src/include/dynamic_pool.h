#include <stdint.h>
#include <stdbool.h>
#include <aerospike/as_bytes.h>
#include <aerospike/as_error.h>
#include <citrusleaf/alloc.h>

/*
 *******************************************************************************************************
 * Dynamic pool maintained to avoid excessive runtime mallocs and efficiently use memory.
 *
 * The dynamic pool maintains a table composed of several groups of as_bytes buffers.
 * New groups are allocated dynamically after the current group is exhausted.
 * As more as_bytes are used, group sizes will grow to reduce malloc calls.

 * The dynamic pool does not allocate any memory unless bytes are used in a command.
 *
 *******************************************************************************************************
 */

/**
 * Pool of as_bytes that grows dynamically.
 *
 * @attr byte_group_table Table which contains groups of as_bytes.
 * @attr group_iterator Group which is currently being filled
 * @attr byte_iterator Index of the next byte to be used
 * @attr bytes_per_group number of bytes in the current group.
 *
 */
typedef struct as_dynamic_pool {
    as_bytes **byte_group_table;
    uint16_t group_iterator;
    uint16_t byte_iterator;
    uint16_t bytes_per_group;
    bool allocate_buffers;
} as_dynamic_pool;

/**
 * Fetches the address of the next as_byte in the pool.
 *
 * @param map_bytes Pointer to an as_bytes.
 * @param dynamic_pool Pointer to a dynamic pool.
 * @param err Pointer to an as_error
 */
as_bytes *as_dynamic_pool_get_as_bytes(as_dynamic_pool *dynamic_pool,
                                       as_error *err);

/**
 * Initializes the byte pool. Must be called anytime a dynamic pool is declared.
 *
 * @param dynamic_pool Pointer to a dynamic pool.
 */
#define BYTE_POOL_INIT_NULL(dynamic_pool)                                      \
    (dynamic_pool)->byte_group_table = NULL;                                   \
    (dynamic_pool)->allocate_buffers = false;
