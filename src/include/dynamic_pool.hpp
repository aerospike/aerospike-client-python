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
#define AS_DYNAMIC_POOL_BYTES_PER_GROUP_MIN 128
#define AS_DYNAMIC_POOL_BYTES_PER_GROUP_MAX 32768
#define AS_DYNAMIC_POOL_GROUPS_PER_ALLOCATION 4

/**
 * Pool of as_bytes that grows dynamically.
 *
 * @attr byte_group_table Table which contains groups of as_bytes.
 * @attr group_iterator Group which is currently being filled
 * @attr byte_iterator Index of the next byte to be used
 * @attr bytes_per_group number of bytes in the current group.
 * @attr allocate_buffers boolean value determining if bytes should be heap allocated in cases where stack allocation is also available.
 * @attr free_buffers boolean value determining if as_bytes_destroy should be called upon destroying the pool.
 *        If the raw bytes array is heap allocated, free_buffers should be true.
 *
 */
typedef struct bytes_dynamic_pool {
    as_bytes **byte_group_table;
    uint16_t group_iterator;
    uint16_t byte_iterator;
    uint16_t bytes_per_group;
    bool allocate_buffers;
    bool free_buffers;
} as_dynamic_pool;

/**
 * Allocates a group of as_bytes to the table.
 *
 * @param dynamic_pool Pointer to a dynamic pool.
 *
 */
static inline void dynamic_pool_malloc_group(as_dynamic_pool *dynamic_pool,
                                       as_error *err)
{
    // Table of groups of bytes.
    as_bytes **table = dynamic_pool->byte_group_table;
    // group number to allocate next.
    uint16_t group_num_to_allocate = dynamic_pool->group_iterator;
    // bytes number to allocate in the next group.
    uint16_t num_bytes_to_allocate = dynamic_pool->bytes_per_group;

    table[group_num_to_allocate] = (as_bytes *)cf_malloc(num_bytes_to_allocate * sizeof(as_bytes));

    // If allocation fails, throw an error.
    if (table[group_num_to_allocate] == NULL) {
        as_error_update(err, AEROSPIKE_ERR,
                        "Failed to allocated memory for a group of bytes");
    }
}

/**
 * Manages and adjusts the number of bytes per group.
 * 
 * bytes_per_group begins at AS_DYNAMIC_POOL_BYTES_PER_GROUP_MIN and cannot exceed AS_DYNAMIC_POOL_BYTES_PER_GROUP_MAX
 *
 * @param dynamic_pool Pointer to a dynamic pool.
 *
 */
static inline void dynamic_pool_shift_bytes_per_group_if_needed(as_dynamic_pool *dynamic_pool)
{
    if (dynamic_pool->bytes_per_group < AS_DYNAMIC_POOL_BYTES_PER_GROUP_MAX) {
        dynamic_pool->bytes_per_group <<= 1;
    }
}

/**
 * Expands the table if more groups are needed.
 *
 * @param dynamic_pool Pointer to a dynamic pool.
 * @param error Pointer to an as_error
 *
 */
static inline void dynamic_pool_expand_table_if_needed(as_dynamic_pool *dynamic_pool, as_error* err)
{   
    // Table containing groups of bytes.
    as_bytes **table = dynamic_pool->byte_group_table;
    // Holds the index of the current byte group 
    uint16_t group_iterator = dynamic_pool->group_iterator;


    // Allocate new groups each time the group iterator reaches a multiple of AS_DYNAMIC_POOL_GROUPS_PER_ALLOCATION
    bool allocate_more_groups = (group_iterator % AS_DYNAMIC_POOL_GROUPS_PER_ALLOCATION) == 0;
    // Allocate using malloc if no groups have be allocated.
    bool allocate_first_group = (group_iterator) == 0;
    if (allocate_more_groups) {
        if(allocate_first_group){
            table = (as_bytes **) cf_malloc(AS_DYNAMIC_POOL_GROUPS_PER_ALLOCATION * sizeof(as_bytes *));
        }
        else{
            table = (as_bytes **) realloc(table, (group_iterator + AS_DYNAMIC_POOL_GROUPS_PER_ALLOCATION) * sizeof(as_bytes *));
        }
    }
    if (table == NULL) {
        if(allocate_first_group){
            as_error_update(err, AEROSPIKE_ERR,
                "Failed to allocate memory for the creation of byte group table");

        }
        else{
            as_error_update(err, AEROSPIKE_ERR,
                "Failed to reallocate memory for a byte group table expansion");
        }
    }
    else{
        // Reassign table back to dynamic_pool
        dynamic_pool->byte_group_table = table;
    }
}

/**
 * Frees all the bytes in a group.
 *
 * @param dynamic_pool Pointer to a dynamic pool.
 * @param group_index group index to free.
 * @param num_bytes number of bytes to free
 *
 */
static inline void dynamic_pool_destroy_bytes_in_group(as_dynamic_pool *dynamic_pool, uint16_t group_index, uint16_t num_bytes)
{
    as_bytes* group = dynamic_pool->byte_group_table[group_index];
    for (uint16_t i = 0; i < num_bytes; i++) {
        as_bytes_destroy(&(group[i]));
    }
}


/**
 * Frees all the data from a group in the table.
 *
 * @param dynamic_pool Pointer to a dynamic pool.
 * @param group_index group index to free.
 * @param num_bytes number of bytes to free
 *
 */
static inline void dynamic_pool_free_group(as_dynamic_pool *dynamic_pool, uint16_t group_index, uint16_t num_bytes)
{
    // Destroy bytes if buffer value is allocated on the heap
    if (dynamic_pool->free_buffers) {
        dynamic_pool_destroy_bytes_in_group(dynamic_pool, group_index, num_bytes);
    }
    as_bytes* group = dynamic_pool->byte_group_table[group_index];
    cf_free(group);
}

/**
 * Frees all the data from the table of the dynamic pool.
 *
 * @param dynamic_pool Pointer to a dynamic pool. *
 */
static inline void dynamic_pool_free_table(as_dynamic_pool *dynamic_pool){
    // Set bytes_per_group back to minimum value to traverse byte pool from the front.
    dynamic_pool->bytes_per_group = AS_DYNAMIC_POOL_BYTES_PER_GROUP_MIN;
    // Free all previous byte groups.
    for (uint16_t group_index = 0; group_index < dynamic_pool->group_iterator; group_index++) {
        dynamic_pool_free_group(dynamic_pool, group_index, dynamic_pool->bytes_per_group);
        dynamic_pool_shift_bytes_per_group_if_needed(dynamic_pool);
    }
    // Free the current byte group.
    dynamic_pool_free_group(dynamic_pool, dynamic_pool->group_iterator, dynamic_pool->byte_iterator);
    // Free the table.
    cf_free(dynamic_pool->byte_group_table);
}


/**
 * Fully initializes a null intialized dynamic pool.
 * 
 * If the group table is full, the table is also expanded.
 *
 * @param dynamic_pool Pointer to a dynamic pool.
 * @param err Pointer to an as_error
 *
 */
static inline void dynamic_pool_init(as_dynamic_pool *dynamic_pool,
                                     as_error *err)
{
    dynamic_pool->group_iterator = 0;
    dynamic_pool->byte_iterator = 0;
    dynamic_pool->bytes_per_group = AS_DYNAMIC_POOL_BYTES_PER_GROUP_MIN;

    dynamic_pool_expand_table_if_needed(dynamic_pool, err);

    dynamic_pool_malloc_group(dynamic_pool, err);
}

/**
 * Adds a new group to the dynamic pool.
 * 
 * If the group table is full, the table is also expanded.
 *
 * @param dynamic_pool Pointer to a dynamic pool.
 * @param err Pointer to an as_error
 *
 */
static inline void dynamic_pool_add_group(as_dynamic_pool *dynamic_pool,
                                       as_error *err)
{
    dynamic_pool->byte_iterator = 0;
    dynamic_pool->group_iterator++;


    dynamic_pool_expand_table_if_needed(dynamic_pool, err);

    dynamic_pool_shift_bytes_per_group_if_needed(dynamic_pool);
    dynamic_pool_malloc_group(dynamic_pool, err);
}

/**
 * Initializes the byte pool. Must be called anytime a dynamic pool is declared. 
 *
 * @param dynamic_pool Pointer to a dynamic pool.
 */
#define BYTE_POOL_INIT_NULL(dynamic_pool)                                       \
    (dynamic_pool)->byte_group_table = NULL;                                    \
    (dynamic_pool)->allocate_buffers = false;                                   \
    (dynamic_pool)->free_buffers = false;

/**
 * Fetches the address of the next as_byte in the pool.
 * 
 * @param map_bytes Pointer to an as_bytes.
 * @param dynamic_pool Pointer to a dynamic pool.
 * @param err Pointer to an as_error
 */
static inline as_bytes* GET_BYTES_POOL(as_dynamic_pool *dynamic_pool, as_error *err) {
    as_bytes **table = dynamic_pool->byte_group_table;

    if (table == NULL) {
        dynamic_pool_init(dynamic_pool, err);
    } else if (dynamic_pool->byte_iterator >= dynamic_pool->bytes_per_group) {
        dynamic_pool_add_group(dynamic_pool, err);
    }

    table = dynamic_pool->byte_group_table;
    uint16_t group_iterator = dynamic_pool->group_iterator;
    as_bytes *group = table[group_iterator];
    uint16_t byte_iterator = dynamic_pool->byte_iterator++;

    return &group[byte_iterator];
}

/**
 * Destroy the dynamic pool. Must be called before the dynamic_pool loses scope.
 * 
 * @param dynamic_pool Pointer to a dynamic pool.
 */
static inline void DESTROY_DYNAMIC_POOL(as_dynamic_pool *dynamic_pool) {
    if(dynamic_pool->allocate_buffers) {
        dynamic_pool->free_buffers = true;
    }
    if (dynamic_pool->byte_group_table != NULL) {
        dynamic_pool_free_table(dynamic_pool);
    }
}
