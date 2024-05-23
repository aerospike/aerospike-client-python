/*
 *******************************************************************************************************
 * Dynamic pool maintained to avoid excessive runtime mallocs.
 * It is comprised of a Pool of pointers to pools of
 * as_bytes containing AS_DYNAMIC_POOL_BLOCK_SIZE as_bytes.
 *******************************************************************************************************
 */
#define AS_DYNAMIC_POOL_BLOCK_SIZE_MIN 128
#define AS_DYNAMIC_POOL_BLOCK_SIZE_MAX 32768
#define AS_DYNAMIC_POOL_POINTER_BLOCK_SIZE 4

typedef struct bytes_dynamic_pool {
    as_bytes **pool;
    uint16_t current_bytes_id;
    uint16_t block_id;
    uint16_t current_block_size;
} as_dynamic_pool;

/*
    PRIVATE HELPER FUNCTIONS
*/
static inline void dynamic_pool_malloc_block(as_dynamic_pool *dynamic_pool,
                                       as_error *err)
{
    dynamic_pool->pool[dynamic_pool->block_id] = (as_bytes *)malloc(sizeof(as_bytes) * dynamic_pool->current_block_size);
    if (dynamic_pool->pool[dynamic_pool->block_id] == NULL) {
        as_error_update(err, AEROSPIKE_ERR,
                        "Failed to allocated memory for dynamic_pool");
    }
}

static inline void dynamic_pool_shift_block_size_if_needed(as_dynamic_pool *dynamic_pool)
{
    if (dynamic_pool->current_block_size != AS_DYNAMIC_POOL_BLOCK_SIZE_MAX) {
        dynamic_pool->current_block_size <<= 1;
    }
}

static inline void dynamic_pool_expand_pool_if_needed(as_dynamic_pool *dynamic_pool, as_error* err)
{
    if (dynamic_pool->block_id % AS_DYNAMIC_POOL_POINTER_BLOCK_SIZE ==
        0) {
        dynamic_pool->pool = (as_bytes **)realloc(
            dynamic_pool->pool, (dynamic_pool->block_id +
                                        AS_DYNAMIC_POOL_POINTER_BLOCK_SIZE) *
    sizeof(as_bytes *));
    }
    if (dynamic_pool->pool == NULL) {
        as_error_update(err, AEROSPIKE_ERR,
                        "Failed to allocated memory for dynamic_pool");
    }
}

static inline void dynamic_pool_create_pool(as_dynamic_pool *dynamic_pool,
                                       as_error *err)
{
    dynamic_pool->pool = (as_bytes **)malloc(
        AS_DYNAMIC_POOL_POINTER_BLOCK_SIZE * sizeof(as_bytes *));
    if (dynamic_pool->pool == NULL) {
        as_error_update(err, AEROSPIKE_ERR,
                        "Failed to allocated memory for dynamic_pool");
    }
}

static inline void dynamic_pool_destroy_bytes_in_block(as_dynamic_pool *dynamic_pool, uint16_t block_num, uint16_t bytes_num)
{
    for (uint16_t i = 0; i < bytes_num; i++) {
        as_bytes_destroy(&dynamic_pool->pool[block_num][i]);
    }
}


static inline void dynamic_pool_free_blocks_and_bytes(as_dynamic_pool *dynamic_pool, bool free_buffer, uint16_t block_num, uint16_t bytes_num)
{
    if (free_buffer) {
        dynamic_pool_destroy_bytes_in_block(dynamic_pool, block_num, dynamic_pool->current_block_size);
    }
    cf_free(((as_dynamic_pool *)dynamic_pool)->pool[block_num]);
    dynamic_pool_shift_block_size_if_needed(dynamic_pool);
}

static inline void dynamic_pool_free_previous_blocks(as_dynamic_pool *dynamic_pool, bool free_buffer)
{
    for (uint16_t block_num = 0; block_num < dynamic_pool->block_id; block_num++) {
        dynamic_pool_free_blocks_and_bytes(dynamic_pool, free_buffer, block_num, dynamic_pool->current_block_size);
    }
}

static inline void dynamic_pool_free_current_block(as_dynamic_pool *dynamic_pool, bool free_buffer)
{
    dynamic_pool_free_blocks_and_bytes(dynamic_pool, free_buffer, dynamic_pool->block_id, dynamic_pool->current_bytes_id);
}


static inline void dynamic_pool_init(as_dynamic_pool *dynamic_pool,
                                     as_error *err)
{
    dynamic_pool->block_id = 0;
    dynamic_pool->current_bytes_id = 0;
    dynamic_pool->current_block_size = AS_DYNAMIC_POOL_BLOCK_SIZE_MIN;

    dynamic_pool_create_pool(dynamic_pool, err);

    dynamic_pool_malloc_block(dynamic_pool, err);
}

static inline void dynamic_pool_resize(as_dynamic_pool *dynamic_pool,
                                       as_error *err)
{
    dynamic_pool->current_bytes_id = 0;
    dynamic_pool->block_id++;

    dynamic_pool_shift_block_size_if_needed(dynamic_pool);

    dynamic_pool_expand_pool_if_needed(dynamic_pool, err);

    dynamic_pool_malloc_block(dynamic_pool, err);

}

/*
    PUBLIC MACROS
*/
#define GET_BYTES_POOL(map_bytes, dynamic_pool, err)                                                \
    if (dynamic_pool->pool == NULL) {                                                               \
        dynamic_pool_init(dynamic_pool, err);                                                       \
    }                                                                                               \
    if (dynamic_pool->current_block_size <= dynamic_pool->current_bytes_id) {                       \
        dynamic_pool_resize(dynamic_pool, err);                                                     \
    }                                                                                               \
    map_bytes = &(dynamic_pool->pool[dynamic_pool->block_id][dynamic_pool->current_bytes_id++]);

#define DESTROY_DYNAMIC_POOL(dynamic_pool, free_buffer)                                             \
    ((as_dynamic_pool*)dynamic_pool)->current_block_size = AS_DYNAMIC_POOL_BLOCK_SIZE_MIN;          \
    dynamic_pool_free_previous_blocks(dynamic_pool, free_buffer);                                   \
    dynamic_pool_free_current_block(dynamic_pool, free_buffer);                                     \
    cf_free(((as_dynamic_pool *)dynamic_pool)->pool);
