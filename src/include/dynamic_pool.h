/*
 *******************************************************************************************************
 * Dynamic pool maintained to avoid excessive runtime mallocs.
 * It is comprised of a Pool of pointers to pools of
 * as_bytes containing AS_DYNAMIC_POOL_BLOCK_SIZE as_bytes.
 *******************************************************************************************************
 */
#define AS_DYNAMIC_POOL_BLOCK_SIZE 128
#define AS_DYNAMIC_POOL_BLOCK_SIZE_MAX 32768
#define AS_DYNAMIC_POOL_POINTER_BLOCK_SIZE 4


typedef struct bytes_dynamic_pool {
    as_bytes** pools;
    uint16_t current_bytes_id;
    uint16_t pool_id;
    uint16_t current_block_size;
} as_dynamic_pool;

#define DYNAMIC_POOL_INIT(dynamic_pool, err)                                                                                                            \
    BYTES_POOLS(dynamic_pool) = (as_bytes **) malloc(AS_DYNAMIC_POOL_POINTER_BLOCK_SIZE * sizeof(as_bytes *));                                          \
    if(BYTES_POOLS(dynamic_pool) == NULL){                                                                                                              \
        as_error_update(err, AEROSPIKE_ERR, "Failed to allocated memory for dynamic_pool");                                                             \
    }                                                                                                                                                   \
    BYTES_POOLS(dynamic_pool)[0] = (as_bytes *) malloc(sizeof(as_bytes) * AS_DYNAMIC_POOL_BLOCK_SIZE);                                                  \
    if(BYTES_POOLS(dynamic_pool)[0] == NULL){                                                                                                           \
        as_error_update(err, AEROSPIKE_ERR, "Failed to allocated memory for dynamic_pool");                                                             \
    }                                                                                                                                                   \
    BYTES_POOL_CNT(dynamic_pool) = 0;                                                                                                                   \
    BYTES_BLOCK_SIZE(dynamic_pool) = AS_DYNAMIC_POOL_BLOCK_SIZE;                                                                                        \
    BYTES_CNT(dynamic_pool) = 0;

#define DYNAMIC_POOL_RESIZE(dynamic_pool, err)                                                                                                          \
    BYTES_CNT(dynamic_pool) = 0;                                                                                                                        \
    BYTES_POOL_CNT(dynamic_pool)++;                                                                                                                     \
    if(BYTES_BLOCK_SIZE(dynamic_pool) != AS_DYNAMIC_POOL_BLOCK_SIZE_MAX){                                                                               \
        BYTES_BLOCK_SIZE(dynamic_pool) = BYTES_BLOCK_SIZE(dynamic_pool) << 1;                                                                           \
    }                                                                                                                                                   \
    if(BYTES_POOL_CNT(dynamic_pool) % AS_DYNAMIC_POOL_POINTER_BLOCK_SIZE == 0){                                                                         \
        BYTES_POOLS(dynamic_pool) = (as_bytes **) realloc(BYTES_POOLS(dynamic_pool), (BYTES_POOL_CNT(dynamic_pool) + AS_DYNAMIC_POOL_POINTER_BLOCK_SIZE) * sizeof(as_bytes *));              \
    }                                                                                                                                                   \
    if(BYTES_POOLS(dynamic_pool) == NULL){                                                                                                              \
        as_error_update(err, AEROSPIKE_ERR, "Failed to allocated memory for dynamic_pool");                                                             \
    }                                                                                                                                                   \
    BYTES_POOLS(dynamic_pool)[BYTES_POOL_CNT(dynamic_pool)] = (as_bytes *)malloc(sizeof(as_bytes) * BYTES_BLOCK_SIZE(dynamic_pool));                    \
    if(BYTES_POOLS(dynamic_pool)[BYTES_POOL_CNT(dynamic_pool)] == NULL){                                                                                \
        as_error_update(err, AEROSPIKE_ERR, "Failed to allocated memory for dynamic_pool");                                                             \
    }

#define BYTES_CNT(dynamic_pool)                                                                                                                         \
    (((as_dynamic_pool *)dynamic_pool)->current_bytes_id)

#define BYTES_POOL_CNT(dynamic_pool)                                                                                                                    \
    (((as_dynamic_pool *)dynamic_pool)->pool_id)

#define BYTES_POOLS(dynamic_pool)                                                                                                                       \
    (((as_dynamic_pool *)dynamic_pool)->pools)

#define BYTES_POOL(dynamic_pool)                                                                                                                        \
    ((as_dynamic_pool *)dynamic_pool)->pools[BYTES_POOL_CNT(dynamic_pool)]

#define BYTES_BLOCK_SIZE(dynamic_pool)                                                                                                                  \
    (((as_dynamic_pool *)dynamic_pool)->current_block_size)

#define GET_BYTES_POOL(map_bytes, dynamic_pool, err)                                                                                                    \
    if(BYTES_POOLS(dynamic_pool) == NULL){                                                                                                              \
        DYNAMIC_POOL_INIT(dynamic_pool, err)                                                                                                            \
    }                                                                                                                                                   \
    if (BYTES_BLOCK_SIZE(dynamic_pool) > BYTES_CNT(dynamic_pool)) {                                                                                     \
        map_bytes = &(BYTES_POOL(dynamic_pool)[BYTES_CNT(dynamic_pool)++]);                                                                             \
    }                                                                                                                                                   \
    else {                                                                                                                                              \
        DYNAMIC_POOL_RESIZE(dynamic_pool, err);                                                                                                         \
        map_bytes = &(BYTES_POOL(dynamic_pool)[BYTES_CNT(dynamic_pool)++]);                                                                             \
    }

#define POOL_DESTROY(dynamic_pool, freeBytes)                                                                                                           \
    uint32_t delete_pool_size = AS_DYNAMIC_POOL_BLOCK_SIZE;                                                                                             \
    for (uint32_t pool_num = 0; pool_num < BYTES_POOL_CNT(dynamic_pool); pool_num++){                                                                   \
        if(freeBytes){                                                                                                                                  \
            for(uint32_t i = 0; i < delete_pool_size; i++ ){                                                                                            \
                as_bytes_destroy(&BYTES_POOL(dynamic_pool)[i]);                                                                                         \
            }                                                                                                                                           \
        }                                                                                                                                               \
        cf_free(((as_dynamic_pool *)dynamic_pool)->pools[pool_num]);                                                                                    \
        if(delete_pool_size != AS_DYNAMIC_POOL_BLOCK_SIZE_MAX){                                                                                         \
            delete_pool_size = delete_pool_size << 1;                                                                                                   \
        }                                                                                                                                               \
    }                                                                                                                                                   \
    if(freeBytes){                                                                                                                                      \
        for(uint32_t i = 0; i < BYTES_CNT(dynamic_pool); i++ ){                                                                                         \
            as_bytes_destroy(&BYTES_POOL(dynamic_pool)[i]);                                                                                             \
        }                                                                                                                                               \
    }                                                                                                                                                   \
    cf_free(((as_dynamic_pool *)dynamic_pool)->pools[BYTES_POOL_CNT(dynamic_pool)]);                                                                    \
    cf_free(((as_dynamic_pool *)dynamic_pool)->pools);
