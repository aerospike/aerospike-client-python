/*
 *******************************************************************************************************
 * Dynamic pool maintained to avoid excessive runtime mallocs.
 * It is comprised of a Pool of pointers to pools of
 * as_bytes containing AS_DYNAMIC_POOL_BLOCK_SIZE as_bytes.
 *******************************************************************************************************
 */
#define AS_DYNAMIC_POOL_BLOCK_SIZE 1024

typedef struct bytes_dynamic_pool {
    as_bytes** pools;
    uint32_t current_bytes_id;
    uint32_t pool_id;
} as_dynamic_pool;

#define DYNAMIC_POOL_INIT(dynamic_pool, err)                                                                                                            \
    BYTES_POOLS(dynamic_pool) = (as_bytes **) malloc(sizeof(as_bytes *));                                                                               \
    if(BYTES_POOLS(dynamic_pool) == NULL){                                                                                                              \
        as_error_update(err, AEROSPIKE_ERR, "Failed to allocated memory for dynamic_pool");                                                             \
    }                                                                                                                                                   \
    BYTES_POOLS(dynamic_pool)[0] = (as_bytes *) malloc(sizeof(as_bytes) * AS_DYNAMIC_POOL_BLOCK_SIZE);                                                  \
    if(BYTES_POOLS(dynamic_pool)[0] == NULL){                                                                                                           \
        as_error_update(err, AEROSPIKE_ERR, "Failed to allocated memory for dynamic_pool");                                                             \
    }                                                                                                                                                   \
    BYTES_POOL_CNT(dynamic_pool) = 0;                                                                                                                   \
    BYTES_CNT(dynamic_pool) = 0;

#define DYNAMIC_POOL_RESIZE(dynamic_pool, err)                                                                                                          \
    BYTES_CNT(dynamic_pool) = 0;                                                                                                                        \
    BYTES_POOL_CNT(dynamic_pool)++;                                                                                                                     \
    BYTES_POOLS(dynamic_pool) = (as_bytes **) realloc(BYTES_POOLS(dynamic_pool), (BYTES_POOL_CNT(dynamic_pool) + 1) * sizeof(as_bytes *));              \
    if(BYTES_POOLS(dynamic_pool) == NULL){                                                                                                              \
        as_error_update(err, AEROSPIKE_ERR, "Failed to allocated memory for dynamic_pool");                                                             \
    }                                                                                                                                                   \
    BYTES_POOLS(dynamic_pool)[BYTES_POOL_CNT(dynamic_pool)] = (as_bytes *)malloc(sizeof(as_bytes) * AS_DYNAMIC_POOL_BLOCK_SIZE);                        \
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

#define GET_BYTES_POOL(map_bytes, dynamic_pool, err)                                                                                                    \
    if(BYTES_POOLS(dynamic_pool) == NULL){                                                                                                              \
        DYNAMIC_POOL_INIT(dynamic_pool, err)                                                                                                            \
    }                                                                                                                                                   \
    if (AS_DYNAMIC_POOL_BLOCK_SIZE > BYTES_CNT(dynamic_pool)) {                                                                                         \
        map_bytes = &(BYTES_POOL(dynamic_pool)[BYTES_CNT(dynamic_pool)++]);                                                                             \
    }                                                                                                                                                   \
    else {                                                                                                                                              \
        DYNAMIC_POOL_RESIZE(dynamic_pool, err);                                                                                                         \
        map_bytes = &(BYTES_POOL(dynamic_pool)[BYTES_CNT(dynamic_pool)++]);                                                                             \
    }

#define POOL_DESTROY(dynamic_pool, freeBytes)                                                                                                           \
    for (uint32_t pool_num = 0; pool_num < BYTES_POOL_CNT(dynamic_pool); pool_num++){                                                                   \
        if(freeBytes){                                                                                                                                  \
            for(uint32_t i = 0; i < AS_DYNAMIC_POOL_BLOCK_SIZE; i++ ){                                                                                  \
                as_bytes_destroy(&BYTES_POOL(dynamic_pool)[i]);                                                                                         \
            }                                                                                                                                           \
        }                                                                                                                                               \
        cf_free(((as_dynamic_pool *)dynamic_pool)->pools[pool_num]);                                                                                    \
    }                                                                                                                                                   \
    if(freeBytes){                                                                                                                                      \
        for(uint32_t i = 0; i < BYTES_CNT(dynamic_pool); i++ ){                                                                                         \
            as_bytes_destroy(&BYTES_POOL(dynamic_pool)[i]);                                                                                             \
        }                                                                                                                                               \
    }                                                                                                                                                   \
    cf_free(((as_dynamic_pool *)dynamic_pool)->pools[BYTES_POOL_CNT(dynamic_pool)]);                                                                    \
    cf_free(((as_dynamic_pool *)dynamic_pool)->pools);
