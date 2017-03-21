#include <stdint.h>
#include <stdlib.h>

typedef struct free_chunk_t
{
    struct free_chunk_t *next;
} free_chunk_t;

typedef struct mempool_buf_t {
    int object_size;

    int used_objects;
    int buf_size_objects;
    uint8_t *buf;

    struct mempool_buf_t *prev;
    struct mempool_buf_t *next;
} mempool_buf_t;

struct mempool_t {
    mempool_buf_t *last;
    free_chunk_t *free;
};

#include "mempool.h"


mempool_buf_t *mempool_create_buf(int object_size, int initial_size,
                                                   mempool_buf_t *prev)
{

    void *buf = malloc(object_size * initial_size);
    if (buf == NULL)
        return 0;
    if (object_size < sizeof(free_chunk_t)) {
        free(buf);
        return 0;
    }

    mempool_buf_t *res = malloc(sizeof(mempool_buf_t));
    res->object_size = object_size;
    res->used_objects = 0;
    res->buf_size_objects = initial_size;
    res->buf = buf;
    res->prev = prev;
    res->next = NULL;

    return res;
}

mempool_t *mempool_create(int object_size, int initial_size)
{
    mempool_buf_t *b = mempool_create_buf(object_size, initial_size, NULL);
    if (b != NULL) {
        mempool_t *res = malloc(sizeof(mempool_t));
        res->last = b;
        res->free = NULL;
        return res;
    } else {
        return NULL;
    }
}

void *mempool_alloc(mempool_t *pool)
{
    if (pool->free) {
        void *res = pool->free;
        pool->free = pool->free->next;
        return res;
    }

    if (pool->last->used_objects == pool->last->buf_size_objects) {
        /* last buf in the chain is full, allocate new one */
        mempool_buf_t *b = mempool_create_buf(pool->last->object_size,
                                              pool->last->buf_size_objects * 5 / 4,
                                              pool->last);
        if (b == NULL)
            return NULL;
        else {
            pool->last->next = b;
            pool->last = b;
        }
    }

    pool->last->used_objects++;
    return &pool->last->buf[(pool->last->used_objects - 1) * pool->last->object_size];
}

void mempool_free(mempool_t *pool, void *obj) {
    free_chunk_t *chunk = (free_chunk_t *)obj;
    chunk->next = pool->free;
    pool->free = chunk;
}

void mempool_destroy(mempool_t *pool) {
    while (pool->last) {
        mempool_buf_t *b = pool->last;
        pool->last = b->prev;
        free(b->buf);
        free(b);
    }
    free(pool);
}