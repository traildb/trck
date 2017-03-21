#pragma once

/*
 * Simple fixed-size object allocator.
 *
 * Internally uses a linked list of memory buffers with exponential sizes.
 */

typedef struct mempool_t mempool_t;

/*
 * Create a memory pool with fixed object size (in bytes).
 * Reserve memory for initial_size objects right away.

 * Object size has to be at least sizeof(void *).
 */
mempool_t *mempool_create(int object_size, int initial_size);

/* malloc */
void *mempool_alloc(mempool_t *pool);

/* free */
void mempool_free(mempool_t *pool, void *obj);

/* Destroy buffer, freeing all memory. */
void mempool_destroy(mempool_t *pool);