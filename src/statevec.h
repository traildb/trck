#pragma once

#include "fns_generated.h"
#include "out_traildb.h"

/*
 * A compressed vector of matcher states (state_t structures).
 *
 *  Creating a new vector is done by declaring a NULL pointer:
 *      statevec_t *vec = NULL
 *
 *  There is no way to get vector size, since in all use cases we have so far
 *  it is known in advance.
 */

typedef uint16_t sv_counter_t;
#define MAX_COUNTER_VALUE 65535

typedef uint8_t statevec_t;

typedef struct statevec_iterator_t {
    uint8_t *sv;
    int i;
} statevec_iterator_t;

/************************ iterating over vectors *****************************/

/* Initialize iterator over a vector. */
void sv_iterate_start(statevec_t *sv, statevec_iterator_t *svi);

/*
 * Advance to the next state using the iterator. May return NULL if next state
 * is empty (i.e. equal to initial state).
 */
state_t *sv_iterate_next(statevec_iterator_t *svi);

/*
 * Advance to the next state, skipping duplicates. num_state will contain the
 * number of duplicate states.
 */
state_t *sv_iterate_next_edge(statevec_iterator_t *svi, int *num_states);

/* Free vector. */
void sv_free(statevec_t *sv);

typedef struct statevec_constructor_t {
    uint8_t *buf;
    int size;
    int max_size_bytes;
} statevec_constructor_t;

/************************ constructing vectors *******************************/

/*
 * Create a vector constructor object.
 * Structure must be zero-initialized when calling this for the first time. Can
 * be called multiple times on the same constructor, it will reset it and reuse
 * already allocated buffers.
 */
void sv_create(statevec_constructor_t *svc, int max_items);

/* Free constructor internal buffers. */
void sv_free_constructor(statevec_constructor_t *svc);

/* Append multiple duplicate states */
void sv_append(statevec_constructor_t *svc, state_t *pstate, int num_states);

/* Finish constructing a vector. */
statevec_t *sv_finish(statevec_constructor_t *svc);

void sv_dump(statevec_t *sv);