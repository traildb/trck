#include <stdbool.h>
#include <stdint.h>
#include <string.h>
#include <Judy.h>

#include "fns_generated.h"
#include "fns_imported.h"

#include "statevec.h"
#include "out_traildb.h"
#include "safeio.h"

#define MIN(a,b) (((a)<(b))?(a):(b))
#define MAX(a,b) (((a)>(b))?(a):(b))

/*
 * State vector is compressed using simple RLE scheme. RLE counter is 16 bits,
 * followed by a state_t structure.
 *
 * Couple extra shortcuts:
 *
 * 1. All empty states at the end of the vector are just truncated.
 *
 * 2. If a vector contains only empty states, we record that as
 * (statevec_t *)NULL, and no memory is allocated.
 */


/* Initialize iterator. */
void sv_iterate_start(statevec_t *sv, statevec_iterator_t *svi)
{
    svi->i = 0;
    svi->sv = sv;
}

/*
 * Advance to the next state using the iterator. May return NULL if next state
 * is empty (i.e. equal to the default state).
 */
state_t *sv_iterate_next(statevec_iterator_t *svi)
{
    if (svi->sv == NULL)
        return NULL;
    else {
        sv_counter_t *pcounter = (sv_counter_t *)svi->sv;

        if (*pcounter == 0)
            return NULL;

        state_t *res = (state_t *)(svi->sv + sizeof(sv_counter_t));
        svi->i++;

        if (svi->i >= *pcounter) {
            svi->i = 0;
            /* advance to the next item in RLE */
            svi->sv += (sizeof(state_t) + sizeof(sv_counter_t));
        }
        return res;
    }
}

state_t *sv_iterate_next_edge(statevec_iterator_t *svi, int *num_states)
{
    if (svi->sv == NULL) {
        *num_states = -1;
        return NULL;
    } else {
        sv_counter_t *pcounter = (sv_counter_t *)svi->sv;

        if (pcounter == NULL) {
            *num_states = -1;
            return NULL;
        }

        if (*pcounter == 0) {
            *num_states = -1;
            return NULL;
        }

        state_t *res = (state_t *)(svi->sv + sizeof(sv_counter_t));
        *num_states = *pcounter - svi->i;

        svi->i = 0;
        /* advance to the next item in RLE */
        svi->sv += (sizeof(state_t) + sizeof(sv_counter_t));
        return res;
    }
}

void sv_dump(statevec_t *sv)
{
    statevec_iterator_t svi = {0};
    sv_iterate_start(sv, &svi);
    int n = 0;
    while(1) {
        state_t *st = sv_iterate_next(&svi);
        if (st == NULL)
            break;

        if (!match_is_initial_state(st))
            fprintf(stderr, "%d(%d) ", n, st->ri);
        n++;
    }
    fprintf(stderr, "\n");
}

/* free entire vector */
void sv_free(statevec_t *sv) {
    free(sv);
}

/* svc must be initialized to 0 when calling this for the first time */
void sv_create(statevec_constructor_t *svc, int max_items)
{
    int max_size_bytes = max_items * (sizeof(state_t) + sizeof(sv_counter_t))
                         + sizeof(sv_counter_t);

    if (svc->max_size_bytes > 0) {
        CHECK(svc->max_size_bytes == max_size_bytes,
              "sv_create: bad max_size\n");
    } else {
        svc->max_size_bytes = max_size_bytes;
        svc->buf = malloc(max_size_bytes);
    }

    svc->size = 0;
}

void sv_free_constructor(statevec_constructor_t *svc)
{
    free(svc->buf);
}

#define ST_PLUS_C (sizeof(state_t) + sizeof(sv_counter_t))

sv_counter_t *sv_append_item_internal(statevec_constructor_t *svc,
                                      state_t *pstate)
{
    svc->size += sizeof(state_t) + sizeof(sv_counter_t);
    CHECK(svc->size <= svc->max_size_bytes, "sv overflow");

    sv_counter_t *pcounter = (sv_counter_t *)&svc->buf[svc->size - ST_PLUS_C];
    state_t *plast_state = (state_t *)&svc->buf[svc->size - sizeof(state_t)];
    *pcounter = 1;
    memcpy(plast_state, pstate, sizeof(state_t));
    return pcounter;
}

void sv_append(statevec_constructor_t *svc, state_t *pstate, int num_states)
{
    sv_counter_t *pcounter = NULL;
    state_t *plast_state = (state_t *)&svc->buf[svc->size - sizeof(state_t)];

    if (svc->size == 0 || (!match_same_state(plast_state, pstate))) {
        num_states -= 1;
        pcounter = sv_append_item_internal(svc, pstate);
    } else {
        pcounter = (sv_counter_t *)&svc->buf[svc->size - ST_PLUS_C];
    }

    while (num_states) {
        int num_appended = MIN(MAX_COUNTER_VALUE - *pcounter, num_states);
        *pcounter += num_appended;
        num_states -= num_appended;

        if (num_states) {
            pcounter = sv_append_item_internal(svc, pstate);
            num_states -= 1;
        }
    }
}

statevec_t *sv_finish(statevec_constructor_t *svc)
{
    /* if all we have is initial states, return NULL */
    state_t *pfirst_state = (state_t *)&svc->buf[sizeof(sv_counter_t)];

    if (svc->size == 0)
        return NULL;

    if ((match_is_initial_state(pfirst_state)) &&
        (svc->size == sizeof(state_t) + sizeof(sv_counter_t)))
    {
        return NULL;
    }

    /* if last state is initial, don't copy it */
    state_t *plast_state = (state_t *)&svc->buf[svc->size - sizeof(state_t)];

    int total_bytes = -1;
    if (match_is_initial_state(plast_state)) {
        total_bytes = svc->size - sizeof(state_t) - sizeof(sv_counter_t);
    } else {
        total_bytes = svc->size;
    }

    statevec_t *res = malloc(total_bytes + sizeof(sv_counter_t));
    memcpy(res, svc->buf, total_bytes);
    sv_counter_t *terminator = (sv_counter_t*)&res[total_bytes];
    *terminator = 0;
    return res;
}

void test1() {
    state_t states[4];
    fprintf(stderr, "test1\n");

    for (int i = 0; i < sizeof(states) / sizeof(state_t); i++)
        match_trail_init(&states[i]);

    statevec_constructor_t svc = {0};
    sv_create(&svc, 4);

    for (int i = 0; i < sizeof(states) / sizeof(state_t); i++)
        sv_append(&svc, &states[i], 1);

    statevec_t *sv = sv_finish(&svc);
    CHECK(sv == NULL, "sv == NULL");
}

void test2() {
    state_t states[4];
    fprintf(stderr, "test2\n");

    for (int i = 0; i < sizeof(states) / sizeof(state_t); i++) {
        match_trail_init(&states[i]);
        states[i].ri = i;
    }

    statevec_constructor_t svc = {0};
    sv_create(&svc, 4);

    for (int i = 0; i < sizeof(states) / sizeof(state_t); i++)
        sv_append(&svc, &states[i], 1);

    statevec_t *sv = sv_finish(&svc);
    CHECK(sv != NULL, "sv!=NULL");

    statevec_iterator_t svi;
    sv_iterate_start(sv, &svi);
    int n = 0;
    while(1) {
        state_t *s = sv_iterate_next(&svi);
        if (s == NULL)
            break;
        CHECK(s->ri == n, "s->ri == %d /= %d", s->ri, n);
        n++;
    }
    CHECK(n==4, "n=%d", n);
}

void test3() {
    state_t states[10];
    fprintf(stderr, "test3\n");

    for (int i = 0; i < sizeof(states) / sizeof(state_t); i++) {
        match_trail_init(&states[i]);
        states[i].ri = i;
    }

    states[4].ri = 0;
    states[5].ri = 0;
    states[6].ri = 0;

    statevec_constructor_t svc = {0};
    sv_create(&svc, sizeof(states)/sizeof(state_t));

    for (int i = 0; i < sizeof(states) / sizeof(state_t); i++)
        sv_append(&svc, &states[i], 1);

    statevec_t *sv = sv_finish(&svc);
    CHECK(sv != NULL, "sv!=NULL");

    statevec_iterator_t svi;
    sv_iterate_start(sv, &svi);
    int n = 0;
    while(1) {
        state_t *s = sv_iterate_next(&svi);
        if (s == NULL)
            break;
        CHECK(s->ri == states[n].ri, "s->ri == %d /= %d", s->ri, states[n].ri);
        n++;
    }
    CHECK(n==sizeof(states)/sizeof(state_t), "n=%d", n);


    sv_iterate_start(sv, &svi);
    int edges[] = {1,1,1,1,3,1,1,1};
    for (int i = 0; i < 8; i++) {
        int num_states;
        sv_iterate_next_edge(&svi, &num_states);
        CHECK(num_states == edges[i], "edges: %d\n", num_states);
        n++;
    }
}

void run_tests() {
    test1();
    test2();
    test3();
    fprintf(stderr, "tests done\n");
}
