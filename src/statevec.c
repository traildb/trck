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

sv_counter_t make_counter(int n, bool is_empty_state)
{
    if (is_empty_state)
        return (sv_counter_t)COUNTER_EMPTY_STATE_FLAG | (sv_counter_t)n;
    else
        return (sv_counter_t)n;
}

bool is_empty_state_counter(sv_counter_t c)
{
    return COUNTER_EMPTY_STATE_FLAG & c;
}

int counter_get_count(sv_counter_t c)
{
    return c & (sv_counter_t)MAX_COUNTER_VALUE;
}

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
state_t *sv_iterate_next(statevec_iterator_t *svi, bool* is_end)
{
    if (svi->sv == NULL) {
        *is_end = true;
        return NULL;
    }
    else {
        sv_counter_t *pcounter = (sv_counter_t *)svi->sv;

        if (*pcounter == 0) {
            *is_end = true;
            return NULL;
        }

        state_t *res = NULL;
        if (!is_empty_state_counter(*pcounter))
            res = (state_t *)(svi->sv + sizeof(sv_counter_t));

        svi->i++;

        if (svi->i >= counter_get_count(*pcounter)) {
            svi->i = 0;
            /* advance to the next item in RLE */
            if (!is_empty_state_counter(*pcounter))
                svi->sv += (sizeof(state_t) + sizeof(sv_counter_t));
            else
                svi->sv += sizeof(sv_counter_t);
        }

        *is_end = false;
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

        if (*pcounter == 0) {
            *num_states = -1;
            return NULL;
        }

        state_t *res = NULL;
        *num_states = counter_get_count(*pcounter) - svi->i;
        svi->i = 0;

        while (*num_states < INT_MAX - MAX_COUNTER_VALUE) {
            if (!is_empty_state_counter(*pcounter)) {
                res = (state_t *)(svi->sv + sizeof(sv_counter_t));
                svi->sv += (sizeof(state_t) + sizeof(sv_counter_t));
            } else {
                res = NULL;
                svi->sv += sizeof(sv_counter_t);
            }

            sv_counter_t *pnext_counter = (sv_counter_t *)svi->sv;

            if (*pnext_counter == 0)
                break;

            if (is_empty_state_counter(*pnext_counter) && (res == NULL)) {
                /* next state is empty too */
                *num_states += counter_get_count(*pnext_counter);
                pcounter = pnext_counter;
                continue;
            }

            if (!is_empty_state_counter(*pnext_counter) && (res != NULL)) {
                /* next state is the same as last state */
                state_t *pnext_state = (state_t *)(svi->sv + sizeof(sv_counter_t));
                if (match_same_state(pnext_state, res)) {
                    *num_states += counter_get_count(*pnext_counter);
                    pcounter = pnext_counter;
                    continue;
                }
            }

            break;
        }

        return res;
    }
}

void sv_dump(statevec_t *sv)
{
    statevec_iterator_t svi = {0};
    sv_iterate_start(sv, &svi);
    int n = 0;
    while(1) {
        bool is_end = false;
        state_t *st = sv_iterate_next(&svi, &is_end);
        if (is_end)
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

    svc->plast_counter = NULL;
    svc->plast_state = NULL;

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
    if (!match_is_initial_state(pstate)) {
        svc->size += ST_PLUS_C;
        CHECK(svc->size <= svc->max_size_bytes, "sv overflow");
        sv_counter_t *pcounter = (sv_counter_t *)&svc->buf[svc->size - ST_PLUS_C];
        state_t *plast_state = (state_t *)&svc->buf[svc->size - sizeof(state_t)];
        *pcounter = make_counter(1, false);
        memcpy(plast_state, pstate, sizeof(state_t));
        return pcounter;
    } else {
        svc->size += sizeof(sv_counter_t);
        sv_counter_t *pcounter = (sv_counter_t *)&svc->buf[svc->size - sizeof(sv_counter_t)];
        *pcounter = make_counter(1, true);
        return pcounter;
    }
}

void sv_append(statevec_constructor_t *svc, state_t *pstate, int num_states)
{
    sv_counter_t *pcounter = NULL;
    state_t *plast_state = svc->plast_state;

    if ((svc->size == 0)
            || ((plast_state == NULL) && !match_is_initial_state(pstate))
            || ((plast_state != NULL) && !match_same_state(plast_state, pstate))) {
        num_states -= 1;
        pcounter = sv_append_item_internal(svc, pstate);
    } else {
        pcounter = svc->plast_counter;
    }

    while (num_states) {
        int num_appended = MIN(MAX_COUNTER_VALUE - counter_get_count(*pcounter), num_states);
        *pcounter += num_appended;
        num_states -= num_appended;

        if (num_states) {
            pcounter = sv_append_item_internal(svc, pstate);
            num_states -= 1;
        }
    }

    svc->plast_counter = pcounter;
    if (is_empty_state_counter(*pcounter))
        svc->plast_state = NULL;
    else
        svc->plast_state = (state_t *)&svc->buf[svc->size - sizeof(state_t)];
}

statevec_t *sv_finish(statevec_constructor_t *svc, uint64_t *out_size_bytes)
{
    /* if all we have is initial states, return NULL */
    sv_counter_t *pfirst_counter = (sv_counter_t *)&svc->buf;

    if (svc->size == 0)
        return NULL;

    if ((is_empty_state_counter(*pfirst_counter)) &&
        (svc->size == sizeof(sv_counter_t)))
    {
        return NULL;
    }


    int total_bytes = -1;
    if (svc->plast_state == NULL) {
        total_bytes = svc->size - sizeof(sv_counter_t);
    } else {
        total_bytes = svc->size;
    }

    if (out_size_bytes)
        *out_size_bytes = total_bytes + sizeof(sv_counter_t);

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

    statevec_t *sv = sv_finish(&svc, NULL);
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

    statevec_t *sv = sv_finish(&svc, NULL);
    CHECK(sv != NULL, "sv!=NULL");

    statevec_iterator_t svi;
    sv_iterate_start(sv, &svi);
    int n = 0;
    while(1) {
        bool is_end = false;
        state_t *s = sv_iterate_next(&svi, &is_end);
        if (is_end)
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

    statevec_t *sv = sv_finish(&svc, NULL);
    CHECK(sv != NULL, "sv!=NULL");

    statevec_iterator_t svi;
    sv_iterate_start(sv, &svi);
    int n = 0;
    while(1) {
        bool is_end = false;
        state_t *s = sv_iterate_next(&svi, &is_end);
        if (is_end)
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
