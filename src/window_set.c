#define _DEFAULT_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <traildb.h>

#include "safeio.h"
#include "judy_128_map.h"
#include "window_set.h"

typedef struct window_set_t {
    struct judy_128_map start_ts;
    struct judy_128_map end_ts;
    struct judy_128_map id_to_cookie_map_hi;
    struct judy_128_map id_to_cookie_map_lo;
} window_set_t;

void free_window_set(window_set_t *s) {
    if (s) {
        j128m_free(&s->start_ts);
        j128m_free(&s->end_ts);
        j128m_free(&s->id_to_cookie_map_hi);
        j128m_free(&s->id_to_cookie_map_lo);
    }
    free(s);
}


window_set_t *parse_window_set(const char *path) {
    char buf[250] = {0};

    window_set_t tmp_res = {0};

    FILE *f = fopen(path, "rb");
    CHECK(f, "Cannot open %s", path);

    j128m_init(&tmp_res.start_ts);
    j128m_init(&tmp_res.end_ts);
    j128m_init(&tmp_res.id_to_cookie_map_hi);
    j128m_init(&tmp_res.id_to_cookie_map_lo);

    uint64_t lineno = 1;
    while (fgets(buf, sizeof(buf), f)) {
        char *pbuf = buf;
        char *token = 0;

        int nt = 0;

        __uint128_t cookie = 0;
        __uint128_t id = 0;

        uint64_t window_start = 0;
        uint64_t window_end = 0;

        char *endptr;

        while ((token = strsep(&pbuf, ",")) != NULL) {
            switch (nt) {
                case 0:
                    CHECK(0 == tdb_uuid_raw((uint8_t *)token, (uint8_t *)&cookie),
                          "invalid format on line %" PRIu64 " in window file %s (should be cookie,timestamp1,timestamp2[,id])",
                          lineno, path);
                    break;
                case 1:
                    window_start = strtol(token, &endptr, 10);
                    CHECK(*endptr == '\0',
                          "invalid start timestamp format on line %" PRIu64 " in window file %s (should be cookie,timestamp1,timestamp2[,id])",
                          lineno, path);
                    break;
                case 2:
                    window_end = strtol(token, &endptr, 10);
                    CHECK(*endptr == '\r' || *endptr == '\n' || *endptr == '\0',
                          "invalid end timestamp format on line %" PRIu64 " in window file %s (should be cookie,timestamp1,timestamp2[,id])",
                          lineno, path);
                    break;
                case 3:
                    CHECK(0 == tdb_uuid_raw((uint8_t *)token, (uint8_t *)&id),
                          "invalid format on line %" PRIu64 " in window file %s (should be cookie,timestamp1,timestamp2[,id])",
                          lineno, path);
            }
            nt++;
        }

        CHECK(nt == 3 || nt == 4, "incorrect number of fields on line %" PRIu64 " in window file %s (should be cookie,timestamp1,timestamp2[,id])", lineno, path);

        if (id) {
            *j128m_insert(&tmp_res.id_to_cookie_map_hi, id) = (uint64_t)((cookie >> 64) & UINT64_MAX);
            *j128m_insert(&tmp_res.id_to_cookie_map_lo, id) = (uint64_t)(cookie & UINT64_MAX);
            *j128m_insert(&tmp_res.start_ts, id) = window_start;
            *j128m_insert(&tmp_res.end_ts, id) = window_end;
        } else if (cookie) {
            *j128m_insert(&tmp_res.start_ts, cookie) = window_start;
            *j128m_insert(&tmp_res.end_ts, cookie) = window_end;
        }

        lineno++;
    }

    CHECK(lineno-1 == j128m_num_keys(&tmp_res.start_ts),
        "duplicate entries in window file %s (%" PRIu64 " lines containing %" PRIu64 " cookies)",
        path, lineno-1, j128m_num_keys(&tmp_res.start_ts));


    fprintf(stderr, "read %" PRIu64 " cookie filters from %s\n", j128m_num_keys(&tmp_res.start_ts), path);

    window_set_t *res = calloc(sizeof(window_set_t), 1);
    memcpy(res, &tmp_res, sizeof(window_set_t));
    return res;
}

int window_set_get(window_set_t *set, const uint8_t *cookie, uint64_t *start_ts, uint64_t *end_ts) {
    __uint128_t idx = *(__uint128_t *)cookie;

    Word_t *pstart = j128m_get(&set->start_ts, idx);
    if (pstart)
        *start_ts = *pstart;
    else
        *start_ts = 0;

    Word_t *pend = j128m_get(&set->end_ts, idx);
    if (pend)
        *end_ts = *pend;
    else
        *end_ts = 0;
    return (pstart && pend);
}

void dump_window_set(window_set_t *res) {
    __uint128_t idx = 0;
    PWord_t pv = NULL;
    j128m_find(&res->start_ts, &pv, &idx);
    while (pv != NULL)
    {
        uint64_t window_start = *pv;
        uint64_t window_end = 0;
        Word_t *e = j128m_get(&res->end_ts, idx);
        if (e){
            window_end = *e;
        }

        char buf[33] = {0};
        tdb_uuid_hex((uint8_t *)&idx, (uint8_t *)buf);
        fprintf(stderr, "%s,%llu,%llu\n", buf, window_start, window_end);
        j128m_next(&res->start_ts, &pv, &idx);
    }
}

__uint128_t *window_set_get_cookies(window_set_t *set, uint64_t *num_cookies) {
    *num_cookies = j128m_num_keys(&set->start_ts);

    __uint128_t *res = malloc(sizeof(__uint128_t) * (*num_cookies));

    __uint128_t idx = 0;
    uint64_t i = 0;

    PWord_t pv = NULL;
    j128m_find(&set->start_ts, &pv, &idx);
    while (pv != NULL)
    {
        res[i] = idx;
        i++;
        j128m_next(&set->start_ts, &pv, &idx);
    }

    return res;
}

void window_set_id_to_cookie(window_set_t *set, const uint8_t *id, __uint128_t *out_cookie) {
    Word_t *result_hi = j128m_get(&set->id_to_cookie_map_hi, *(__uint128_t *)id);
    Word_t *result_lo = j128m_get(&set->id_to_cookie_map_lo, *(__uint128_t *)id);
    if (!result_hi && !result_lo) {
        *out_cookie = *(__uint128_t *)id;
        return;
    }

    ((Word_t *)out_cookie)[1] = *result_hi;
    ((Word_t *)out_cookie)[0] = *result_lo;
}

int test_main(int argc, char **argv) {
    window_set_t *s = parse_window_set(argv[1]);
    dump_window_set(s);
    free_window_set(s);
    return 0;
}
