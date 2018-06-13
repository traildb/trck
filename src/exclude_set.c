#define _DEFAULT_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <traildb.h>

#include "safeio.h"
#include "judy_128_map.h"
#include "exclude_set.h"


typedef struct exclude_set_t {
    struct judy_128_map uuids;
} exclude_set_t;

void free_exclude_set(exclude_set_t *s) {
    if (s) {
        j128m_free(&s->uuids);
    }
    free(s);
}

exclude_set_t *parse_exclude_set(const char *path) {
    char buf[250] = {0};

    exclude_set_t tmp_res = {0};

    FILE *f = fopen(path, "rb");
    CHECK(f, "Cannot open %s", path);

    j128m_init(&tmp_res.uuids);

    uint64_t lineno = 1;
    while (fgets(buf, sizeof(buf), f)) {
        char *pbuf = buf;

        int nt = 0;

        __uint128_t cookie = 0;

        CHECK(0 == tdb_uuid_raw((uint8_t *)pbuf, (uint8_t *)&cookie),
              "invalid format on line %" PRIu64 " in exclude file %s (should be uuid)",
              lineno, path);

        if (cookie) {
            *j128m_insert(&tmp_res.uuids, cookie) = 1;
        }

        lineno++;
    }

    CHECK(lineno-1 == j128m_num_keys(&tmp_res.uuids),
          "duplicate entries in exclude file %s (%" PRIu64 " lines containing %" PRIu64 " uuids)",
          path, lineno-1, j128m_num_keys(&tmp_res.uuids));


    fprintf(stderr, "read %" PRIu64 " uuids from %s\n", j128m_num_keys(&tmp_res.uuids), path);

    exclude_set_t *res = calloc(sizeof(exclude_set_t), 1);
    memcpy(res, &tmp_res, sizeof(exclude_set_t));
    return res;
}

int exclude_set_contains(exclude_set_t *set, const uint8_t *uuid) {
    __uint128_t idx = *(__uint128_t *)uuid;
    Word_t *r = j128m_get(&set->uuids, idx);
    return r != NULL ? *r == 1 : 0;
}

void dump_exclude_set(exclude_set_t *res) {
    __uint128_t idx = 0;
    PWord_t pv = NULL;
    j128m_find(&res->uuids, &pv, &idx);
    while (pv != NULL) {
        char buf[33] = {0};
        tdb_uuid_hex((uint8_t *)&idx, (uint8_t *)buf);
        fprintf(stderr, "%s\n", buf);
        j128m_next(&res->uuids, &pv, &idx);
    }
}
