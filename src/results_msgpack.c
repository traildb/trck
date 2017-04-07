#include <stdbool.h>
#include <Judy.h>
#include <msgpack.h>
#include <msgpack/fbuffer.h>
#include <traildb.h>

#include "fns_generated.h"
#include "foreach_util.h"
#include "results_msgpack.h"
#include "safeio.h"
#include "utils.h"

void msgpack_add_int(void *p, char *name, int64_t value) {
    msgpack_packer *pk = (msgpack_packer *)p;

    msgpack_pack_str(pk, strlen(name));
    msgpack_pack_str_body(pk, name, strlen(name));

    msgpack_pack_map(pk, 2);

    msgpack_pack_str(pk, 4);
    msgpack_pack_str_body(pk, "type", 4);
    msgpack_pack_str(pk, 3);
    msgpack_pack_str_body(pk, "int", 3);

    msgpack_pack_str(pk, 5);
    msgpack_pack_str_body(pk, "value", 5);
    msgpack_pack_int64(pk, value);
}

/*
 * Given a set of tuples, count number of distinct first items.
 */
uint64_t get_num_heads(set_t *s) {
    uint64_t res = 0;

    uint8_t index[10000];
    index[0] = '\0';

    uint8_t head[1000];
    int head_size = 0;

    uint8_t prev_head[10000];
    prev_head[0] = '\0';
    int prev_head_size = 0;

    Word_t * pv;
    JSLF(pv, *s, index);

    int result_type = 0;

    while (pv) {
        string_tuple_extract_head((char *)index, sizeof(head), head, &head_size, &result_type);

        if ((head_size != prev_head_size) || (memcmp(head, prev_head, head_size) != 0)) {
            memcpy(prev_head, head, head_size);
            prev_head_size = head_size;
            res += 1;
        }

        JSLN(pv, *s, index);
    }

    return res;
}

void output_set(msgpack_packer *pk, set_t *value, int multiset) {
    uint8_t index[10000];
    index[0] = '\0';

    uint8_t head[10000];
    int head_size = 0;

    uint8_t prev_head[10000];
    prev_head[0] = '\0';
    int prev_head_size = 0;

    Word_t * pv;
    JSLF(pv, *value, index);

    Pvoid_t lexicon = NULL;
    int lexicon_size = 0;

    /* buffer to store indexed tail ids */
    int buf_size = 0;
    int max_buf_size = 1024;
    int64_t *buf = malloc(max_buf_size * sizeof(int64_t));

    int64_t num_heads = get_num_heads(value);

    msgpack_pack_str(pk, 4);
    msgpack_pack_str_body(pk, "data", 4);

    msgpack_pack_map(pk, num_heads);

    int idx = 0;
    while (pv) {
        /* 1. Break tuple into head and tail.
         *
         * 2. If head is different from previous iteration, output head,
         * buffer and reset the buffer.
         *
         * 3. Get lexicon id for the tail, add it to the buffer.
         */
        int result_type = 0;
        char *tail = string_tuple_extract_head((char *)index, sizeof(head), head, &head_size, &result_type);

        if (idx == 0) {
            /* Initialize prev_head on the very first iteration */
            memcpy(prev_head, head, head_size);
            prev_head_size = head_size;
        } else if ((head_size != prev_head_size) || (memcmp(head, prev_head, head_size) != 0)) {
            /* Output head, buffer */
            msgpack_pack_str(pk, prev_head_size);
            msgpack_pack_str_body(pk, prev_head, prev_head_size);
            msgpack_pack_array(pk, buf_size);
            for (int i = 0; i < buf_size; i++)
                msgpack_pack_int64(pk, buf[i]);

            /* Reset buffer size */
            buf_size = 0;

            /* Update prev_head */
            memcpy(prev_head, head, head_size);
            prev_head_size = head_size;
        }


        /* Try to get id for the tail. if missing, generate a new id */
        Word_t *pId;
        JSLI(pId, lexicon, (uint8_t *)tail);

        if (*pId == 0) {
            lexicon_size += 1;
            *pId = lexicon_size;
        }

        /* Append id to the buffer */
        buf[buf_size] = (int64_t)*pId;

        buf_size += 1;

        /* Append value to the buffer too if serializing multiset */
        if (multiset) {
            buf[buf_size] = (int64_t)*pv;
            buf_size += 1;
        }

        /* Resize buffer if necessary */
        if (buf_size >= max_buf_size - 2) {
            max_buf_size = max_buf_size * 3 / 2;
            buf = realloc(buf, sizeof(int64_t) * max_buf_size);
        }

        JSLN(pv, *value, index);
        idx++;
    }

    /* If there were any items at all.. */
    if (idx) {
        /* Output last buffer */
        msgpack_pack_str(pk, prev_head_size);
        msgpack_pack_str_body(pk, prev_head, prev_head_size);
        msgpack_pack_array(pk, buf_size);
        for (int i = 0; i < buf_size; i++)
            msgpack_pack_int64(pk, buf[i]);
    }

    /* Output lexicon */
    msgpack_pack_str(pk, 7);
    msgpack_pack_str_body(pk, "lexicon", 7);
    msgpack_pack_map(pk, lexicon_size);

    char lex_item[10000] = "";

    JSLF(pv, lexicon, (uint8_t *)lex_item);

    while (pv) {
        if (!string_tuple_is_empty(lex_item)) {
            /*
             * First char of the encoded tuple is a type id, skip it.
             *
             * This part is kinda lazy, basically we store an escaped string
             * in the lexicon here, but the assumption is that msgpack format
             * is used primarily for feature extraction and these strings are
             * only used for debugging.
             *
             * In other words, if you use anything except yields of the form
             *
             *   `yield uuid,<string> to <whatever>`
             *
             * You can get some weird looking string in the lexicon in msgpack.
             */
            msgpack_pack_str(pk, strlen(lex_item) - 1);
            msgpack_pack_str_body(pk, lex_item + 1, strlen(lex_item) - 1);
            msgpack_pack_int(pk, *pv);
        } else {
            msgpack_pack_nil(pk);
            msgpack_pack_int(pk, *pv);
        }
        JSLN(pv, lexicon, (uint8_t *)lex_item);
    }

    int rc;
    JSLFA(rc, lexicon);
    free(buf);
}

void msgpack_add_set(void *p, char *name, set_t *value) {
    msgpack_packer *pk = (msgpack_packer *)p;

    msgpack_pack_str(pk, strlen(name));
    msgpack_pack_str_body(pk, name, strlen(name));

    msgpack_pack_map(pk, 3); /* type, data, lexicon */

    msgpack_pack_str(pk, 4);
    msgpack_pack_str_body(pk, "type", 4);
    msgpack_pack_str(pk, 3);
    msgpack_pack_str_body(pk, "set", 3);

    output_set(pk, value, 0);
}

void msgpack_add_hll(void *p, char *name, hyperloglog_t *hll) {
    // don't support hlls in msgpack for now
    return;

}


void msgpack_add_multiset(void *p, char *name, set_t *value) {
    msgpack_packer *pk = (msgpack_packer *)p;

    msgpack_pack_str(pk, strlen(name));
    msgpack_pack_str_body(pk, name, strlen(name));

    msgpack_pack_map(pk, 3); /* type, data, lexicon */

    msgpack_pack_str(pk, 4);
    msgpack_pack_str_body(pk, "type", 4);
    msgpack_pack_str(pk, 8);
    msgpack_pack_str_body(pk, "multiset", 8);

    output_set(pk, value, 1);
}

void count_set(void *p, char *name, set_t *value) {
    *((int64_t *)p) += 1;
}

void count_int(void *p, char *name, int64_t value) {
    *((int64_t *)p) += 1;
}

void output_groupby_vars_msgpack(msgpack_packer* pk, groupby_info_t *gi, int i) {
    msgpack_pack_map(pk, gi->num_vars);

    for (int j = 0; j < gi->num_vars; j++) {
        string_val_t *tuple = &gi->tuples[i * gi->num_vars];
        if (gi->var_names[j][0] == '%') {
            msgpack_pack_str(pk, strlen(gi->var_names[j]));
            msgpack_pack_str_body(pk, gi->var_names[j], strlen(gi->var_names[j]));
            msgpack_pack_str(pk, tuple[j].len);
            msgpack_pack_str_body(pk, tuple[j].str, tuple[j].len);
        } else if (gi->var_names[j][0] == '#') {
            msgpack_pack_str(pk, strlen(gi->var_names[j]));
            msgpack_pack_str_body(pk, gi->var_names[j], strlen(gi->var_names[j]));

            msgpack_pack_array(pk, tuple[j].len);
            for (int k = 0; k < tuple[j].len; k++) {
                string_val_t v = tuple[j].str_set[k];
                msgpack_pack_str(pk, v.len);
                msgpack_pack_str_body(pk, v.str, v.len);
            }

        } else {
            CHECK(false, "not supposed to reach this while printing tuple\n");
        }
    }
}

void output_msgpack(groupby_info_t *gi, results_t *results)
{
    fprintf(stderr, "Generating msgpack output\n");
    if (gi == NULL || gi->num_vars == 0 || gi->merge_results) {
        int64_t num_items = 0; /* Count number of items to save */
        match_save_result(results, &num_items, count_int, count_set, count_set, msgpack_add_hll);


        /*
         * Store result as a map.
         */
        msgpack_packer* pk = msgpack_packer_new(stdout, msgpack_fbuffer_write);
        msgpack_pack_map(pk, num_items);
        match_save_result(results, pk, msgpack_add_int, msgpack_add_set, msgpack_add_multiset, msgpack_add_hll);
    } else {

        msgpack_packer* pk = msgpack_packer_new(stdout, msgpack_fbuffer_write);

        /*
         * If you have foreach clause and not use merged results mode, we
         * produce an array of msgpack maps instead of a single map.
         */
        msgpack_pack_array(pk, gi->num_tuples);

        for (int i = 0; i < gi->num_tuples; i++) {
            results_t *pres = (results_t *)((uint8_t *)results + match_get_result_size() * i);

            msgpack_pack_map(pk, 2);

            /*
             * Store result object. That's the one containing "yield" variables.
             * Same format as you get in the non-foreach case, except in that
             * case it was the only thing you'd get.
             */
            msgpack_pack_str(pk, strlen("result"));
            msgpack_pack_str_body(pk, "result", strlen("result"));

            int64_t num_items = 0; /* Count number of items to save */
            match_save_result(pres, &num_items, count_int, count_set, count_set, msgpack_add_hll);

            msgpack_pack_map(pk, num_items);
            match_save_result(pres, pk, msgpack_add_int, msgpack_add_set, msgpack_add_multiset, msgpack_add_hll);

            /*
             * Store foreach variable values that correspond to this result.
             */
            msgpack_pack_str(pk, strlen("vars"));
            msgpack_pack_str_body(pk, "vars", strlen("vars"));

            output_groupby_vars_msgpack(pk, gi, i);
        }
    }
}
