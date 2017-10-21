#include <stdbool.h>
#include <Judy.h>
#include <json-c/json.h>
#include <traildb.h>
#include <string.h>

#include "fns_generated.h"
#include "foreach_util.h"
#include "results_json.h"


#include "results_json_internal.h"
#include "safeio.h"
#include "utils.h"


const unsigned char *utf8_check(const unsigned char *s);


void print_escaped(const char *str, int len)
{
    json_hex_chars = "0123456789abcdef";
    int64_t pos = 0, start_offset = 0;
    unsigned char c;
    while (len--)
    {
        c = str[pos];
        pos++;

        switch(c)
        {
        case '\b':
        case '\n':
        case '\r':
        case '\t':
        case '\f':
        case '"':
        case '\\':
        case '/':
            if(c == '\b') printf("\\b");
            else if(c == '\n') printf("\\n");
            else if(c == '\r') printf("\\r");
            else if(c == '\t') printf("\\t");
            else if(c == '\f') printf("\\f");
            else if(c == '"') printf("\\\"");
            else if(c == '\\') printf("\\\\");
            else if(c == '/') printf("\\/");
            break;
        default:
            if(c < ' ')
            {
                printf("\\u00%c%c",
                     json_hex_chars[c >> 4],
                     json_hex_chars[c & 0xf]);
            } else
                putc(c, stdout);
        }
    }
}


void print_json_string(char *str, int64_t len) {
    printf("\"");
    if (len == -1) {
        const unsigned char *c = utf8_check((unsigned char *)str);
        if (c == NULL)
            print_escaped(str, strlen(str));
        else {
            print_escaped(str, c - (const unsigned char *)str);
        }
    } else {
        print_escaped(str, len);
    }
    printf("\"");
}


void json_add_int(void *p, char *name, int64_t value) {
    int64_t *nitem = (int64_t *)p;

    if (*nitem)
        printf(",");
    (*nitem) += 1;
    print_json_string(name, -1);
    printf(":%lld", value);
}

void set_to_json(set_t *src)
{
    uint8_t index[10000];
    index[0] = '\0';

    Word_t * pv;
    JSLF(pv, *src, index);

    printf("[");

    int nitem = 0;
    while (pv) {
        if (nitem)
            printf(",");
        nitem++;

        char buf[1000];
        string_tuple_to_json((char*)index, buf);
        print_json_string((char *)buf, -1);
        JSLN(pv, *src, index);
    }

    printf("]");
}

void multiset_to_json(set_t *src)
{
    uint8_t index[10000];
    index[0] = '\0';

    Word_t * pv;
    JSLF(pv, *src, index);

    printf("{");
    int nitem = 0;

    while (pv) {
        if (nitem)
            printf(",");
        nitem++;

        char buf[1000];
        string_tuple_to_json((char*)index, buf);
        print_json_string(buf, -1);
        printf(":%ld", *pv);
        JSLN(pv, *src, index);
    }

    printf("}");
}

void json_add_set(void *p, char *name, set_t *value) {
    int64_t *nitem = (int64_t *)p;

    if (*nitem)
        printf(",");
    (*nitem) += 1;

    print_json_string(name, -1);
    printf(":");
    set_to_json(value);

}


void json_add_multiset(void *p, char *name, set_t *value) {
    int64_t *nitem = (int64_t *)p;

    if (*nitem)
        printf(",");
    (*nitem) += 1;

    print_json_string(name, -1);
    printf(":");
    multiset_to_json(value);
}

void json_add_hll(void *p, char *name, hyperloglog_t *hll) {
    int64_t *nitem = (int64_t *)p;
    if (*nitem)
        printf(",");
    (*nitem) += 1;

    print_json_string(name, -1);
    printf(":");
    char * hll_string = hll_to_string(hll);
    print_json_string(hll_string, -1);
    free(hll_string);
}

int match_results_to_json(results_t *results) {
    int64_t nitems = 0;
    match_save_result(results, &nitems, json_add_int, json_add_set, json_add_multiset, json_add_hll);
    return nitems;
}

void output_groupby_result_json(groupby_info_t *gi, int i, results_t *results)
{
    CHECK(i < gi->num_tuples, "invalid tuple index to print: %d\n", i);

    string_val_t *tuple = &gi->tuples[i * gi->num_vars];

    results_t *pres = (results_t *)((uint8_t *)results + match_get_result_size() * i);

    printf("{");
    int nitems = match_results_to_json(pres);

    for (int j = 0; j < gi->num_vars; j++) {
        if (gi->var_names[j][0] == '%') {
            if (nitems)
                printf(",");
            nitems++;
            print_json_string(gi->var_names[j], -1);
            printf(":");
            print_json_string(tuple[j].str, tuple[j].len);
        } else if (gi->var_names[j][0] == '#') {
            if (nitems)
                printf(",");
            nitems++;
            print_json_string(gi->var_names[j], -1);
            printf(":");
            printf("[");
            for (int k = 0; k < tuple[j].len; k++) {
                string_val_t v = tuple[j].str_set[k];
                if (k != 0)
                    printf(",");
                print_json_string(v.str, v.len);
            }
            printf("]");
        } else {
            CHECK(false, "not supposed to reach this while printing tuple\n");
        }
    }

    printf("}");
}

void output_json(groupby_info_t *gi, results_t *results)
{
    fprintf(stderr, "Generating JSON output\n");
    if (gi == NULL || gi->num_vars == 0 || gi->merge_results) {
        /* simple non-groupby query, single result, just print it */
        printf("{\n");
        int nitems = match_results_to_json(results);
        printf("}\n");
    } else {
        /* Print results to stdout as JSON */
        printf("[\n");
        int n_printed = 0;

        for (int i = 0; i < gi->num_tuples; i++) {
            if (n_printed)
                printf(",\n");
            output_groupby_result_json(gi, i, results);
            n_printed++;
        }
        printf("]\n");
    }
}
