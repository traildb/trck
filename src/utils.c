#include <stdbool.h>
#include <string.h>
#include <Judy.h>
#include "fns_generated.h"
#include "fns_imported.h"
#include "utils.h"
#include "safeio.h"

char *string_tuple_to_string(string_tuple_t *tuple, int *length)
{
    *length = tuple->len;
    return tuple->buf;
}

void set_add(set_t *dst, const set_t *src)
{
    uint8_t index[10000];
    index[0] = '\0';

    Word_t * pv;
    JSLF(pv, *src, index);
    while (pv) {
        Word_t *pv2;
        JSLI(pv2, *dst, index);
        *pv2 += *pv;

        JSLN(pv, *src, index);
    }
}

void set_insert(set_t *dst, string_tuple_t *tuple)
{
    int tlen;
    char *tval = string_tuple_to_string(tuple, &tlen);

    Word_t *pv;
    JSLI(pv, *dst, (uint8_t *)tval);
    (*pv) += 1;
}

void mset_add(set_t *dst, const set_t *src)
{
    set_add(dst, src);
}

void mset_insert(set_t *dst, string_tuple_t *tuple)
{
    set_insert(dst, tuple);
}

void set_free(set_t *s)
{
    Word_t bytes;
    JSLFA(bytes, *s);
}


void string_tuple_init(string_tuple_t *tuple)
{
    memset(tuple->buf, 0, sizeof(tuple->buf));
    tuple->len = 0;
}

/*
 * Tuple encoding is done as follows: most bytes are left as is, except
 * 0x00  -> 0xff 0xfe
 * ','   -> 0xff 0xfe
 * 0xff  -> 0xff 0xff
 */
void string_tuple_append(char *val, int length, int type, string_tuple_t *tuple) {
    if (tuple->len == sizeof(tuple->buf)-1)
        return;

    if (tuple->len) {
        tuple->buf[tuple->len] = ',';
        tuple->len++;
    }

    tuple->buf[tuple->len] = type;
    tuple->len++;


    for (int i = 0; i < length; i++) {
        switch(val[i]) {
            case ',':
                tuple->buf[tuple->len] = '\xff';
                tuple->buf[tuple->len+1] = '\xfd';
                tuple->len += 2;
                break;
            case '\0':
                tuple->buf[tuple->len] = '\xff';
                tuple->buf[tuple->len+1] = '\xfe';
                tuple->len += 2;
                break;
            case '\xff':
                tuple->buf[tuple->len] = '\xff';
                tuple->buf[tuple->len+1] = '\xff';
                tuple->len += 2;
                break;
            default:
                tuple->buf[tuple->len] = val[i];
                tuple->len += 1;
        }
        if (tuple->len >= sizeof(tuple->buf)-2)
            break;
    }

}

int string_tuple_is_empty(char *tuple) {
    return *tuple == '\0';
}
/*
 * Given a string-encoded tuple, copy first item to a buffer pointed to by
 * result. Return result length in result_len. Tuple is assumed to be not
 * empty.
 *
 * Return a pointer to the rest of the tuple (think tuple[1:] in python).
 */
char *string_tuple_extract_head(char *tuple, int result_buf_size, uint8_t *result_buf, int *result_len, int *result_type) {
    uint8_t *d = result_buf;
    int i = 1;

    *result_type = *(uint8_t *)tuple;

    while (tuple[i] && tuple[i] != ',') {
        if (tuple[i] != '\xff') {
            if (d - result_buf < result_buf_size) {
                *d = tuple[i];
                d++;
            }

            i++;
        } else {
            i++;
            if (d - result_buf < result_buf_size) {
                switch (tuple[i]) {
                    case '\xff': *d = 0xff; break;
                    case '\xfe': *d = 0; break;
                    case '\xfd': *d = ','; break;
                }
                d++;
            }
            i++;
        }
    }

    *result_len = d - result_buf;

    return tuple + i + (int)(tuple[i] == ',');
}

void error(char *err) {
    CHECK(false, "error while %s", err);
}
