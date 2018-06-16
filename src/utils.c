#include <stdbool.h>
#include <string.h>
#include <Judy.h>
#include "fns_generated.h"
#include "fns_imported.h"
#include "hyperloglog.h"
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


int string_tuple_size(char* index) {
    int count = 1;
    for (int i = 0; index[i] != '\0'; i++) {
        if (index[i] ==  ',') count++;
    }
    return count;
}


/*
 * Tuple encoding is done as follows: most bytes are left as is, except
 * 0x00  -> 0xff 0xfe
 * ','   -> 0xff 0xfd
 * 0xff  -> 0xff 0xff
 */
void string_tuple_append(char *val, int length, int type, string_tuple_t *tuple) {
    /*
     * Make sure there is space for zero terminator, comma,
     * type and one byte of the new value.
     */
    if (tuple->len == sizeof(tuple->buf)-5)
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

hyperloglog_t *hll_rle_decode(const char* hll_rle_str)
{
    enum State {
        LEN,
        LEN_E,
        M_VAL
    };
    uint8_t p = hex_byte_to_byte(&hll_rle_str[0]);
    enum State curstate = LEN;
    hyperloglog_t *res = hll_init(p);
    uint32_t len = 0;
    uint32_t register_index = 0;
    hll_rle_str += 4;
    /* if we get passed a json string it ends with a qutation mark
       handle both just in case 
    */
    while((*hll_rle_str != '\0') && (*hll_rle_str != '\"')){
        uint32_t val = hex_byte_to_byte(hll_rle_str);
        if(curstate == LEN){
            len = val & ((1 <<7) -1);
            // If MSB is 1 length didn't fit in 1 byte
            if (val & (1<<7)){
                curstate = LEN_E;
            } else {
                curstate = M_VAL;
            }
        } else if (curstate == LEN_E){
            len |= (val << 7);
            curstate = M_VAL; 
        }
        else if (curstate == M_VAL){
            uint32_t stop = register_index + len;
            for(; register_index < stop; register_index++){
                res->M[register_index] = val; 
            }
            len =0;
            curstate = LEN;
        } else {
            return NULL;
        }
        hll_rle_str += 2;
    }
    return res;
}

uint8_t hex_byte_to_byte(const char* h){
    uint8_t res = 0;
    for(int i=0; i<2; i++){
        uint8_t byte = h[i];
        // This shouldn't be a problem because we should always get even strings
        // but just in case. Segfaults are a bitch
        if(byte != 0){
            if ( h[i]>= '0' && h[i] <= '9') byte = byte - '0';
            else if (byte >= 'a' && byte <='f') byte = byte - 'a' + 10;
            else if (byte >= 'A' && byte <='F') byte = byte - 'A' + 10;
        }
        res = (res << 4) | (byte & 0xF);
    }
    return res;
}

char *run_length_encode(char *in, size_t size, int *out_size) {
    char *out;
    *out_size = 0;
    if (size == 0) {
        out = malloc(1);
        *out = '\0';
        return out;
    } else {
        /* Allocate twice the size of the input string
           which is the max amount memory we could need,
           in the worst case. */
        out = malloc(size * 2);
        out[size * 2 - 1] = 0;
    }

    char curr = in[0];

    uint16_t count = 1;

    for (int i = 1; i <= size; i++) {
        if (i < size && curr == in[i]) {
            count++;
            continue;
        }
        if (count > (1 << 7) - 1) {
            // Set MSB and store first set of bits of the length
            out[*out_size] = (1 << 7) | (count & ((1 << 7) - 1));
            // Storing second part of bite of the length
            out[*out_size + 1] = count >> 7;
            *out_size += 2;
        } else {
            out[*out_size] = count;
            (*out_size)++;
        }
        out[*out_size] = curr;
        (*out_size)++;
        if(i < size){
            curr = in[i];
            count = 1;
        }
    }

    char *result = malloc(*out_size); // Allocate the exact bytes used
    memcpy(result, out, *out_size); 
    free(out);

    return result;
}

hyperloglog_t *hll_insert(hyperloglog_t *hll, string_tuple_t *tuple) {
    int tlen;
    char *tval = string_tuple_to_string(tuple, &tlen);

    if (hll == NULL)
        hll = hll_init(14);

    hll_add(hll, tval, tlen);
    return hll;
}

void str_to_hex_str(char *dst, char *src, size_t size)
{
  char hex_str[]= "0123456789abcdef";

  if (!size) {
      *dst = '\0';
  }

  dst[size * 2 - 1] = 0;

  for (int i = 0; i < size; i++) {
      dst[i * 2] = hex_str[(src[i] >> 4) & 0x0F];
      dst[i * 2 + 1] = hex_str[(src[i]) & 0x0F];
  }
}

void error(char *err) {
    CHECK(false, "error while %s", err);
}


int JSL_size(set_t *value) {
    uint8_t index[MAXLINELEN];
    index[0] = '\0';
    Word_t *pv;
    JSLF(pv, *value, index);
    int count = 0;
    while(pv) {
        count++;
        JSLN(pv, *value, index);
    }
    return count;
}
