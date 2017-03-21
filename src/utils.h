#pragma once

/*
 * Extract field values from current item in ctx and return a string-encoded
 * tuple for `yield to` statements.
 *
 * Tuple is encoded so that it doesn't have 0 bytes and can be used with JSL
 * arrays.
 *
 * Generated matcher code doesn't care about encoding specifics.
 */
typedef struct string_tuple_t {
    char buf[256];
    int len;
} string_tuple_t;

#define TUPLE_ITEM_TYPE_STRING 'S'
#define TUPLE_ITEM_TYPE_BYTES  'B'

/*
 * Create a new tuple.
 */
void string_tuple_init(string_tuple_t *tuple);

/*
 * Add an item to a tuple. Type is one of the types above.
 *
 * - use BYTES type for encoding binary strings that may contain zero bytes
 * - use STRING type for utf-8 encoded strings
 */
void string_tuple_append(char *val, int length, int type, string_tuple_t *tuple);

/*
 * Extract and decode first item from an encoded tuple.
 *
 * Returns a pointer to the "tail" of the tuple, that is everything except the
 * extracted first item. You can call this function over the same tuple
 * multiple times to extract all items.
 */
char *string_tuple_extract_head(char *tuple, int result_buf_size, uint8_t *result_buf, int *result_len, int *result_type);

/*
 * Check if tuple is empty.
 */
int string_tuple_is_empty(char *tuple);

void set_add(set_t *dst, const set_t *src);
void mset_add(set_t *dst, const set_t *src);

void set_insert(set_t *dst, string_tuple_t *tuple);
void mset_insert(set_t *dst, string_tuple_t *tuple);

void set_free(set_t *s);

/*
 * Fail with error.
 */
void error(char *err);