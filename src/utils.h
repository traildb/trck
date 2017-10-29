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

typedef struct json_object json_object;

#define TUPLE_ITEM_TYPE_STRING 'S'
#define TUPLE_ITEM_TYPE_BYTES  'B'

// Max Judy line length
#define MAXLINELEN 1000000

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
 * Applies the run-length encoding to the `in` string.
 * Returns a pointer to the encoded string, the caller is responsible
 * to free the memory pointed by `out`.
 */
char *run_length_encode(char *in, size_t size, int *out_size);
/*
 * Converts string `str` to hex string and writes it to `dst`.
 */
void str_to_hex_str(char *dst, char *src, size_t size);

/*
 * "FF" -> 255
 *
 * Assumes there is indeed two chars in the buffer it is passed in,
 * handles null terminators by ignoring them
 */
uint8_t hex_byte_to_byte(const char* h);


/* Free hll struct */
void hll_free(hyperloglog_t *hll);

/* merge another hll into this one*/
hyperloglog_t *hll_merge(hyperloglog_t *this, hyperloglog_t *other);

/* Given the run-length encoded hex string representation of ah HLL,
 * decodes it into an hyperloglog_t */
hyperloglog_t *hll_rle_decode(const char* hll_rle_str);

hyperloglog_t *hll_insert(hyperloglog_t *hll, string_tuple_t *tuple);

/*
 * Fail with error.
 */
void error(char *err);

/*
 * Calculate the size of a set by iterating through all of the elements
 */
int JSL_size(set_t *value);
