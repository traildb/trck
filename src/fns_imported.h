#pragma once

#include <stdbool.h>

typedef uint64_t timestamp_t;

typedef const void *item_t;

/*
 *  Generated matcher code knows nothing about traildbs and can virtually run
 *  with any underlying storage engine; following functions are used by
 *  generated code to access trail data.
 */

/*
 ******************************************************************************
 *                        db_ functions                                       *
 ******************************************************************************
 */

int db_get_key_id(const char *, db_t *);
int db_get_value_id(const char *, int, int, db_t *);


/*
 ******************************************************************************
 *                        item_ functions                                     *
 ******************************************************************************
 */

int item_get_value_id(item_t, int);

/*
 *  Trail may have dummy empty items appended at the end to trigger end-of trail
 *  conditions.
 */
bool item_is_empty(item_t item);

/* Get item timestamp */
timestamp_t item_get_timestamp(item_t);

/*
 ******************************************************************************
 *                         ctx_ functions                                     *
 ******************************************************************************
 */

const char *ctx_get_item_value(ctx_t *, item_t, int, int*);
item_t ctx_get_item(ctx_t *);
void ctx_get_cookie(ctx_t *, char buf[33]);
uint64_t ctx_get_cookie_timestamp_filter_end(ctx_t *);
uint64_t ctx_get_cookie_timestamp_filter_start(ctx_t *);

/* Advance context to next item in the trail */
void ctx_advance(ctx_t *ctx);

/* Get current position within the trails */
int64_t ctx_get_position(ctx_t *ctx);

/* Check if we're at the end of trail */
bool ctx_end_of_trail(ctx_t *ctx);

/* Hints for query optimizer passed through opaque ctx object. */
typedef enum {GROUPBY_USED=1, RESULT_UPDATED=2} stats_flag;
void ctx_update_stats(ctx_t *ctx, stats_flag flag);
