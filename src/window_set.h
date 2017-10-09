#pragma once

/*
 * Set of timestamp windows, mapping a cookie to {start_ts,end_ts}
 */
typedef struct window_set_t window_set_t;

window_set_t *parse_window_set(const char *path);

void free_window_set(window_set_t *s);

/* get start/end timestamps for a cookie */
int window_set_get(window_set_t *set, const uint8_t *cookie, uint64_t *start_ts, uint64_t *end_ts);

/* get all cookies as a flat array */
__uint128_t *window_set_get_ids(window_set_t *set, uint64_t *num_ids);

void window_set_id_to_cookie(window_set_t *set, const uint8_t *id, __uint128_t *out_cookie);

void dump_window_set(window_set_t *res);
