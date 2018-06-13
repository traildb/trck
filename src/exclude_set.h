#pragma once

/*
 * Set of cookies to exclude
 */
typedef struct exclude_set_t exclude_set_t;

exclude_set_t *parse_exclude_set(const char *path);

void free_exclude_set(exclude_set_t *s);

/* return if set contains cookie */
int exclude_set_contains(const exclude_set_t *set, const uint8_t *cookie);

void dump_exclude_set(const exclude_set_t *res);
