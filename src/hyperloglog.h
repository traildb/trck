#pragma once
#include <json-c/json_object.h>

#include "hll_common.h"

#define MIN_P 14

void hll_add(hyperloglog_t *this, void *v, size_t nbytes);

/* Get cardinality estimate using HLL */
double hll_estimate(hyperloglog_t *this);

/* Returns standard error for HLL with 2^p registers */
double hll_error(int p);


hyperloglog_t * json_to_hll(json_object * obj);

char * hll_to_string(const hyperloglog_t * hll);

hyperloglog_t * string_to_hll(const char * shll);

