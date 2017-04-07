#ifndef __HYPERLOGLOG_H__
#define __HYPERLOGLOG_H__
#include <x86intrin.h>
#include <stdint.h>
#include <string.h>
#include <math.h>
#include <json-c/json.h>
#include <json-c/json_object.h>
#include <stdio.h>

#include "hll_common.h"
#include "safeio.h"
#include "utils.h"

#define MIN_P 14

/* Add value to HLL */
extern void hll_add(hyperloglog_t *this, void *v, size_t nbytes);

/* Get cardinality estimate using HLL */
extern double hll_estimate(hyperloglog_t *this);

/* Free hll struct */
extern void hll_free(hyperloglog_t *hll);

/* Returns standard error for HLL with 2^p registers */
double hll_error(int p);

/* merge another hll into this one*/
extern hyperloglog_t *hll_merge(hyperloglog_t *this, hyperloglog_t *other);

extern json_object * hll_to_json(hyperloglog_t * hll);

extern hyperloglog_t * json_to_hll(json_object * obj);

extern char * hll_to_string(const hyperloglog_t * hll);

extern hyperloglog_t * string_to_hll(const char * shll);

#endif

