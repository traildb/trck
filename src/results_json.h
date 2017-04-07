#pragma once

#include "hyperloglog.h"
#include "utils.h"

struct results_t;


void output_json(groupby_info_t *gi, results_t *results);
