#pragma once

void json_add_int(void *p, char *name, int64_t value);
void json_add_set(void *p, char *name, set_t *value);
void json_add_multiset(void *p, char *name, set_t *value);
int match_results_to_json(struct results_t *results);
void set_to_json(set_t *src);
void json_add_hll(void *p, char *name, hyperloglog_t *hll);
void output_groupby_result_json(groupby_info_t *gi, int i, results_t *results);
char * hll_to_string(hyperloglog_t * hll);
