#pragma once

/*
 * foreach (aka groupby) statements syntax is
 *  FOREACH var1,var2...varN IN tuple_list
 *
 *  Basically normal "for" loop with destructuring in e.g. Python.
 *
 * var1..varN are variables, each has associated traildb field
 * and its value can be either a scalar (string) or a set of strings.
 *
 * tuple_list is a list of tuples to iterate over.
 *
 */
#include "fns_imported.h"

/******** Tuple item representation ***************************************/

/*
 * Either scalar with explicit length, or set of strings, stored as array of
 * string_val_t sized of size 'len'. All memory pointed to by 'str' must be
 * allocated manually.
 */
typedef struct string_val_t {
    uint64_t len;
    union {
        char *str;
        struct string_val_t *str_set;
    };
} string_val_t;


/*
 * Same as above, represented as integer ids, specific for a traildb. Either
 * scalar int or Judy set.
 */
typedef union id_value_t {
    Pvoid_t id_set;
    int id;
} id_value_t;


/********* Utils for converting between above two **************************/

/* may return -1 if value not found */
int scalar_to_local(db_t *db, int field_id, const char *value, int length);

Pvoid_t set_to_local(db_t *db, int field_id, string_val_t *values, int length);


/*
 * Everything we know about foreach loop after parsing.
 *
 *   foreach var1..varN in @array
 *
 * Below
 *      N is stored as num_vars
 *      @array size is stored as num_tuples
 */
typedef struct groupby_info_t {
    string_val_t *tuples; /* num_tuples x num_vars */
    int num_tuples;

    int num_vars;
    char **var_names;
    char **var_fields;

    int merge_results;
} groupby_info_t;



/* Convert string-based tuples to id-based tuples */
id_value_t *groupby_ids_create(const groupby_info_t *gi, db_t *db);

void groupby_ids_free(const groupby_info_t *gi, id_value_t *id_tuples);

/*
 * Value id to tuple index.
 *
 * This maps value id to indices in the tuple list that contain that value in
 * some way: either as a scalar or as member of a set.
 *
 * (field_id, value_id) -> [tuple_idx ...]
 */
typedef struct vti_index_t {
    int num_fields;
    Pvoid_t *by_field_id; /* Array of judyL maps, one for every field */
} vti_index_t;

void vti_index_create(vti_index_t *idx, const groupby_info_t *gi, id_value_t *id_tuples, tdb *db);

/* Returned tuple index array is terminated by -1 value. */
int *vti_index_lookup(vti_index_t *index, int field_id, int val_id);

int vti_index_have_field(vti_index_t *index, int field_id);

void vti_index_free(vti_index_t *index);
