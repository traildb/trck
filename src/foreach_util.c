#include <string.h>
#include <traildb.h>
#include <Judy.h>
#include <stdbool.h>
#include "fns_generated.h"
#include "safeio.h"
#include "foreach_util.h"
#include "match_internal.h"

#if DEBUG
#define DBG_PRINTF(msg, ...) fprintf(stderr, msg, ##__VA_ARGS__);
#else
#define DBG_PRINTF(msg, ...)
#endif

/*
 * Convert string value to a db-specific ("local") id.
 * May return -1 if value not found.
 */
int scalar_to_local(db_t *db, int field_id, const char *value, int len)
{
    return db_get_value_id(value, len, field_id, db);
}

/*
 * Convert a set of string values to a set of db-specific ("local") ids.
 */
Pvoid_t set_to_local(db_t *db, int field_id, string_val_t *values, int len)
{

    Pvoid_t set = NULL;
    for(int i = 0; i < len; i++) {
        /* get value id */
        int v = scalar_to_local(db, field_id, values[i].str, values[i].len);

        /* if valid, insert into set */
        int Rc_int;
        if (v > 0) {
            J1S(Rc_int, set, v);
        }
    }
    return set;
}

/*
 * Convert foreach tuples to db-specific ids.
 */
id_value_t *groupby_ids_create(const groupby_info_t *gi, db_t *db)
{
    /* walk through string tuples and convert them to traildb value id tuples */
    id_value_t *res = calloc(gi->num_tuples * gi->num_vars, sizeof(id_value_t));
    CHECK(res, "cannot allocate local groupby id array");

    tdb_field field_ids[gi->num_vars];
    tdb_field field;
    for (int j = 0; j < gi->num_vars; j++) {
        tdb_error e = tdb_get_field(db->db, gi->var_fields[j], &field_ids[j]);
        if (e)
            field_ids[j] = -1;
    }

    id_value_t *out = res;
    for (int i = 0; i < gi->num_tuples; i++) {
        for (int j = 0; j < gi->num_vars; j++) {
            int field_id = field_ids[j];

            if (field_id == -1)
                continue;

            string_val_t tuple = gi->tuples[i * gi->num_vars + j];

            switch (gi->var_names[j][0]) {
                case '%':
                    out->id = scalar_to_local(db,
                                              field_id,
                                              tuple.str,
                                              (int)tuple.len);

                    out++;
                    break;
                case '#':
                    out->id_set = set_to_local(db,
                                               field_id,
                                               tuple.str_set,
                                               tuple.len);
                    out++;
                    break;
                default:
                    CHECK(false, "invalid var name : %s\n", gi->var_names[j]);
            }
        }
    }

    return res;
}

void groupby_ids_free(const groupby_info_t *gi, id_value_t *id_tuples)
{
    for (int i = 0; i < gi->num_tuples; i++) {
        id_value_t *tuple = &id_tuples[i * gi->num_vars];
        for (int j = 0; j < gi->num_vars; j++) {
            int rc;

            switch (gi->var_names[j][0]) {
                case '%':
                    /* do nothing */
                    break;
                case '#':
                    if (tuple[j].id_set != NULL)
                        J1FA(rc, tuple[j].id_set);
                    break;
            }
        }
    }
    free(id_tuples);
}

void vti_index_create(vti_index_t *idx, const groupby_info_t *gi,
                      id_value_t *id_tuples, tdb *db)
{

    /*
     * Walk through id_tuples and create a mapping from value id to a list of
     * tuple indices.
     */
    tdb_field field_ids[gi->num_vars];

    for (int j = 0; j < gi->num_vars; j++) {
        tdb_error e = tdb_get_field(db, gi->var_fields[j], &field_ids[j]);
        if (e)
            field_ids[j] = -1;
    }

    /*
     * First go through id_tuples and create mapping as
     *
     *    field id -> value id -> judy set of tuple indexes
     *
     * Then we convert judy sets to int *arrays as they are more convenient
     * during matching.
     */
    Pvoid_t *tmp = calloc(tdb_num_fields(db), sizeof(Pvoid_t));
    CHECK(tmp, "cannot allocate temp vti index\n");

    for (int i = 0; i < gi->num_tuples; i++) {
        id_value_t *tuple = &id_tuples[i * gi->num_vars];

        PWord_t pv;
        int rc, rc2;
        Word_t index;

        for (int j = 0; j < gi->num_vars; j++) {
            int field_id = field_ids[j];

            if (field_id <= 0)
                continue;

            switch (gi->var_names[j][0]) {
                case '%':
                    JLI(pv, tmp[field_id], tuple[j].id);
                    J1S(rc, *(Pvoid_t *)pv, i);
                    CHECK(*pv != 0, "woo");
                    break;
                case '#':
                    index = 0;
                    J1F(rc, tuple[j].id_set, index);
                    while (rc) {
                        JLI(pv, tmp[field_id], index);
                        J1S(rc2, *(Pvoid_t *)pv, i);
                        J1N(rc, tuple[j].id_set, index);
                    }
                    break;
            }
        }
    }


    idx->by_field_id = calloc(tdb_num_fields(db), sizeof(Pvoid_t));
    idx->num_fields = tdb_num_fields(db);
    CHECK(idx->by_field_id, "cannot allocate vti field indexes\n");

    for (int f = 0; f < tdb_num_fields(db); f++) {
        PWord_t pid_set;
        Word_t value_id = 0;

        JLF(pid_set, tmp[f], value_id);

        while (pid_set) {
            Pvoid_t id_set = *(Pvoid_t *)pid_set;

            /* get judy set of tuple indices */
            Word_t set_size;
            J1C(set_size, id_set, 0, -1);

            /* allocate linear array to fit all the values (terminated by -1) */
            uint32_t *id_array = malloc(sizeof(uint32_t) * (set_size + 1));
            CHECK(id_array, "cannot allocate id array");

            /* copy items to the array */
            Word_t set_index = 0;
            int rc;
            uint32_t *out = id_array;
            J1F(rc, id_set, set_index);
            while(rc) {
                *out = set_index;
                out++;
                J1N(rc, id_set, set_index);
            }
            *out = -1;

            /* free temporary judy set */
            J1FA(rc, id_set);

            /* insert value into idx map for this field */
            PWord_t out_pv;
            JLI(out_pv, idx->by_field_id[f], value_id);
            *(uint32_t **)out_pv = id_array;

            JLN(pid_set, tmp[f], value_id);
        }

        int rc;
        JLFA(rc, tmp[f]);
    }
    free(tmp);
}

/*
 * Look up list of tuple indexes from the index given field_id and value id
 */
int *vti_index_lookup(vti_index_t *index, int field_id, int val_id) {
    CHECK(field_id < index->num_fields, "invalid field_id in vti_lookup");
    CHECK(field_id > 0, "invalid field_id in vti_lookup: %d", field_id);

    PWord_t pv;
    JLG(pv, index->by_field_id[field_id], val_id);
    if (pv)
        return *(int **)pv;
    else
        return NULL;
}

int vti_index_have_field(vti_index_t *index, int field_id) {
    CHECK(field_id > 0, "invalid field_id in vti_index_have_field: %d", field_id);
    return (index->by_field_id[field_id] == 0) ? 0 : 1;
}

void vti_index_free(vti_index_t *index) {
    for (int f = 0; f < index->num_fields; f++) {
        Word_t tuple_idx = 0;
        PWord_t pv;
        JLF(pv, index->by_field_id[f], tuple_idx);
        while (pv) {
            free(*(int **)pv);
            JLN(pv, index->by_field_id[f], tuple_idx);
        }

        int rc;
        JLFA(rc, index->by_field_id[f]);
    }
    free(index->by_field_id);
}
