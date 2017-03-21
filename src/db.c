#include <stdbool.h>
#include <string.h>
#include <traildb.h>
#include <Judy.h>

#include "fns_generated.h"
#include "fns_imported.h"
#include "match_internal.h"
#include "safeio.h"
#include "traildb_filter.h"


#define TIMESTAMP_FIELD_ID 10000


void db_open(db_t *db, const char *traildb_path, const char *filter)
{
    tdb *t = tdb_init();
    CHECK(t != NULL, "failed to create db, out of memory?");
    db->db = t;
    tdb_error res = tdb_open(db->db, traildb_path);
    tdb_set_opt(db->db, TDB_OPT_CURSOR_EVENT_BUFFER_SIZE, opt_val(100000));

    CHECK(res == 0, "failed to open traildb %s, error code %d", traildb_path, res);
    memset(&db->id_lookup_table[0], 0, sizeof(db->id_lookup_table));

    int filter_size = 0;
    struct tdb_event_filter *compiled_filter = NULL;

    if (filter && strlen(filter) > 0)
        compiled_filter = traildb_compile_filter(db->db, filter, strlen(filter));
    db->filter = compiled_filter;
}

void db_close(db_t *db)
{
    int rc;
    int lu_size = sizeof(db->id_lookup_table) / sizeof(db->id_lookup_table[0]);
    for (int i = 0; i < lu_size; i++)
        if (db->id_lookup_table[i])
            JHSFA(rc, db->id_lookup_table[i]);
    tdb_close(db->db);
    free(db->filter);
}

/*
 * Get key id by name.
 * Return -1 if key does not exist.
 */
int db_get_key_id(const char* key, db_t *db)
{
    if (strcmp(key, "timestamp") == 0)
        return TIMESTAMP_FIELD_ID;
    else {
        tdb_field field;
        tdb_error e = tdb_get_field(db->db, key, &field);
        return (e == 0)?field : -1;
    }
}


Pvoid_t db_get_lookup_table(db_t *db, int field_id)
{
    CHECK(field_id < sizeof(db->id_lookup_table) / sizeof(db->id_lookup_table[0]),
          "field_id %d out of bounds", field_id);

    if (db->id_lookup_table[field_id] == NULL) {
        Pvoid_t res = NULL;
        uint64_t size = tdb_lexicon_size(db->db, field_id);
        CHECK(size > 0, "no entries in lexicon for field %d", field_id);
        for (uint64_t i = 0 ; i < size; i++) {
            uint64_t value_length;
            const char *v = tdb_get_value(db->db,
                                          field_id,
                                          i,
                                          &value_length);
            Word_t *pv;
            JHSI(pv, res, (uint8_t *) v, value_length);
            *pv = i;
        }
        db->id_lookup_table[field_id] = res;
    }
    return db->id_lookup_table[field_id];
}

/*
 * Get value id by name.
 * Returns 0 if value does not exist.
 */
int db_get_value_id(const char *val, int len, int keyid, db_t *db)
{
    if (keyid == TIMESTAMP_FIELD_ID) {
        char *endptr;
        long res = strtol(val, &endptr, 10);
        CHECK(endptr - val == strlen(val), "invalid timestamp value: %s", val);
        return res;
    }

    if (len == 0)
        return 0;

    if (keyid == -1)
        return -1;

    Pvoid_t table = db_get_lookup_table(db, keyid);
    Word_t *pv;
    JHSG(pv, table, (uint8_t *)val, len);
    if (pv) {
        return *(int *)pv;
    } else {
        return -1;
    }
}