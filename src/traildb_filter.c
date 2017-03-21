#include <stdio.h>
#include <json-c/json.h>
#include <traildb.h>
#include <string.h>

#include "traildb_filter.h"
#include "safeio.h"

struct tdb_event_filter *traildb_compile_filter(tdb *db, const char *filter_str,
                                               int len)
{
    struct json_tokener *tok = json_tokener_new();
    json_object *jobj = json_tokener_parse_ex(tok, filter_str, len);

    CHECK(json_tokener_success == json_tokener_get_error(tok),
          "failed to parse filter JSON");

    json_object *jclauses;
    CHECK(json_object_object_get_ex(jobj, "clauses", &jclauses),
          "JSON must contain clauses array");

    CHECK(json_object_is_type(jclauses, json_type_array),
          "clauses not an array");

    struct tdb_event_filter *filter = tdb_event_filter_new();
    CHECK(filter, "failed to create event filter");

    int nclauses = json_object_array_length(jclauses);

    for (int i = 0; i < nclauses; i++) {
        json_object *jclause = json_object_array_get_idx(jclauses, i);
        CHECK(json_object_is_type(jclause, json_type_array),
              "clause %d is not an array", i+1);

        int nterms = json_object_array_length(jclause);

        if (i > 0)
            CHECK(tdb_event_filter_new_clause(filter) == 0,
                         "failed to create filter clause");

        for (int j = 0; j < nterms; j++) {
            json_object *jterm = json_object_array_get_idx(jclause, j);

            json_object *jfield;
            CHECK(json_object_object_get_ex(jterm, "field", &jfield),
                  "term must contain field name (clause %d term %d)", i+1, j+1);

            CHECK(json_object_is_type(jfield, json_type_string),
                  "field name must be string (clause %d term %d)", i+1, j+1);

            const char *field_name = json_object_get_string(jfield);

            json_object *jvalue;
            CHECK(json_object_object_get_ex(jterm, "value", &jvalue),
                  "term must contain value (clause %d term %d)", i+1, j+1);

            CHECK(json_object_is_type(jvalue, json_type_string),
                  "value must be string (clause %d term %d)", i+1, j+1);

            const char *value = json_object_get_string(jvalue);

            json_object *jop = NULL;
            const char *op = "equal";
            if (json_object_object_get_ex(jterm, "op", &jop)) {

                CHECK(json_object_is_type(jop, json_type_string),
                      "op must be string (clause %d term %d)", i+1, j+1);

                op = json_object_get_string(jop);
            }

            int is_negative = (strcmp(op, "notequal") == 0) ? 1 : 0;

            tdb_field field_id;
            tdb_error e = tdb_get_field(db, field_name, &field_id);
            if (e != 0) {
                /* If field is not found, we assume it is equal to "" */
                if (strcmp("", value) == 0) {
                    CHECK(tdb_event_filter_add_term(filter, 0, is_negative ^ 1) == 0,
                          "tdb_event_filter_add_term");
                } else {
                    CHECK(tdb_event_filter_add_term(filter, 0, is_negative) == 0,
                          "tdb_event_filter_add_term");
                }
            } else {
                /*
                 * tdb_get_item() returns 0 if field value is not in the db
                 * This is fine, then this term evaluates to FALSE
                 */
                uint64_t value_length = strlen(value);
                tdb_item item = tdb_get_item(db, field_id, value, value_length);

                CHECK(tdb_event_filter_add_term(filter, item, is_negative) == 0,
                      "tdb_event_filter_add_term");
            }
        }

    }

    json_object_put(jobj);
    json_tokener_free(tok);
    return filter;
}
