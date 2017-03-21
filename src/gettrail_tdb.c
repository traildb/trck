#define _XOPEN_SOURCE 700
#define  _BSD_SOURCE

#include <stdio.h>
#include <traildb.h>
#include <Judy.h>

#include <stdbool.h>
#include <string.h>
#include <getopt.h>

#include "safeio.h"
#include "traildb_filter.h"

#define TRAIL_BUFSIZE 2000000

/* No reason for this particular number, just sanity check */
#define MAX_FIELD_NAME_LENGTH 100000

typedef uint64_t timestamp_t;
#define MAX(a,b) (((a)>(b))?(a):(b))


/*
 * Get distinct field names as zero-separated string array from a set of
 * traildbs.
 *
 * Returns pointer to char array, with num_result_fields set to size of array.
 */
char **get_tdb_field_names(char **traildb_paths,
                          int num_paths,
                          int *num_result_fields)
{

    Pvoid_t name_set = NULL;

    int max_field_len = 0;
    int num_fields = 0;

    Word_t *pv;

    /*
     * Go through traildbs, get all field names and insert them in name_set.
     * Compute number of distinct fields and max_field_len.
     */
    for (int ti = 0; ti < num_paths; ti++) {
        tdb *db = tdb_init();
        tdb_error res = tdb_open(db, traildb_paths[ti]);
        CHECK(res == 0, "cannot open %s, error code: %d", traildb_paths[ti], res);

        for (int i = 1; i < tdb_num_fields(db); i++) {
            const char *field_name = tdb_get_field_name(db, i);
            JSLI(pv, name_set, (uint8_t *)field_name);
            if (*pv == 0) {
                *pv = 1;
                num_fields += 1;
                max_field_len = MAX(strlen(field_name), max_field_len);
            }
        }

        tdb_close(db);
    }

    CHECK(max_field_len < MAX_FIELD_NAME_LENGTH, "Field name too long");

    /*
     * Prepare a buffer to store field names in.
     */
    char **result = calloc(num_fields, sizeof(char *));
    int n = 0;

    /*
     * Temporary field name buffer
     */
    char *field_name = calloc(max_field_len + 1, 1);

    JSLF(pv, name_set, (uint8_t *)field_name);       /* get first string */

    while (pv != NULL)
    {
        result[n] = strdup(field_name);
        n++;

        JSLN(pv, name_set, (uint8_t *)field_name);   /* get next string */
    }

    int rc;
    JSLFA(rc, name_set);
    free(field_name);

    *num_result_fields = num_fields;
    return result;
}

void extract_trails(char **traildb_paths, int num_paths, Pvoid_t cookies,
                    char *output, char *filter)
{
    int num_result_fields = 0;

    /*
     * Get field names for the resulting traildb. Input traildbs do not have to
     * have same field set; for example, if you have two input traildbs, one
     * with fields {A, B} and another having {B, C}, we'll produce an aggregate
     * traildb with fields {A,B,C}, where C will be empty for events that came
     * from first traildb, and A empty for events that came from second.
     */
    char **field_names = get_tdb_field_names(traildb_paths, num_paths, &num_result_fields);
    CHECK(field_names, "failed to get db fields");

    tdb_cons *cons = tdb_cons_init();
    tdb_error e = tdb_cons_open(cons, output, (const char **)field_names, num_result_fields);
    CHECK(e == 0, "failed to create a new traildb");

    /* Store values to insert here. */
    const char **values = calloc(num_result_fields, sizeof(char *));
    uint64_t *lengths = calloc(num_result_fields, sizeof(uint64_t));

    for (int ti = 0; ti < num_paths; ti++) {
        tdb *db = tdb_init();
        tdb_error res = tdb_open(db, traildb_paths[ti]);
        CHECK(res == 0, "failed to open traildb %s, error code: %d", traildb_paths[ti], res);

        tdb_cursor *cursor = tdb_cursor_new(db);

        struct tdb_event_filter *compiled_filter = NULL;
        if (filter && strlen(filter) > 0)
            compiled_filter = traildb_compile_filter(db, filter, strlen(filter));

        tdb_field result_field_ids[TDB_MAX_NUM_FIELDS] = {-1};

        /*
         * Map fields in an input traildb to result fields; we ignore errors
         * here as some of the result fields may be missing from some of input
         * traildbs. These will keep values of -1.
         */
        for (int i = 0; i < num_result_fields; i++)
            tdb_get_field(db, field_names[i], &result_field_ids[i]);


        for (int i = 0; i < tdb_num_trails(db); i++) {
            const uint8_t *cookie = tdb_get_uuid(db, i);

            uint8_t hexcookie[33] = {0};
            tdb_uuid_hex(cookie, hexcookie);

            Word_t *pv;
            JSLG(pv, cookies, hexcookie);

            if (pv) {
                tdb_error e = tdb_get_trail(cursor, i);
                CHECK(e == 0, "could not read trail %d", i);

                if (compiled_filter != NULL)
                    tdb_cursor_set_event_filter(cursor, compiled_filter);

                const tdb_event *event;


                while ((event = tdb_cursor_next(cursor))) {
                    timestamp_t ts = event->timestamp;

                    for (int k = 0; k < num_result_fields; k++) {
                        tdb_field rfid = result_field_ids[k];

                        if (rfid > 0)
                            values[k] = tdb_get_item_value(db, event->items[rfid-1], &lengths[k]);
                        else
                            lengths[k] = 0;
                    }

                    tdb_cons_add(cons, cookie, ts, values, lengths);
                }
            }

        }

        tdb_cursor_free(cursor);
        if (compiled_filter)
            free(compiled_filter);
        tdb_close(db);

    }

    tdb_cons_finalize(cons);
    tdb_cons_close(cons);

    for (int i = 0; i < num_result_fields; i++)
        free(field_names[i]);

    free(field_names);
    free(values);
    free(lengths);
}


int parse_args(int argc, char **argv, char **filter)
{
    *filter = 0;

    while (1) {
        int c;
        int option_index = 0;
        static struct option long_options[] = {
            {"filter",    required_argument, 0,   'f' },
            {0,           0,                 0,    0 }
        };

        c = getopt_long(argc, argv, "f:",
                        long_options, &option_index);
        if (c == -1)
            break;

      switch (c) {
          case 'f': *filter = optarg; break;
      }
    }

    return optind;
}

int main(int argc, char **argv)
{
    char *filter = 0;
    int base = parse_args(argc, argv, &filter);

    if (argc - base < 3) {
        fprintf(stderr, "too few arguments, usage: gettrail_tdb COOKIE,... OUT_TDB IN_TDB1 IN_TDB2 ...\n");
        exit(1);
    }

    char *cookies = strdup(argv[base]);

    Pvoid_t cookie_set = NULL;
    Word_t *pv;

    char buf[50] = {0};

    /* cookie list passed on stdin */
    if (strcmp(cookies, "-") == 0) {
        while (fgets(buf, sizeof(buf), stdin)) {
            if (strlen(buf) == 33) {
                buf[32] = 0;
                JSLI(pv, cookie_set, (uint8_t *)buf);
                *pv = 1;
            } else {
                fprintf(stderr, "invalid cookie: '%s'\n", buf);
                exit(1);
            }
        }
    } else {
        /* cookie list passed as argument */
        char *list = cookies;
        char *token;

        while ((token = strsep(&cookies, ",")) != NULL) {
            JSLI(pv, cookie_set, (uint8_t *)token);
            *pv = 1;
        }

        free(list);
    }

    extract_trails(&argv[base + 2], argc - base - 2, cookie_set, argv[base + 1], filter);
    free(cookies);
    int rc;
    if (cookie_set)
        JSLFA(rc, cookie_set);
}
