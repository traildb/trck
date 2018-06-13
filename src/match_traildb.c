#define _XOPEN_SOURCE 700
#include <stdio.h>
#include <json-c/json.h>
#include <stdbool.h>
#include <string.h>
#include <Judy.h>
#include "judy_str_map.h"
#include <sys/stat.h>
#include <unistd.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <getopt.h>
#include <time.h>
#include <sys/time.h>

#ifdef _OPENMP
#include <omp.h>
#endif

#include "fns_generated.h"
#include "fns_imported.h"

#include <traildb.h>
#include "match_internal.h"

#include "judy_128_map.h"
#include "out_traildb.h"
#include "safeio.h"
#include "mempool.h"
#include "statevec.h"
#include "foreach_util.h"
#include "distinct.h"
#include "results_json.h"
#include "results_msgpack.h"
#include "utils.h"
#include "window_set.h"
#include "exclude_set.h"
#include "ctx.h"
#include "db.h"

#define MAX_STRING_LEN 1024 * 1024 * 100
#define MIN(a,b) (((a)<(b))?(a):(b))

#define MAX_TIMESTAMP 0xfffffffffffffffe
#define TRAIL_BUFSIZE 2000000

#if DEBUG
#define DBG_PRINTF(msg, ...) fprintf(stderr, msg, ##__VA_ARGS__);
#else
#define DBG_PRINTF(msg, ...)
#endif


/*
 * Run matcher on a dummy trail containing only one event with specified
 * timestamp to trigger remaining `after` clauses.
 */
void match_timestamp_only(timestamp_t timestamp,
                          state_t *state,
                          results_t *results,
                          const uint8_t *cookie)
{
    DBG_PRINTF("=======================\nmatch_timestamp_only\n====================\n");
    tdb_event e;
    e.timestamp = timestamp;
    e.num_items = 0;

    ctx_t ctx = {
        .current_event = &e,
        .cursor = NULL,
        .position = 0,
        .cookie = *(__uint128_t *)cookie
    };

    kvids_t ids;

    match_trail(state, results, &ids, &ctx);
}

/*
 * Note that after calling this, JSON object `params` internal memory is
 * referenced by `ids`. Therefore `params` object should not be freed before
 * `ids`.
 */
void set_params_from_json(json_object *params, kvids_t *ids, db_t *db)
{
    if (match_num_free_vars && params == NULL)
        CHECK(false, "Parameter not set: %s\n", match_free_vars[0]);

    for (int i = 0; i < match_num_free_vars; i++) {
        const char *param_name = match_free_vars[i];
        json_object *val;
        json_bool found = json_object_object_get_ex(params, param_name, &val);

        CHECK(found, "can't find parameter value for %s\n", param_name);
        CHECK((param_name[0] == '#') || (param_name[0] == '%'),
              "parameter name must start with %% or #: %s\n", param_name);

        /* get field name corresponding to value */
        const char *field = match_get_param_field(match_get_param_id(param_name));

        /* get field id corresponding to value */
        int field_id = db_get_key_id(field, db);

        if (field_id < 0) {
            DBG_PRINTF("cannot find field %s in the traildb", field);
            match_set_param(match_get_param_id(param_name), -1, ids, 0, 0);
            continue;
        }

        if (param_name[0] == '%') {

            CHECK(json_object_get_type(val) == json_type_string,
                  "%s is expected to have string value", param_name);

            const char *value = json_object_get_string(val);
            int value_length = json_object_get_string_len(val);

            match_set_param(match_get_param_id(param_name),
                            scalar_to_local(db, field_id, value, value_length),
                            ids,
                            (char *)value,
                            value_length);
        } else
        if (param_name[0] == '#') {
            DBG_PRINTF("parsing '%s' set from json params\n", param_name);

            CHECK(json_object_get_type(val) == json_type_array,
                    "%s is expected to be an array", param_name);

            int nvalues = json_object_array_length(val);
            string_val_t *values = malloc(sizeof(string_val_t) * nvalues);
            CHECK(values, "cannot allocate mem to read parameter array");

            for (int j = 0; j < nvalues; j++) {
                json_object *jitem = json_object_array_get_idx(val, j);
                CHECK(json_object_get_type(jitem) == json_type_string,
                      "expecting lists of strings as set parameter values for %s",
                      param_name);
                const char *str_value = json_object_get_string(jitem);
                size_t str_len = json_object_get_string_len(jitem);
                values[j].len = (uint64_t) str_len;
                values[j].str = (char *) str_value;
            }

            string_val_t set;
            set.str_set = values;
            set.len = nvalues;

            /* pass parameter to the matcher */
            match_set_list_param(match_get_param_id(param_name),
                                 set_to_local(db, field_id, set.str_set, set.len),
                                 ids);

            free(values);
        }
    }
}


#define MAX_VALUE_LEN 1024

static void *get_lexicon_fold_fun(uint64_t id, const char *value, uint64_t len,
                                  void *state)
{
    string_val_t *tuples = (string_val_t *) state;

    /*
     * We want to drop the judy_str_map, create copies of the entries,
     * which will later be free'd by free_groupby_info
     */

    char *str = malloc(len);
    CHECK(str != NULL, "failed to allocate memory while building lexicon");

    memcpy(str, value, len);
    tuples[id].len = len;
    tuples[id].str = str;

    return (void *) tuples;
}

/*
 * Get union of field lexicon across multiple dbs. Used for cases with implicit
 * foreach loop range.
 */
string_val_t *get_lexicon(char **traildb_paths, int num_paths,
                          char *field_name, int *cardinality)
{
    struct judy_str_map jsm;
    jsm_init(&jsm);
    tdb_error res;

    for (int i = 0; i < num_paths; i++) {
        tdb *t = tdb_init();
        res = tdb_open(t, traildb_paths[i]);
        CHECK(res == 0, "cannot open traildb %s", traildb_paths[i]);

        tdb_field groupby_field_id;
        res = tdb_get_field(t, field_name, &groupby_field_id);
        CHECK(res == 0, "groupby field %s is not defined for this traildb: %s\n", field_name, traildb_paths[i]);

        int cardinality = tdb_lexicon_size(t, groupby_field_id);
        for (int j = 0; j < cardinality; j++) {
            uint64_t val_length;
            const char *val = tdb_get_value(t, groupby_field_id, j, &val_length);
            if (val_length == 0)
                continue;

            uint64_t existing_id = jsm_get(&jsm, val, val_length);
            if (existing_id == 0) {
                /* judy_str_map creates a copy of what we insert */
                uint64_t new_id = jsm_insert(&jsm, val, val_length);
                CHECK(new_id != 0, "failed to insert into judy_str_map");
            }
        }
        tdb_close(t);
    }

    /* +1 to include the dummy empty string */
    *cardinality = jsm_num_keys(&jsm) + 1;
    string_val_t *result = malloc(sizeof(string_val_t) * (*cardinality));

    jsm_fold(&jsm, get_lexicon_fold_fun, result);
    jsm_free(&jsm);

    /* The first entry in the lexicon must always be the empty string */
    string_val_t first;
    first.len = 0;
    first.str = malloc(1);
    CHECK(first.str != NULL, "could not allocate memory");
    result[0] = first;

    return result;
}

void free_lexicon(char **lexicon, int cardinality)
{
    for (int i = 0; i < cardinality; i++)
        free(lexicon[i]);
    free(lexicon);
}

void add_results_vec(results_t *vec, int n, results_t *value)
{
    if (!match_is_zero_result(value)) {
        for (int k = 0; k < n; k++) {
            match_add_results(&vec[k], value);
        }
    }
}

void print_trail(ctx_t *ctx)
{
    for (int i = 0; i < ctx->num_events; i++) {
        tdb_event *ev = (tdb_event *)&ctx->buf[i * ctx->event_size];

        fprintf(stderr, "%" PRIu64 " ", ev->timestamp);
        for (int j = 0; j < ev->num_items; j++) {
            const char *field_name = tdb_get_field_name(ctx->db->db,
                                                        tdb_item_field(ev->items[j]));

            uint64_t value_length;
            const char *field_value = tdb_get_item_value(ctx->db->db, ev->items[j],
                                                         &value_length);
            fprintf(stderr, "%s=%.*s ", field_name, (int) value_length, field_value);
        }
        fprintf(stderr,", ");
    }
    fprintf(stderr, "\n");
}

/*
 * Helper function to run match for a given groupby value; s_in is initial state
 * (can be NULL). Assumes ctx and ids to be initialized with db-wide values.
 *
 * This also adds a reference to memory owned by `gi` to `ids`, therefore `ids`
 * should be freed before `gi`.
 */
void run_groupby_match(int groupby_idx,
                       state_t *s_in, groupby_info_t *gi,
                       id_value_t *id_tuples, int *param_ids,
                       state_t *s_out, results_t *r_out,
                       ctx_t *ctx, kvids_t *ids)
{

    if (!s_in) {
        match_trail_init(s_out);
    } else {
        memcpy(s_out, s_in, sizeof(state_t));
    }

    /* (Re-)initialize cursor to start from beginning of trail */

    ctx_reset_position(ctx);

    DBG_PRINTF("======== Calling match_trail() with groupby_idx = %d (val '%.*s') ========\n",
               groupby_idx,
               (int) gi->tuples[groupby_idx * gi->num_vars].len,
               gi->tuples[groupby_idx * gi->num_vars].str);

    id_value_t *tuple = &id_tuples[groupby_idx * gi->num_vars];

    for (int i = 0; i < gi->num_vars; i++) {
        DBG_PRINTF("setting var %s to %d (field id=%d)\n",
                    gi->var_names[i], tuple[i].id, param_ids[i]);

        if (gi->var_names[i][0] == '%')
            match_set_param(param_ids[i],
                            tuple[i].id,
                            ids,
                            gi->tuples[groupby_idx * gi->num_vars + i].str,
                            gi->tuples[groupby_idx * gi->num_vars + i].len);
        else if (gi->var_names[i][0] == '#')
            match_set_list_param(param_ids[i], tuple[i].id_set, ids);
        else
            CHECK(false, "bad var name %s\n", gi->var_names[i]);
    }

    match_trail(s_out, r_out, ids, ctx);

    /* print matching trails */
/*
    uint8_t hexcookie[33] = {0};
    tdb_uuid_hex(ctx->cookie, hexcookie);

    if (!match_is_zero_result(r_out)) {
        fprintf(stderr, "%s (%d)", (char *)hexcookie, ctx->num_events);
        print_trail(ctx);
        fprintf(stderr, "\n");
    }
*/
    ctx->perf_stats.match_calls++;
}

/*
 * Multi-traildb version of foreach aka groupby
 *
 * Here's roughly how it works:
 *
 * Make a list of all groupby values across all traildbs, let's say there's N of
 * them. Then run matcher for every groupby value for every trail, keeping track
 * of state between traildbs. That is now we have a vector of states of size N
 * for EVERY trail. Yeah that's a lot of memory and CPU.
 *
 * There are some optimizations to make that feasible.
 *
 * Since average trail has (hopefully) very few values that match patterns in
 * the program, most of state machines would stay in the initial, default
 * matcher state.
 *
 * Moreover, a lot of state vectors are likely to end up containing only copies
 * of the default state. So if a state vector is completely empty, we don't even
 * store it.
 *
 * There is a common use case of programs impementing patterns like
 *
 *      {field1=x} <then> {field1=y, field2=%groupby}
 *
 * Where most state vectors won't be empty since many state machines would match
 * first clause but way fewer will match the second one.
 *
 * However, in this case most states across in a state vector would still be
 * identical; therefore we can RLE-encode the vector to save memory.
 *
 * TODO: memory management for state vectors could easily be way more efficient,
 * we just need separate pools of memory per traildb. Doesn't seem to be a
 * bottlneck yet though, despite a ton of malloc calls.
 */

int run_groupby_query2(char **traildb_paths, int num_paths, groupby_info_t *gi,
                       json_object *params, results_t *results,
                       const char *filter, window_set_t *window_set, exclude_set_t *exclude_set)
{
    #ifdef _OPENMP
    size_t num_threads = omp_get_max_threads();
    fprintf(stderr, "max threads %d\n", (uint32_t) num_threads);
    #else
    size_t num_threads = 1;
    fprintf(stderr, "no openmp support, using single thread\n");
    #endif

    /*
     * Judy128 array for storing cookie state vectors across multiple
     * TrailDBs. Threads read from this array, but write into
     * thread-local output arrays. After processing a full TrailDB,
     * the output arrays are merged into this array.
     */
    struct judy_128_map *states = calloc(1, sizeof(struct judy_128_map));
    CHECK(states, "could not allocate states array\n");
    j128m_init(states);


    __uint128_t *window_ids = 0;
    uint64_t num_windows = 0;

    if (window_set)
        // This is a list of cookies, unless there's a 4th id column, in which case it's a list of ids.
        window_ids = window_set_get_ids(window_set, &num_windows);

    /*
     * For each OpenMP thread, we keep a separate results
     * structure. After running through all TrailDBs, we can safely
     * merge them as all datatypes are monoids (ie. sets, counters).
     *
     * In case OpenMP decides to use less threads than
     * omp_get_max_threads(), we'll allocate too much space, but the
     * merge will still work correctly.
     */

    results_t **thread_results = calloc(num_threads, sizeof(results_t*));
    CHECK(thread_results, "Could not allocate thread_results\n");

    for (int t = 0; t < num_threads; t++) {
        thread_results[t] = (results_t *) calloc(gi->merge_results ? 1 : gi->num_tuples, sizeof(results_t));
        CHECK(thread_results[t], "could not allocate thread_results[%d]\n", t);
    }


    uint64_t min_ts = 0;

    for (int di = 0; di < num_paths; di++) {
        uint64_t tstart = (uint32_t) time(NULL);
        const char *traildb_path = traildb_paths[di];
        fprintf(stderr, "Opening traildb %s\n", traildb_path);

        perf_stats_t db_perf_stats = {0};

        uint64_t num_windows_applied = 0;
        uint64_t num_trails_done_global = 0;
        uint64_t state_size_global = 0;

        /* Anything in the next block is executed in parallel by all threads */
        #pragma omp parallel
        {
        #ifdef _OPENMP
        uint32_t tid = omp_get_thread_num();
        #else
        uint32_t tid = 0;
        #endif


        db_t db;
        db_open(&db, traildb_path, filter);

        /* field ids */
        int field_ids[gi->num_vars];
        int param_ids[gi->num_vars];

        for (int j = 0; j < gi->num_vars; j++) {
            tdb_field groupby_field_id = -1;

            if (gi->var_fields[j]) {
                tdb_error res = tdb_get_field(db.db, gi->var_fields[j],
                                              &groupby_field_id);

                if (res) {
                    fprintf(stderr, "WARNING: groupby field %s is not defined for this traildb: %s %d\n",
                    gi->var_fields[j], traildb_path, groupby_field_id);
                    groupby_field_id = -1;
                }
            }
            field_ids[j] = groupby_field_id;
            param_ids[j] = match_get_param_id(gi->var_names[j]);
        }

        /* Translate foreach values (tuples) to ids specific to this traildb */
        id_value_t *id_tuples = groupby_ids_create(gi, &db);


        /*
         * Thread-local states array. Only used for writing the new
         * state. The previous state is read from the global states
         * array.
         */
        struct judy_128_map *local_states = calloc(1, sizeof(struct judy_128_map));
        CHECK(local_states, "could not allocate local_states\n");
        j128m_init(local_states);

        struct judy_128_map *local_empty_states = calloc(1, sizeof(struct judy_128_map));
        CHECK(local_empty_states, "could not allocate local_empty_states\n");
        j128m_init(local_empty_states);

        /*
         * Create an index mapping db-specific value id to foreach tuple.
         */
        vti_index_t vti;
        vti_index_create(&vti, gi, id_tuples, db.db);

        /*
         * Create a "cursor".
         */
        ctx_t ctx;
        ctx_init(&ctx, &db);

        statevec_constructor_t out_svc = {0};

        kvids_t ids;
        match_db_init(&ids, &db);
        set_params_from_json(params, &ids, &db);

        struct timeval tval1;
        gettimeofday(&tval1, NULL);

        uint64_t num_trails_done = 0;
        uint64_t state_size = 0;

        fprintf(stderr, "Opening traildb took %" PRIu64 " seconds (tid=%d)\n", time(NULL) - tstart, tid);

        uint64_t num_trails = 0;

        /*
         * If we have a set of timestamp filters, loop over cookies in the
         * filter, not the traildb, assuming that the number of cookies in the
         * filter is way smaller than the number of cookies in the traildb.
         *
         * It could be smarter than that and switch between looping over filter
         * vs looping over traildb depending on their actual sizes.
         */
        if (window_set)
            num_trails = num_windows;
        else
            num_trails = tdb_num_trails(db.db);

        #pragma omp for schedule(static)
        for (uint64_t i = 0; i < num_trails; i++) {
            const uint8_t *cookie;
            __uint128_t id = 0;

            uint64_t trail_id = 0;

            uint64_t window_start = 0;
            uint64_t window_end = 0;

            if (window_set) {
                // Convert the id -> cookie (simply returns the cookie if the last id column is not present).
                __uint128_t out_cookie = 0;

                window_set_id_to_cookie(window_set, (uint8_t *)&window_ids[i], &out_cookie);
                cookie = (uint8_t *)&out_cookie;
                id = window_ids[i];

                if (tdb_get_trail_id(db.db, cookie, &trail_id) != 0)
                    continue; /* cookie not found in the traildb */

                // We need to use the original id type (be it an id or cookie) to look up the start/end times, because
                // that id type is what's stored in the maps.
                window_set_get(window_set, (uint8_t *)&window_ids[i], &window_start, &window_end);
            } else {
                trail_id = i;
                cookie = tdb_get_uuid(db.db, trail_id);
                id = *(__uint128_t *)cookie;
            }

            if (exclude_set && exclude_set_contains(exclude_set, cookie))
                continue; // If the uuid is in the exclude_set skip its trail

            window_start = (window_start < min_ts) ? min_ts : window_start;
            ctx_read_trail(&ctx, trail_id, id, window_start, window_end);

            /*
             * Get state vector for this cookie from global input
             * array
             */
            PWord_t pv;

            #pragma omp critical
            {
            pv = j128m_get(states, *(__uint128_t *)cookie);
            }

            statevec_t *in_sv = pv ? *(statevec_t **)pv : NULL;
            statevec_iterator_t svi;
            sv_iterate_start(in_sv, &svi);
            sv_create(&out_svc, gi->num_tuples);


            /*
             *   Note that match_trail has no side effects.
             *
             *   match_trail: G, S, T -> (S', R)
             *
             *   G is group variable value
             *   S is initial state
             *   T is trail
             *   S' is result state
             *   R  is result (assuming results are additive)
             *
             *   Here we have a vector of initial states S, each corresponding
             *   to one foreach item, and a trail T. We want to produce a vector
             *   of final states S' and update results (R) for every foreach
             *   item.
             *
             *   Basic way of doing this would be to apply match_trail to
             *   every pair of (g,S) and T in a loop. That's pretty expensive
             *   but there are some optimizations that can be applied here.
             *
             *   First, if we somehow know that for a given value of S and T
             *   match_trail doesn't depend on G at all, we can only evaluate it
             *   once for all states in the initial vector that are equal to S.
             *
             *   Second, if match_trail does depend on both G and S, we can look
             *   into T and see that since match_trail only uses foreach values
             *   for equal/not equal comparisons, number of execution paths and
             *   therefore distinct outcomes is limited by N+1 when N is number
             *   of distinct values of G within T.
             */


            /*
             * Distinct_vals holds information about distinct foreach values
             * within current trail, initialized lazily
             */
            bitvec_t distinct_vals;
            bool got_distinct_vals = false;

            for (int j = 0; j < gi->num_tuples; /**/) {
                DBG_PRINTF("============== TUPLE %d / %d ==============\n", j,
                           gi->num_tuples);

                state_t st;
                /* get initial state for current foreach value */
                state_t *pstate = &st;

                int num_eq_states; /* number of identical states in a row */
                state_t *saved_state = sv_iterate_next_edge(&svi, &num_eq_states);
                if (!saved_state && (num_eq_states == -1))
                    num_eq_states = gi->num_tuples - j;

                results_t r = {0};
                run_groupby_match(j,
                                  saved_state, gi,
                                  id_tuples, param_ids,
                                  &st, &r,
                                  &ctx, &ids);

                /*
                 * If merge_results is set, we can don't have to produce a separate result
                 * object for every foreach iteration.
                 */
                results_t *output_result = &thread_results[tid][gi->merge_results?0:j];

                if (!(ctx.stats & GROUPBY_USED)) {
                    sv_append(&out_svc, pstate, num_eq_states);

                    CHECK(num_eq_states + j <= gi->num_tuples, "num_eq_states: %d j: %d\n", num_eq_states, j);
                    add_results_vec(output_result, num_eq_states, &r);

                    j += num_eq_states;
                    ctx.perf_stats.early_breaks++;
                    DBG_PRINTF("====================== early break, j=%d\n", j);
                } else {
                    /* store result we computed above */
                    match_add_results(output_result, &r);
                    sv_append(&out_svc, pstate, 1);
                    j++;

                    /*
                     * Now we know GROUPBY var value is really used when computing
                     * next state for this (S,T) pair.
                     *
                     * We need to run matcher for all distinct values of foreach
                     * variable within the trail plus run it once for the rest.
                     */
                    int next_diff_state = j + num_eq_states - 1;
                    DBG_PRINTF("next diff state %d\n", next_diff_state);

                    /* compute distinct values if we haven't yet done this for current trail */
                    if (!got_distinct_vals) {
                        distinct_vals_get_multi(&ctx, gi->num_vars,
                                                field_ids, &vti, &distinct_vals);
                        got_distinct_vals = true;
                    }

                    /*
                     * Memoized result and final state for foreach values that
                     * do not appear in current in trail (computed below).
                     */
                    results_t ndr = {0};
                    state_t nds;
                    bool got_ndr = false;

                    for (int k = j; k < next_diff_state; /**/) {

                        int ndn = non_distinct_series(k, next_diff_state,
                                                      &distinct_vals);

                        /*
                         * If merge_results is set, we can don't have to produce a separate result
                         * object for every foreach iteration.
                         */
                        results_t *output_result = &thread_results[tid][gi->merge_results?0:k];

                        /*
                         * k happens to appear in the trail, gotta run match
                         * for that value
                         */
                        if (ndn == 0) {
                            results_t r = {0};
                            run_groupby_match(k,
                                              saved_state, gi,
                                              id_tuples, param_ids,
                                              &st, &r,
                                              &ctx, &ids);

                            match_add_results(output_result, &r);
                            sv_append(&out_svc, &st, 1);
                            match_free_results(&r);
                            k++;
                        } else {
                            /*
                             * And here we have a series of groupby values that
                             * do not appear in the trail, and have same current
                             * state S. Therefore it is enough to run
                             * match_trail only once for these.
                             */
                            if (!got_ndr) {
                                run_groupby_match(k,
                                                  saved_state, gi,
                                                  id_tuples, param_ids,
                                                  &nds, &ndr,
                                                  &ctx, &ids);

                                got_ndr = true;
                            }

                            CHECK(k + ndn <= gi->num_tuples,
                                  "trail: %" PRIu64 " k: %d ndn: %d groupby_cardinality: %d next_diff_state: %d\n",
                                  ctx.trail_id, k, ndn, gi->num_tuples, next_diff_state);

                            add_results_vec(output_result, ndn, &ndr);
                            sv_append(&out_svc, &nds, ndn);
                            k += ndn;
                        }
                    }

                    DBG_PRINTF("==== next diff state %d\n", next_diff_state);
                    j = next_diff_state;
                    match_free_results(&ndr);
                }

                match_free_results(&r);
            }

            if (got_distinct_vals)
                distinct_vals_free(&distinct_vals);

            num_trails_done++;
            if (num_trails_done % 1000000 == 0) {
                struct timeval tval2, tval_diff;
                gettimeofday(&tval2, NULL);
                /* Lifted from sys/time.h */
                tval_diff.tv_sec = tval2.tv_sec - tval1.tv_sec;
                tval_diff.tv_usec = tval2.tv_usec - tval1.tv_usec;
                if (tval_diff.tv_usec < 0) {
                    --tval_diff.tv_sec;
                    tval_diff.tv_usec += 1000000;
                }

                fprintf(stderr, "%ld.%03ld s per 1M cookies, %.1f match calls per cookie (%" PRIu64 "), %" PRIu64 " times groupby not used, %.1f bytes of state per cookie, thread %d\n",
                        (long int)tval_diff.tv_sec, (long int)tval_diff.tv_usec,
                        ctx.perf_stats.match_calls/100000.,
                        ctx.perf_stats.match_calls,
                        ctx.perf_stats.early_breaks,
                        (double)state_size / num_trails_done,
                        tid);

                tval1 = tval2;

                #pragma omp atomic
                db_perf_stats.match_calls += ctx.perf_stats.match_calls;

                ctx.perf_stats.match_calls = 0;
                ctx.perf_stats.early_breaks = 0;
            }


            uint64_t state_vec_size = 0;

            statevec_t *out_sv = sv_finish(&out_svc, &state_vec_size);

            state_size += state_vec_size;
            /*
             * Insert state vector into thread-local states array if
             * not empty.
             */
            if (out_sv) {
                pv = j128m_insert(local_states, *(__uint128_t *)cookie);
                *(statevec_t **)pv = out_sv;
            } else {
                pv = j128m_insert(local_empty_states, *(__uint128_t *)cookie);
                *pv = 1;
            }
        }


        sv_free_constructor(&out_svc);
        match_free_params(&ids);
        vti_index_free(&vti);
        ctx_free(&ctx);
        /* groupby_ids_free(gi, id_tuples); */

        min_ts = tdb_max_timestamp(db.db);

        db_close(&db);

        /*
         * Wait for all threads to finish the loop before we start
         * modifying the global states Judy array
         */
        #pragma omp barrier

        /*
         * Merge thread-local states into global states array,
         * used for reading states in the next TrailDB
         */
        #pragma omp critical
        {
            __uint128_t idx = 0;
            PWord_t pv = NULL;
            j128m_find(local_states, &pv, &idx);
            while (pv != NULL)
            {
                PWord_t global_pv = j128m_insert(states, idx);
                CHECK(global_pv, "could not insert into states array\n");
                *global_pv = *pv;
                j128m_next(local_states, &pv, &idx);
            }
            j128m_free(local_states);
            free(local_states);

            /* delete stuff */
            j128m_find(local_empty_states, &pv, &idx);
            while (pv != NULL)
            {
                j128m_del(states, idx);
                j128m_next(local_empty_states, &pv, &idx);
            }
            j128m_free(local_empty_states);
            free(local_empty_states);

            db_perf_stats.match_calls += ctx.perf_stats.match_calls;

            num_trails_done_global += num_trails_done;

            state_size_global += state_size;

        }


        } // omp parallel

        uint32_t tend = (uint32_t) time(NULL);

        uint64_t num_states = j128m_num_keys(states);
        fprintf(stderr, "done processing traildb %s, " \
                        "%" PRIu64 "s wallclock, " \
                        "%" PRIu64 " state vectors, " \
                        "%" PRIu64 " match calls, " \
                        "%" PRIu64 " windows applied, " \
                        "to %" PRIu64 " cookies, " \
                        "%" PRIu64 " MiB state size\n",
                traildb_path,
                (tend - tstart),
                num_states,
                db_perf_stats.match_calls,
                num_windows_applied,
                num_trails_done_global,
                state_size_global / (1024*1024));
    }


    time_t tstart = time(NULL);

    /*
     * Merge thread results into output results
     */
    int num_results = gi->merge_results ? 1 : gi->num_tuples;

    for (int t = 0; t < num_threads; t++) {
        for (uint64_t j = 0; j < num_results; j++) {
            if (!match_is_zero_result(&thread_results[t][j]))
                match_add_results(&results[j], &thread_results[t][j]);
        }
        free(thread_results[t]);
    }
    free(thread_results);


    time_t tend = time(NULL);

    fprintf(stderr, "Merging thread results took %ld\n", (tend - tstart));


    /*
     * Finalize open states for cookies that have unfinished state but they
     * were not in the last traildb.
     */


    tstart = time(NULL);

    __uint128_t idx = 0;
    int nfinalized = 0;
    PWord_t pv = NULL;
    j128m_find(states, &pv, &idx);
    while (pv != NULL)
        {
        statevec_iterator_t svi;
        sv_iterate_start(*(statevec_t **)pv, &svi);

        for (int j = 0; j < gi->num_tuples; /**/) {
            int num_eq_states;

            /* Get next series of equal states from state vector,
               we only need to run matcher once for them as
               results are guaranteed to be the same.
            */
            state_t *pstate = sv_iterate_next_edge(&svi, &num_eq_states);
            if ((pstate == NULL) && (num_eq_states == -1))
                num_eq_states = gi->num_tuples - j;

            results_t r = {0};

            /* We only need to run matcher for non-initial states. */
            if (pstate && !match_is_initial_state(pstate)) {
                uint8_t cookie[16] = {0};
                memcpy(cookie, &idx, 16);
                match_timestamp_only(MAX_TIMESTAMP, pstate, &r, cookie);
                nfinalized++;
            }

            results_t *output_result = gi->merge_results ? &results[0] : &results[j];
            add_results_vec(output_result, num_eq_states, &r);
            match_free_results(&r);

            j += num_eq_states;
            CHECK(j <= gi->num_tuples,
                "j==groupby_cardinality num_eq_states = %d", num_eq_states);
        }
        sv_free(*(statevec_t **)pv);
        j128m_next(states, &pv, &idx);
    }
    j128m_free(states);

    free(window_ids);

    tend = time(NULL);
    fprintf(stderr, "finalizing states took %ld\n", tend-tstart);

    return 0;
}

void mk_groupby_info(groupby_info_t *gi, json_object *params,
                     char **traildb_paths, int num_paths)
{
    gi->num_vars = match_num_groupby_vars;
    gi->merge_results = match_merge_results;

    if (gi->num_vars) {
        gi->var_names = match_groupby_vars;
        gi->var_fields = malloc(sizeof(char *) * gi->num_vars);
    } else {
        gi->var_fields = NULL;
        gi->var_names = NULL;
        gi->num_tuples = 1;
        gi->tuples = calloc(1, sizeof(string_val_t));
        return;
    }

    for (int i = 0; i < gi->num_vars; i++) {
        int param_id = match_get_param_id(gi->var_names[i]);
        gi->var_fields[i] = match_get_param_field(param_id);
    }

    char *array_param = match_groupby_array_param;

    if (array_param == NULL) {
        CHECK(gi->num_vars == 1,
              "number of groupby vars must be 1 if groupby array is implicit\n");

        int param_id = match_get_param_id(match_groupby_vars[0]);
        char *param_field = match_get_param_field(param_id);
        gi->tuples = get_lexicon(traildb_paths, num_paths, param_field,
                                 &gi->num_tuples);
    } else {
        /*
         * Array parameter is a list of tuples, where each element is either
         * string or a set (array) of strings
         */
        json_object *val;
        json_bool found = json_object_object_get_ex(params, array_param, &val);

        CHECK(found,
              "can't find parameter value for %s\n", array_param);
        CHECK(array_param[0] == '@',
              "parameter name must start with @: %s\n", array_param);
        CHECK(json_object_get_type(val) == json_type_array,
              "%s is expected to be an array", array_param)

        int num_tuples = json_object_array_length(val);
        string_val_t *tuples = malloc(sizeof(string_val_t) * num_tuples * gi->num_vars);

        /* for each tuple */
        for (int j = 0; j < num_tuples; j++) {
            json_object *jtuple = json_object_array_get_idx(val, j);
            CHECK(json_object_get_type(jtuple) == json_type_array,
                  "%s is expected to be an array of arrays", array_param);
            CHECK(json_object_array_length(jtuple) == gi->num_vars,
                  "each element of %s is supposed to be a tuple of %d not %d",
                  array_param, gi->num_vars, json_object_array_length(jtuple));

            /* for each item in this tuple */
            for (int f = 0; f < gi->num_vars; f++) {
                json_object *jitem = json_object_array_get_idx(jtuple, f);

                /* item is a string */
                if (gi->var_names[f][0] == '%') {
                    CHECK(json_object_get_type(jitem) == json_type_string,
                          "foreach item %d tuple element %d is expected to be a string since it binds to variable %s\n",
                          j, f, gi->var_names[f]);

                    const char *str_value = json_object_get_string(jitem);
                    size_t str_len = json_object_get_string_len(jitem);

                    tuples[j * gi->num_vars + f].len = MIN((uint64_t) str_len, MAX_STRING_LEN);
                    tuples[j * gi->num_vars + f].str = strndup(str_value, MAX_STRING_LEN);

                } else
                /* item is an array */
                if (gi->var_names[f][0] == '#') {
                    CHECK(json_object_get_type(jitem) == json_type_array,
                         "foreach item %d tuple element %d is expected to be an array since it binds to variable %s\n",
                         j, f, gi->var_names[f]);

                    int len = json_object_array_length(jitem);

                    string_val_t *string_set = malloc(sizeof(string_val_t) * len);

                    for (int k = 0; k < len; k++) {
                        json_object *jsetitem = json_object_array_get_idx(jitem, k);
                        CHECK(json_object_get_type(jsetitem) == json_type_string,
                              "foreach item %d tuple element %d is expected to "
                              "be an array of strings since it binds to variable %s\n",
                              j, f, gi->var_names[f]);

                        const char *str_value = json_object_get_string(jsetitem);
                        size_t str_len = json_object_get_string_len(jsetitem);
                        string_set[k].len = MIN((uint64_t) str_len, MAX_STRING_LEN);
                        string_set[k].str = strndup(str_value, MAX_STRING_LEN);
                    }

                    tuples[j * gi->num_vars + f].len = len;
                    tuples[j * gi->num_vars + f].str_set = string_set;

                } else {
                    CHECK(false, "bad variable name: %s\n", gi->var_names[f]);
                }
            }
        }
        gi->tuples = tuples;
        gi->num_tuples = num_tuples;
    }
 }

void free_groupby_info(groupby_info_t *gi)
{
    for (int j = 0; j < gi->num_tuples; j++) {
        for (int f = 0; f < gi->num_vars; f++) {
            int index = j * gi->num_vars + f;
            if (gi->var_names[f][0] == '%')
                free(gi->tuples[index].str);
            else if (gi->var_names[f][0] == '#') {
                string_val_t *string_set = gi->tuples[index].str_set;
                uint64_t string_set_len = gi->tuples[index].len;
                for (int k = 0; k < string_set_len; k++) {
                    free(string_set[k].str);
                }
                free(string_set);
            }
        }
    }
    free(gi->var_fields);
    free(gi->tuples);
}


typedef enum output_format_t {
    FORMAT_JSON,
    FORMAT_MSGPACK
} output_format_t;


int run_query(char **traildb_paths, int num_paths,
              const char *params_config_file,
              const char *filter, output_format_t format,
              const char *window_file,
              const char *exclude_file)
{
    json_object *json_params = NULL;

    if (params_config_file) {
        json_params = json_object_from_file(params_config_file);
        fprintf(stderr, "using config file %s\n", params_config_file);
    }

    groupby_info_t gi = {0};
    mk_groupby_info(&gi, json_params, traildb_paths, num_paths);

    /* have a result for every groupby value */
    int num_results = gi.merge_results ? 1 : gi.num_tuples;

    results_t *results = malloc(num_results * sizeof(results_t));
    memset(results, 0, num_results * sizeof(results_t));

    window_set_t *window_set = 0;

    if (window_file) {
        window_set = parse_window_set(window_file);
    }

    exclude_set_t *exclude_set = 0;

    if (exclude_file) {
        exclude_set = parse_exclude_set(exclude_file);
    }

    run_groupby_query2(traildb_paths, num_paths, &gi, json_params,
                       results, filter, window_set, exclude_set);

    switch (format) {
        case FORMAT_JSON:
            output_json(&gi, results);
            break;
        case FORMAT_MSGPACK:
            output_msgpack(&gi, results);
            break;
        default:
            CHECK(0, "Unknown format");
    }

    free(results);

    if (json_params)
        json_object_put(json_params);

    free_groupby_info(&gi);
    free_window_set(window_set);
    free_exclude_set(exclude_set);
    return 0;
}


int parse_args(int argc, char **argv,
               char **params_config_file,
               char **filter,
               char **format,
               char **window_file,
               char **exclude_file)
{
    *params_config_file = 0;
    *window_file = 0;
    *exclude_file = 0;
    *filter = 0;
    *format = 0;

    while (1) {
        int c;
        int option_index = 0;
        static struct option long_options[] = {
            {"params",    required_argument, 0,   'p' },
            {"output-format", required_argument, 0,   'o' },
            {"filter",    required_argument, 0,   'f' },
            {"window-file",required_argument, 0,   'w' },
            {"exclude-file",required_argument, 0,   'e' },
            {0,           0,                 0,    0 }
        };

        c = getopt_long(argc, argv, "",
                        long_options, &option_index);
        if (c == -1)
            break;

      switch (c) {
          case 'p': *params_config_file = optarg; break;
          case 'f': *filter = optarg; break;
          case 'w': *window_file = optarg; break;
          case 'e': *exclude_file = optarg; break;
          case 'o': *format = optarg; break;
      }
    }
    CHECK(optind < argc, "required: traildb path");

    return argc - optind;
}

int run_tests();

output_format_t parse_format(char *format) {
    if (format == NULL)
        return FORMAT_JSON;
    if (strcmp(format, "msgpack") == 0)
        return FORMAT_MSGPACK;
    if (strcmp(format, "json") == 0)
        return FORMAT_JSON;

    CHECK(0, "Incorrect format %s", format);
}

__attribute__((weak))
void finalize() {
    /* do nothing, this function can be overriden in external module */
}

__attribute__((weak))
void initialize() {
    /* do nothing, this function can be overriden in external module */
}

int main(int argc, char **argv)
{
    char *params_config_file, *filter, *format, *window_file, *exclude_file;

    int num_dbs = parse_args(argc, argv,
                             &params_config_file,
                             &filter, &format, &window_file, &exclude_file);

    if (num_dbs == 0) {
        fprintf(stderr, "usage: %s TRAILDB_PATH [groupby FIELD]\n", argv[0]);
        return 1;
    }

    if (num_dbs > 1 && !match_no_rewind()) {
        fprintf(stderr, "programs using rewind (restart-from-start) are currently not supported with multiple traildbs\n");
        return 1;
    }
    initialize();
    run_query(&argv[argc-num_dbs],
              num_dbs,
              params_config_file,
              filter,
              parse_format(format),
              window_file,
              exclude_file);
    finalize();
    return 0;
}
