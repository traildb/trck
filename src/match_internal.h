#pragma once


/* overall performance counters */
typedef struct perf_stats_t {
    uint64_t match_calls;
    uint64_t early_breaks;
} perf_stats_t;

struct db_t {
    tdb *db;
    Pvoid_t id_lookup_table[256];
    struct tdb_event_filter *filter;
};

struct ctx_t {
    int64_t num_events;
    int64_t event_size;

    uint8_t *buf;
    size_t buf_size;

    tdb_cursor *cursor;
    uint64_t trail_id;

    tdb_event *current_event;

    int64_t position;
    int stats; /* used for jit-like optimizations */
    perf_stats_t perf_stats;
    __uint128_t cookie;
    db_t *db;

    uint64_t ts_window_start;
    uint64_t ts_window_end;
};
