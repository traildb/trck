#include <string.h>
#include <traildb.h>
#include <Judy.h>
#include <stdbool.h>

#include "fns_generated.h"
#include "fns_imported.h"

#include "match_internal.h"
#include "safeio.h"

/* call once after opening the db */
void ctx_init(ctx_t *ctx, db_t *db) {
    ctx->trail_id = 0;

    ctx->cursor = tdb_cursor_new(db->db);
    if (db->filter)
        tdb_cursor_set_event_filter(ctx->cursor, db->filter);

    ctx->event_size = tdb_num_fields(db->db) + 1;
    ctx->num_events = 0;
    ctx->cookie = 0;
    ctx->db = db;
    ctx->position = 0;
    ctx->ts_window_end = 0;
    ctx->ts_window_start = 0;

    ctx->buf = 0;
    ctx->buf_size = 0;

    memset(&ctx->perf_stats, 0, sizeof(ctx->perf_stats));
}

void ctx_free(ctx_t *ctx) {
    free(ctx->buf);
}

void ctx_read_trail(ctx_t *ctx, uint64_t trail_id, uint64_t window_start, uint64_t window_end) {
    ctx->ts_window_end = window_end;
    ctx->ts_window_start = window_start;
    ctx->trail_id = trail_id;
    ctx->cookie = tdb_get_uuid(ctx->db->db, trail_id);

    tdb_error res = tdb_get_trail(ctx->cursor, trail_id);
    CHECK(res == 0, "could not get trail %" PRIu64, trail_id);

    size_t offset = 0;

    ctx->num_events = 0;

    const tdb_event *e;
    while ((e = tdb_cursor_next(ctx->cursor))){
        uint64_t size = sizeof(tdb_event) + e->num_items * sizeof(tdb_item);
        ctx->event_size = size;

        if (ctx->ts_window_start && e->timestamp < ctx->ts_window_start)
            continue;

        if (ctx->ts_window_end && e->timestamp >= ctx->ts_window_end)
            break;

        if (ctx->buf_size == 0) {
            ctx->buf_size = 10 * size;
            ctx->buf = malloc(ctx->buf_size);
        }

        if (offset + size > ctx->buf_size) {
            ctx->buf_size = ctx->buf_size * 2;
            ctx->buf = realloc(ctx->buf, ctx->buf_size);
        }

        memcpy(&ctx->buf[offset], e, size);

        offset += size;
        ctx->num_events += 1;
    }
}


void ctx_reset_position(ctx_t *ctx) {
    ctx->position = 0;
    ctx->current_event = (tdb_event *)&ctx->buf[ctx->position * ctx->event_size];
    if (ctx->num_events == 0) ctx->current_event = NULL;
    ctx->stats = 0;
}

bool ctx_end_of_trail(ctx_t *ctx)
{
    return ctx->current_event == NULL;
}

item_t ctx_get_item(ctx_t *ctx)
{
    return ctx->current_event;
}

void ctx_update_stats(ctx_t *ctx, stats_flag flag)
{
    ctx->stats |= flag;
}

void ctx_advance(ctx_t * ctx)
{
    if (ctx->num_events == 0)
        ctx->current_event = NULL;


    while (ctx->position < ctx->num_events) {
        tdb_event *prev =(tdb_event *)&ctx->buf[ctx->position * ctx->event_size];

        ctx->position++;

        if (ctx->position < ctx->num_events) {
            tdb_event *next = (tdb_event *)&ctx->buf[ctx->position * ctx->event_size];

            /*
             * Simply a shortcut to avoid calling libc memcmp in most cases.
             * Could be replaced with better inline memcmp.
             */
            if (prev->timestamp == next->timestamp)
                if(memcmp(prev, next, sizeof(tdb_event) + sizeof(tdb_item) * prev->num_items) == 0)
                    continue;

            ctx->current_event = next;
            return;
        }
    }

    ctx->current_event = NULL;
}

int64_t ctx_get_position(ctx_t *ctx)
{
    return ctx->position;
}

uint64_t ctx_get_cookie_timestamp_filter_end(ctx_t *ctx)
{
    return ctx->ts_window_end;
}

uint64_t ctx_get_cookie_timestamp_filter_start(ctx_t *ctx)
{
    return ctx->ts_window_start;
}

void ctx_get_cookie(ctx_t *ctx, char buf[static 16])
{
    memcpy((uint8_t *)buf, ctx->cookie, 16);
}

const char *ctx_get_item_value(ctx_t *ctx, item_t item, int keyid, int *len)
{
    int valueid = item_get_value_id(item, keyid);
    if (valueid == 0)
        return "";


    uint64_t value_length;
    const char *val = tdb_get_value(ctx->db->db, keyid, valueid, &value_length);
    *len = value_length;
    return val;
}


timestamp_t item_get_timestamp(item_t item)
{
    return ((tdb_event *)item)->timestamp;
}

int item_get_value_id(item_t item, int keyid)
{
    const tdb_event *ev = item;

    CHECK(keyid == -1 || keyid <= ev->num_items, "keyid out of bounds: keyid=%d, num_items=%" PRIu64, keyid, ev->num_items);

    /* non-existent keys default to 0 */
    if (keyid == -1)
        return 0;

    return tdb_item_val(ev->items[keyid-1]);
}

bool item_is_empty(item_t item)
{
    return ((tdb_event *)item)->num_items == 0;
}


