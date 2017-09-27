#pragma once

void ctx_init(ctx_t *ctx, db_t *db);
void ctx_free(ctx_t *ctx);
void ctx_read_trail(ctx_t *ctx, uint64_t trail_id, __uint128_t cookie, uint64_t window_start, uint64_t window_end);
void ctx_reset_position(ctx_t *ctx);

/*
 * See fns_imported.h for the rest of ctx_ and item_ functions
 */
