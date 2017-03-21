#include <traildb.h>
#include <Judy.h>
#include <stdbool.h>
#include "fns_generated.h"
#include "fns_imported.h"
#include "foreach_util.h"
#include "distinct.h"
#include "safeio.h"
#include "ctx.h"

#if DEBUG
#define DBG_PRINTF(msg, ...) fprintf(stderr, msg, ##__VA_ARGS__);
#else
#define DBG_PRINTF(msg, ...)
#endif


#define MIN(a,b) (((a)<(b))?(a):(b))


void distinct_vals_get_multi(ctx_t *ctx, int num_fields,
                             int *field_ids, vti_index_t *id_map,
                             bitvec_t *out_bitvec)
{
    out_bitvec->judy = NULL;
    for (int i = 0; i < num_fields; i++) {
        int rc;
        int field_id = field_ids[i];

        if (!vti_index_have_field(id_map, field_id))
            continue;

        ctx_reset_position(ctx);

        if (field_id == -1) {
            continue;
        }

        /*
         * Optimization: if we already added this index set to bitvec, we can
         * skip going through it again.
         */
        int prev_val_id = -1;

        while (!ctx_end_of_trail(ctx))
        {
            int val_id = item_get_value_id(ctx_get_item(ctx), field_id);

            ctx_advance(ctx);

            if (val_id == prev_val_id)
                continue;
            else
                prev_val_id = val_id;

            /* Get indexes of foreach array elements that include this value */
            int *gindexes = vti_index_lookup(id_map, field_id, val_id);

            if (gindexes) {
                int *gi = gindexes;
                while (*gi != -1) {
                    J1S(rc, out_bitvec->judy, *gi);
                    gi++;
                }
            }
        }
    }
}

void distinct_vals_print(bitvec_t *bitvec)
{
    DBG_PRINTF("distinct vals: ");

    Word_t index = 0;
    int rc;
    J1F(rc, bitvec->judy, index);
    while (rc) {
        DBG_PRINTF(" %lu", index);
        J1N(rc, bitvec->judy, index);
    }
    DBG_PRINTF("\n");
}

int non_distinct_series(int val, int limit, bitvec_t *bitvec)
{
    int rc;

    Word_t index = val;
    J1F(rc, bitvec->judy, index);
    if (!rc) {
        return limit-val;
    } else {
        return MIN(index, limit) - val;
    }
}

void distinct_vals_free(bitvec_t *bitvec)
{
    int rc;
    if (bitvec->judy) {
       J1FA(rc, bitvec->judy);
    }
    bitvec->judy = NULL;
}
