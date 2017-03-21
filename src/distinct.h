#pragma once


/* Figure out what groupby array items appear in the trail.*/

/* Thus structure is logically a bit vector having a bit for each item in the
 * foreach array, if bitvec[i] = 1 then i-th foreach item appears in the trail
 * (as a field value for some item).
 *
 * So if we have
 *      foreach array = [A B C D E A]
 *      trail = [{field1=A} {field1=A} {field1=C} {field1=A} {field1=A}],
 *
 * Bit vector would contain [1 0 1 0 0 1].
 *
 * Groupby values always have single static field "type", so we know what field
 * to look at.
 *
 * This gets more complicated when groupby array is a list of tuples, where
 * values correspond to different fields; in that case we simply compute bit
 * vectors for every field as above and OR them.
 */
typedef struct bitvec_t {
    Pvoid_t judy;
} bitvec_t;


/* For the case when groupby array is an array of tuples.

 * Since buf contains only local value ids, id_map helps to translate these
 * to groupby array indexes that contain them.
 */
void distinct_vals_get_multi(ctx_t *ctx, int num_fields,
                             int *field_ids,
                             vti_index_t *id_map,
                             bitvec_t *out_bitvec);

/* free */
void distinct_vals_free(bitvec_t *bitvec);

/* Return the number of consecutive foreach values with id greater or equal to
 * `val` and less than `limit` that do NOT appear in the trail. May be 0 in
 * case if `val` does appear in the trail.
 *
 * E.g.
 *
 *   trail = [B Z U A A]
 *
 *   foreach values = [A B C D]
 *   bitvec = [1 1 0 0]
 *
 *   non_distinct_series(0, 4) -> 0
 *   non_distinct_series(1, 4) -> 0
 *   non_distinct_series(2, 4) -> 2
 *   non_distinct_series(3, 4) -> 1
 */
int non_distinct_series(int val, int limit, bitvec_t *bitvec);
