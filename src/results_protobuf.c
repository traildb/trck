#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <Judy.h>
#include <traildb.h>
#include "fns_generated.h"
#include "foreach_util.h"
#include "utils.h"
#include "safeio.h"
#include "results_protobuf.h"
/*
 * This is a dummy file that provides a stub implementation of output_proto so that
 * the linker doesn't explode when the real results_protobuf.c is not provided.
 */

const int protobuf_enabled = 0;

void output_proto(groupby_info_t *gi, results_t *results) {
	// Do nothing. This should never be called
}
