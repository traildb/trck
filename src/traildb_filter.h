#pragma once

struct tdb_event_filter *traildb_compile_filter(tdb *db, const char *filter_str,
                                                int len);
