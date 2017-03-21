#pragma once

void db_open(db_t *db, const char *traildb_path, const char *filter);
void db_close(db_t *db);

/*
 * See fns_imported.h for the rest of db_ functions
 */