#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>

// Judy
#include <Judy.h>

// TrailDB
#include <traildb.h>

// trck headers
#include "fns_generated.h"
#include "foreach_util.h"
#include "utils.h"
#include "safeio.h"

// protobuf-c headers
#include "Result.pb-c.h"

// maximum string size in judy
#define MAXLINELEN 1000000

/*
Generate Result.pb-c.c
protoc --c_out=. Result.proto

# Generate trck executable (Optional)
bin/trck -c example.tr -o normal-example

# Generate source and header files for example
mkdir gen
bin/trck -c example.tr --gen-c > gen/out_traildb.c
bin/trck -c example.tr --gen-h > gen/out_traildb.h

Compile results_protobuf
gcc \
	src/results_protobuf.c \
	gen/out_traildb.c \
	Result.pb-c.c \
	src/statevec.c \
	src/match_traildb.c \
	-o protobuf-example \
	-I. \
	-Igen \
	-Isrc \
	-Ideps/traildb/src \
	-I/usr/local/include \
	-L/usr/local/lib \
	-lprotobuf-c \
	-ltraildb \
	-lJudy \
	-lcmph \
	-ljemalloc \
	-lmsgpackc \
	-ljson-c \
	-lm \
	/Users/alexholyoke/projects/trck/lib/libtrck.a

# Generate example tdb
python generate.py
tdb dump -i testexample


./protobuf-example testexample.tdb --params=params.json --output-format json


Rebuild trck
pushd deps/trck && make clean all && popd

*/

// void (*save_int)(void *, char *, int64_t)
void proto_add_int(void *p, char *name, int64_t value) {
	Trck__Result *msg = (Trck__Result *) p;
	if (!strcmp(name, "var_y")) {
		msg->var_y = value;
	}
}

// void (*save_set)(void *, char *, set_t *)
void proto_add_set(void *p, char *name, set_t *value) {
	Trck__Result *msg = (Trck__Result *) p;
	if (!strcmp(name, "var_x")) {
		uint8_t index[MAXLINELEN];
		Word_t *pv;

		msg->n_var_x = 0;
		JSLF(pv, *value, index);
		while(pv) {
			char buf[1000];
        	int len = string_tuple_to_json((char*)index, buf);
        	msg->var_x[msg->n_var_x] = malloc(sizeof(char) * len);
        	strncpy(msg->var_x[msg->n_var_x], buf, len);

        	msg->n_var_x++;

			JSLN(pv, *value, index);
		}
	}
	if (!strcmp(name, "var_z")) {
		// Skip this for now
		msg->n_var_z = 0;
	}
}

// void (*save_multiset)(void *, char *, set_t *)
void proto_add_multiset(void *p, char *name, set_t *value) {
}


// void (*save_hll)(void *, char *, hyperloglog_t *)
void proto_add_hll(void *p, char *name, hyperloglog_t *value) {
}

void output_groupby_result_proto(groupby_info_t *gi, int i, results_t *results) {

	string_val_t *tuple = &gi->tuples[i * gi->num_vars];

	Trck__Result msg = TRCK__RESULT__INIT;

	unsigned len;
	void *buf;

	/*
	required string param_a = 1;
	required string param_b = 2;
	repeated string var_x = 3;
	required int64 var_y = 4;
	repeated Tuple_z var_z = 5;
	*/

	// Input params

	// Save param_a
	msg.param_a = malloc(sizeof(char) * tuple[0].len);
	strncpy(msg.param_a, tuple[0].str, tuple[0].len);

	// Save param_b
	msg.param_b = malloc(sizeof(char) * tuple[1].len);
	strncpy(msg.param_b, tuple[1].str, tuple[1].len);

	// Output variables

	match_save_result(results, &msg, proto_add_int, proto_add_set, proto_add_multiset, proto_add_hll);

	len = trck__result__get_packed_size(&msg);
	buf = malloc(len);
	trck__result__pack(&msg, buf);
	fwrite(buf, len, 1, stdout);

	free(buf);
}

void output_proto(groupby_info_t *gi, results_t *results) {
	void *buf;
	unsigned len;

	output_groupby_result_proto(gi, 0, results);

}
