VERSION?=$(shell git describe --long --abbrev=5 --match "v*" | perl -ple 's/^v(([0-9]+)(\.[0-9]+)+)-([0-9]+)-([^-]+)/$$1-$$4-h$$5/')

INCLUDEPATH=-Ideps/msgpack-c/include -I/usr/local/include

all: src/parsetab.py lib/libtrck.a bin/gettrail bin/gettrail_tdb bin/gettrail_print

.PHONY: clean install all

clean:
	rm src/out_*.c src/out_*.h src/parsetab.py lib/* || true

src/parsetab.py: src/trparser.py
	cd src && python trparser.py gen

prefix?=/
exec_prefix?=$(prefix)
datarootdir=/usr/share
bindir=/usr/local/bin
includedir?=/usr/include
libdir?=/usr/lib

install: msgpack all
	install -m 0755 -d $(datarootdir)/trck/src $(datarootdir)/trck/bin $(datarootdir)/trck/lib
	install -m 0755 -d $(includedir)/xxhash
	install -m 0644 -t $(datarootdir)/trck/src/ src/*.c src/*.py src/*.h
	install -m 0644 -t $(datarootdir)/trck/lib/ lib/*
	install -m 0644 -t $(datarootdir)/trck/bin/ bin/*
	install -m 0644 -t $(includedir)/xxhash/ src/xxhash/*.h
	install -m 0644 -t $(includedir) deps/traildb/src/*.h
	echo 'exec python $(addprefix $(datarootdir), /trck/bin/trck) $$@' >$(addprefix $(bindir), /trck)
	chmod +x $(addprefix $(bindir), /trck)
	#cp bin/gettrail bin/gettrail_tdb $(bindir)/

CSRCS = foreach_util.c mempool.c traildb_filter.c distinct.c utf8_check.c results_json.c results_msgpack.c utils.c judy_128_map.c window_set.c ctx.c db.c hyperloglog.c xxhash/xxhash.c judy_str_map.c
COBJS  = $(addprefix lib/, $(notdir $(patsubst %.c,%.o,$(CSRCS))))

msgpack:
	cd deps/msgpack-c && cmake . && make && make install

lib/xxhash.o: src/xxhash/xxhash.c
	$(CC) -c -std=c11 -O3 -g $(INCLUDEPATH) $(CFLAGS) -o $@ $^ $(CLIBS)

lib/%.o: src/%.c
	$(CC) -c -std=c11 -O3 -g $(INCLUDEPATH) $(CFLAGS) -o $@ $^ $(CLIBS)

lib/libtrck.a: $(COBJS)
	$(AR) -ruvs $@ $^

bin/gettrail: src/gettrail.c
		$(CC) -std=c99  -O3 -g -Wall -Wno-unused-variable -Wno-unused-label -DDEBUG=$(DEBUG) $(INCLUDEPATH) $^ -ltraildb -lJudy -lcurl -ltraildb -ljson-c -o $@

bin/gettrail_print: src/gettrail_print.c
		$(CC) -std=c99  -O3 -g -Wall -Wno-unused-variable -Wno-unused-label -DDEBUG=$(DEBUG) $(INCLUDEPATH) $^ -ltraildb -lJudy -lcurl -ltraildb -o $@

bin/gettrail_tdb: src/gettrail_tdb.c src/traildb_filter.c
	$(CC) -std=c99  -O3 -g -Wall -DDEBUG=$(DEBUG) $(INCLUDEPATH) $^ -ltraildb -lJudy -lcurl -ltraildb -ljson-c -o $@

test: all
ifdef TEST
	cd test && ./run_test.sh $(realpath $(TEST))
else
	cd test && ./run_all_tests_c.sh
endif
