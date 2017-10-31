#!/usr/bin/env python
# -*- encoding: utf-8 -*-
"""
# Generate test traildb
python proto-example/test.py --generate proto-example/testexample

# Compile trck script
bin/trck -c proto-example/example.tr -o matcher --proto ./proto-example/Results.proto

# Run executable
./matcher proto-example/testexample.tdb --params=proto-example/params.json --output-format proto > proto-example/results.msg

# Generate python stubs so we can dump the results
protoc --python_out=proto-example --proto_path=src --proto_path=proto-example Results.proto Tuple.proto

# Dump the results
python proto-example/test.py --dump proto-example/results.msg
"""
import argparse
import fileinput
import sys
import traildb
from uuid import uuid4


def parse_args(argv):
	parser = argparse.ArgumentParser()
	parser.add_argument("--generate", required=False, help="Generate test tdb")
	parser.add_argument("--dump", required=False, help="Dump results from file input")
	args = parser.parse_args(argv)
	return args


def main(args):
	if args.generate:
		generate(args.generate)

	if args.dump:
		dump(args.dump)


def generate(name):
	tdb_cons = traildb.TrailDBConstructor(name, ["type", "a"])
	c1 = uuid4().hex
	c2 = uuid4().hex
	c3 = uuid4().hex
	c4 = uuid4().hex

	tdb_cons.add(c1, 1, ["t1", "1"])
	tdb_cons.add(c1, 2, ["t1", "1"])
	tdb_cons.add(c1, 3, ["t2", "1"])
	tdb_cons.add(c1, 4, ["t3", "1"])
	tdb_cons.add(c1, 5, ["t1", "1"])
	tdb_cons.add(c1, 6, ["t2", "1"])

	tdb_cons.add(c2, 1, ["t1", "1"])
	tdb_cons.add(c2, 2, ["t1", "2"])
	tdb_cons.add(c2, 5, ["t1", "0"])
	tdb_cons.add(c2, 6, ["t2", "1"])
	tdb_cons.add(c2, 7, ["t4", "1"])

	tdb_cons.add(c3, 1, ["t2", "0"])
	tdb_cons.add(c3, 4, ["t1", "0"])
	tdb_cons.add(c3, 7, ["t2", "0"])

	tdb_cons.add(c4, 1, ["t1", "1"])
	tdb_cons.add(c4, 3, ["t1", "1"])
	tdb_cons.add(c4, 8, ["t2", "1"])

	tdb_cons.finalize()


def dump(name):
	import Results_pb2
	res = Results_pb2.Results()
	with open(name, "rb") as f:
		res.ParseFromString(f.read())

	for row in res.rows:
		print("a={} b={} y={} x={} z={}".format(
			row.scalar_a,
			row.scalar_b,
			row.counter_y,
			list(row.set_x),
			[z.values for z in row.set_z],
		))


if __name__ == "__main__":
	main(parse_args(sys.argv[1:]))
