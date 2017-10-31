import sys
import Results_pb2
# protoc --python_out=./python-gen Results.proto Tuple.proto
# python ./python-gen/test.py results.msg

res = Results_pb2.Results()
with open(sys.argv[1], "rb") as f:
	res.ParseFromString(f.read())



for row in res.rows:
	print ("a={} b={} y={} x={} z={}".format(
		row.scalar_a,
		row.scalar_b,
		row.counter_y,
		list(row.set_x),
		[z.values for z in row.set_z],
	))
