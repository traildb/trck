import traildb
from uuid import uuid4

def main():
	tdb_cons = traildb.TrailDBConstructor("testexample", ["type", "a"])
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


if __name__ == "__main__":
	main()