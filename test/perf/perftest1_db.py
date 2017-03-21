from __future__ import print_function
import traildb
import sys
import json

if __name__ == '__main__':
    tdb_paths = sys.argv[1:]

    ofields = ['advertisable_eid', 'segment_eid']
    ncookies = 100000

    nevents = 200

    res = {"":0}
    for ndb, tdb_path in enumerate(tdb_paths):
        t = traildb.TrailDBConstructor(tdb_path, ofields=ofields)
        adv_eids = []

        print("generating %s" % tdb_path, file=sys.stderr)

        for i in range(ncookies):
            base_ts = 1000000 + 100000 * ndb

            cookie_hex = str(i).encode('hex').ljust(32, '0')
            base = i + 1

            for j in range(nevents / len(tdb_paths)):
                seg_eid = (base % 100) + 1
                adv_eid = str(j % seg_eid)
                t.add(cookie_hex, base_ts + j, [adv_eid, str(seg_eid)])

                res[adv_eid] = res.get(adv_eid, 0) + 1

            if i % 1000 == 0:
                print("%d%%" % (i * 100 / ncookies), file=sys.stderr, end="\r")

        print("100%", file=sys.stderr, end="\r")
        t.finalize()

        print("\ndone", file=sys.stderr)

    print(json.dumps([{'$r' : v, '%aeid' : k} for k,v in sorted(res.items())]))

