################## WARNING UGLY UNIT TEST CODE ##############################
# msgpack and json output format are not really compatible in a sense there
# is no way to convert unambiguously convert one to another. For example, json
# we encode cookies as hex strings and they are stored as binary strings in
# MSGPACK.
#
# This script does this by applying a bunch of hacks, like treating every
# 16-byte string as cookie, and some other ugly stuff. It is only supposed to
# be used for unit tests where every JSON is an artificial toy example.
#

import re
import sys
import json
import msgpack


def to_msgpack(item):
    if isinstance(item, list):
        return map(to_msgpack, item)
    elif isinstance(item, dict):
        if 'type' in item and item['type'] == 'int':
            return item['value']
        elif 'type' in item and item['type'] == 'multiset':
            r = {}
            invlexicon = {v: k for k, v in item['lexicon'].iteritems()}
            for k, v in item['data'].iteritems():
                for i in xrange(0, len(v), 2):
                    kk, vv = v[i:(i+2)]

                    if len(k) == 16:
                        k = k.encode('hex')

                    tail = invlexicon[kk]

                    if tail:
                        tail = re.sub(',.', ',', tail)

                    if tail:
                        r[k + "," + tail] = vv
                    else:
                        r[k] = vv
            return r
        elif 'type' in item and item['type'] == 'set':
            r = []
            invlexicon = {v: k for k, v in item['lexicon'].iteritems()}
            for k, v in item['data'].iteritems():
                for kk in v:
                    if len(k) == 16:
                        k = k.encode('hex')

                    tail = invlexicon[kk]
                    if tail:
                        tail = re.sub(',.', ',', tail)

                    if tail:
                        r.append(k + "," + tail)
                    else:
                        r.append(k)
            return r
        else:
            return {k: to_msgpack(item[k]) for k in item}


if __name__:
    with open(sys.argv[1]) as f:
        m = msgpack.load(f)

    if isinstance(m, list):
        res = []
        for x in m:
            r = to_msgpack(x['result'])
            for v in x['vars']:
                r[v] = x['vars'][v]
            res.append(r)
    else:
        res = to_msgpack(m)
    # convert msgpack tuples to json format

    with open(sys.argv[2], 'w') as f:
        json.dump(res, f)
