#!/usr/bin/env python

import json
import sys

def obj_equals(a, b, prefixes):
    for k in b.keys() + a.keys():
        if k[0] in prefixes:
            if not ((k in a) and (k in b)):
                return False
            if isinstance(a[k], int) or isinstance(a[k], str) or isinstance(a[k], unicode):
                if a[k] != b[k]:
                    return False
            elif isinstance(a[k], list) and isinstance(b[k], list):
                if set(a[k]) != set(b[k]):
                    return False
            elif isinstance(a[k], dict) and isinstance(b[k], dict):
                return a[k] == b[k]
    return True

def find_by_key(val, array):
    for a in array:
        if obj_equals(val, a, prefixes=('#','%')):
            return a

def compare_lists(j1, j2):
    succ = True

    for v in j1:
        r = find_by_key(v, j2)
        if r is None:
            print >>sys.stderr, "not found: ", v
            succ = False
        elif obj_equals(r, v, prefixes=('$','#','&')):
            continue
        else:
            print >>sys.stderr, "expected list", json.dumps(v), "got", json.dumps(r)
            succ = False
    if not succ:
        sys.exit(1)

if __name__ == '__main__':
    if sys.argv[1] != '-':
        with open(sys.argv[1]) as f:
            j1 = json.load(f)
    else:
        j1 = json.load(sys.stdin)

    if sys.argv[2] != '-':
        with open(sys.argv[2]) as f:
            j2 = json.load(f)
    else:
        j2 = json.load(sys.stdin)

    assert(isinstance(j1, list) == isinstance(j2, list))

    if isinstance(j1, list):
        compare_lists(j1, j2)
    else:
        obj_equals(j1, j2, prefixes=('$','#','&'))
        #compare_dicts(j1, j2)
