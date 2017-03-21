#!/bin/bash
set -e -o pipefail

export LD_LIBRARY_PATH=deps/traildb/lib

TMP_PATH=/tmp/testgettr
mkdir $TMP_PATH 2>/dev/null  || true

rm -rf $TMP_PATH/tdb{1..10} 2>/dev/null || true
rm -rf $TMP_PATH/tdbout 2>/dev/null  || true

python src/json2tdb.py $TMP_PATH/tdb1 <<END
{
    "aaaaaaaaaaaaaaaa" : [
        {"fieldA" : "foo", "fieldB" : "1", "timestamp" : 10000},
        {"fieldA" : "foo", "fieldB" : "2", "timestamp" : 10001}
    ],
    "bbbbbbbbbbbbbbbb" : [
        {"fieldA" : "bzz", "fieldB" : "3", "timestamp" : 999},
        {"fieldA" : "bzz", "fieldB" : "4", "timestamp" : 10004}
    ]
}
END

python src/json2tdb.py $TMP_PATH/tdb2 <<END
{
    "aaaaaaaaaaaaaaaa" : [
        {"fieldA" : "foo", "fieldB" : "7", "timestamp" : 5000, "fieldC" : "az"},
        {"fieldA" : "foo", "fieldB" : "8", "timestamp" : 15001}
    ],
    "cccccccccccccccc" : [
        {"fieldA" : "bzz", "fieldB" : "9", "timestamp" : 999},
        {"fieldA" : "bzz", "fieldB" : "10", "timestamp" : 10004}
    ]
}
END

./bin/gettrail_tdb 61616161616161616161616161616161 $TMP_PATH/tdbout $TMP_PATH/tdb1 $TMP_PATH/tdb2

python src/tdb2json.py $TMP_PATH/tdbout

NUM_MATCHED=$(python src/tdb2json.py $TMP_PATH/tdbout | jq '.aaaaaaaaaaaaaaaa | length')

if [ $NUM_MATCHED -eq 4  ] ; then
    echo "SUCCEDED"
else
    echo "FAILED $NUM_MATCHED"
    exit 1
fi