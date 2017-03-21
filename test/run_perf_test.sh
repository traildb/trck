#!/bin/bash
set -e -o pipefail

FAILED=0
SUCCEEDED=0

SOURCE=perf/perftest1.tr

export PATH=../bin:$PATH

TDBS="/tmp/tdbperftest1 /tmp/tdbperftest2"

echo "Generating test traildbs... That may take a while"
python perf/perftest1_db.py $TDBS >/tmp/res2.json

BIN=/tmp/matcher

trck -c $SOURCE -o $BIN

export OMP_NUM_THREADS=2

time -p $BIN $TDBS >/tmp/res1.json

set +e
./ddiff.py /tmp/res1.json /tmp/res2.json
ERRCODE=$?
set -e
FAILED=$((FAILED+ERRCODE))

red='\033[0;31m'
NC='\033[0m' # No Color
if [ $FAILED -ne 0 ]; then
    echo -ne "${red}######## FAILED $SOURCE #######${NC}\n"
    exit $FAILED
else
    echo "######## SUCCEEDED $SOURCE #######"
fi
