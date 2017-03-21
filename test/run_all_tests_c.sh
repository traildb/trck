#!/bin/bash
set -e

FAILED=0
for x in json/test*.json tr/test*.tr; do
    if [[ $x =~ \.tr$ ]] ; then
        cat $x | awk '{ if (x) { print} ;}/-- ?unit tests ?--/{x=1}' | sed 's/^--*//' >/tmp/tests.json
        TEST=/tmp/tests.json
    else
        TEST=$x
    fi

    NUM_TESTS=$(cat $TEST | jq ".tests | length")
    TOTAL_TESTS=$((TOTAL_TESTS+NUM_TESTS))
    set +e
	./run_test.sh $x
    ERRCODE=$?
    set -e
    FAILED=$((FAILED+ERRCODE))
done
green='\033[0;32m'
red='\033[0;31m'
NC='\033[0m' # No Color

if [ $FAILED -eq 0 ]; then
    echo -ne "${green}#### $((TOTAL_TESTS-FAILED))/$TOTAL_TESTS TESTS SUCCEDED ####${NC}\n"
else
    echo -ne "${red}#### $((TOTAL_TESTS-FAILED))/$TOTAL_TESTS TESTS SUCCEDED, $FAILED TEST FAILED ####${NC}\n"
fi
exit $FAILED
