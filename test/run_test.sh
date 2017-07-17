#!/bin/bash
set -e -o pipefail

SOURCE=$1
: ${DEBUG:=0}


red='\033[0;31m'
NC='\033[0m' # No Color
export PATH=../bin:$PATH
export LD_LIBRARY_PATH=../deps/traildb/.libs
export PYTHONPATH=../deps/traildb-python/

if uname -a | grep Darwin >/dev/null ; then
    export DYLD_LIBRARY_PATH=../deps/traildb/lib
else
    export LD_LIBRARY_PATH=../deps/traildb/.libs
fi

BIN=/tmp/matcher-traildb

if [[ $SOURCE =~ \.tr$ ]] ; then
    cat $SOURCE | awk '{ if (x) { print} ;}/-- ?unit tests ?--/{x=1}' | sed 's/^--*//' >/tmp/tests.json
    TEST=/tmp/tests.json
else
    TEST=$SOURCE
fi

NUM_TESTS=$(cat $TEST | jq ".tests | length")
FAILED=0
SUCCEEDED=0

set +e
DEBUG=$DEBUG trck -c $SOURCE -o $BIN #--gen-c
ERRCODE=$?
set -e
if [ $ERRCODE -ne 0 ]  ; then
    echo -ne "${red}################# FAILED $SOURCE ###################${NC}\n"
    exit $NUM_TESTS
fi

if [ "$NUM_TESTS" == "" ]; then NUM_TESTS=0 ; fi

for ((x=0; x<$NUM_TESTS; x += 1 )) do
    DESC=$(cat $TEST | jq ".tests|.[$x]|.desc")

    echo "############ running test $x of $SOURCE #############"
    if [ "$DESC" != "null" ] ; then
        echo "### $DESC"
    fi

    GROUPBY=$(cat $TEST | jq -r ".groupby")
    if [ "$GROUPBY" == "null" ]; then
        GROUPBY_ARG=""
    else
        GROUPBY_ARG="--groupby $GROUPBY"
    fi

    PARAMS=$(cat $TEST | jq -r ".params")
    if [ "$PARAMS" != "null" ]; then
        echo "$PARAMS" >/tmp/params.json
        PARAM_ARG='--params /tmp/params.json'
    else
        if [ -f "$SOURCE.params.json" ]; then
            cp "$SOURCE.params.json" /tmp/params.json
            PARAM_ARG='--params /tmp/params.json'
        else
            PARAM_ARG=""
        fi
    fi

    if [ -f $SOURCE.window.csv ]; then
        WINDOW_FILE_ARG="--window-file $SOURCE.window.csv"
    else
        WINDOW_FILE_ARG=""
    fi

    FILTER=$(cat $TEST | jq -r ".tests | .[$x] | .filter")
    if [ "$FILTER" == "null" ]; then
        FILTER=""
    fi


    rm -rf /tmp/tdbs/
    mkdir -p /tmp/tdbs/

    NUM_DBS=$(cat $TEST | jq ".tests|.[$x]|.trails|length")

    DBS=""

    for ((d=0; d<$NUM_DBS; d += 1 )) do
        if [ $DEBUG -eq 1 ] ; then
            echo "cat $TEST | jq '.tests|.[$x]|.trails|.[$d]' | json2tdb /tmp/tdbs/tdb$d"
        fi
        cat $TEST | jq ".tests|.[$x]|.trails|.[$d]" | json2tdb /tmp/tdbs/tdb$d
        DBS="$DBS /tmp/tdbs/tdb$d"
    done

    if [ "$FORMAT" == "msgpack" ] ; then
        OUTFILE=/tmp/result.msgpack
        FMT_ARG="--output-format msgpack"
    else
        OUTFILE=/tmp/result.json
    fi

    set +e
    if [ $DEBUG -eq 1 ] ; then
        echo $BIN --filter '"$FILTER"' $PARAM_ARG $DBS
        $BIN --filter "$FILTER" $PARAM_ARG $FMT_ARG $WINDOW_FILE_ARG $DBS | tee $OUTFILE
    else
        $BIN --filter "$FILTER" $PARAM_ARG $FMT_ARG $WINDOW_FILE_ARG $DBS 2>/dev/null >$OUTFILE
    fi
    ERRCODE=$?
    set -e
    if [ $ERRCODE -ne 0 ]  ; then
        FAILED=$((FAILED+1))
        break
    fi

    if [ "$FORMAT" == "msgpack" ] ; then
        python msgpack2json.py $OUTFILE /tmp/result.json
    fi

    set +e
    cat $TEST | jq ".tests | .[$x] | .expected" | ./ddiff.py - /tmp/result.json
    ERRCODE=$?
    set -e
    if [ $ERRCODE -ne 0 ]  ; then
        echo "output: $(cat /tmp/result.json)" >&2
        FAILED=$((FAILED+1))
    fi
done



if [ $FAILED -ne 0 ]; then
    echo -ne "${red}################# FAILED $SOURCE ###################${NC}\n"
    exit $FAILED
else
    echo "################# SUCCEEDED $SOURCE ################"
fi
