#!/usr/bin/env bash

this=`readlink -fe "$0"`
this_dir=`dirname "$this"`
cd "$this_dir"

sudo=""
test `id -u` -eq 0 || sudo="sudo"

if [ "$1" -a -f "$1" ]; then
    export $(cat "$1" | grep -v '^#' | xargs)
    shift
elif [ -f ".env" ]; then
    export $(cat .env | grep -v '^#' | xargs)
elif [ -f ".env.phpunit" ]; then
    export $(cat .env.phpunit | grep -v '^#' | xargs)
fi

function in_array() {
    local what="$1"
    shift
    for elem; do
        if [ "$elem" = "$what" ]; then
            return 0
        fi
    done
    return 1
}

POOL="$BITMAP_POOL"
POOL=`echo "$POOL" | tr ',' ' '`
POOL=($POOL)
OBJ="$1"
if [ -z "$OBJ" ]; then
    OBJ="0"
else
    shift
fi

if [ -n "$OBJ" -a "$OBJ" -eq "$OBJ" ] 2>/dev/null; then
    n="$OBJ"
    OBJ="${POOL[$n]}"
fi

OBJ=${OBJ^^}

if ! in_array "$OBJ" "${POOL[@]}"; then
    echo 1>&2 "OBJ NOT FOUND!"
    exit 1
fi

"$sudo" docker run -ti --link phpunit_bitmap_"$OBJ":host \
    ejzspb/bitmap node etc/client.js 61000 host
