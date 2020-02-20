#!/usr/bin/env bash

this=`readlink -fe "$0"`
this_dir=`dirname "$this"`
cd "$this_dir"

if [ "$1" -a -f "$1" ]; then
    export $(cat "$1" | xargs)
    shift
elif [ -f ".env" ]; then
    export $(cat .env | xargs)
elif [ -f ".env.phpunit" ]; then
    export $(cat .env.phpunit | xargs)
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

ENVS="$BITMAP_ENVS"
ENVS=`echo "$ENVS" | tr ',' ' '`
ENVS=($ENVS)
ENV="$1"
if [ -z "$ENV" ]; then
    ENV="0"
else
    shift
fi

if [ -n "$ENV" -a "$ENV" -eq "$ENV" ] 2>/dev/null; then
    n="$ENV"
    ENV="${ENVS[$n]}"
fi

ENV=${ENV^^}

if ! in_array "$ENV" "${ENVS[@]}"; then
    echo 1>&2 "ENV NOT FOUND!"
    exit 1
fi

HOST=`printenv "BITMAP_${ENV}_HOST"`
HOST="${HOST:-$BITMAP_HOST}"

PORT=`printenv "BITMAP_${ENV}_PORT"`
PORT="${PORT:-$BITMAP_PORT}"

redis-cli -h "$HOST" -p "$PORT" "$@"
