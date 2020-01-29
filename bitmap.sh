#!/usr/bin/env bash

this=`readlink -fe "$0"`
this_dir=`dirname "$this"`
cd "$this_dir"

if [ "$1" -a -f "$1" ]; then
    export $(cat "$1" | xargs)
    shift
else
    export $(cat .env | xargs)
fi

ENV="$1"
[ "$ENV" ] || exit
ENV=${ENV^^}
shift

HOST=`printenv "BITMAP_${ENV}_HOST"`
HOST="${HOST:-$BITMAP_HOST}"

PORT=`printenv "BITMAP_${ENV}_PORT"`
PORT="${PORT:-$BITMAP_PORT}"

redis-cli -h "$HOST" -p "$PORT" "$@"
