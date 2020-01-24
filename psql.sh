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

HOST=`printenv "DB_${ENV}_HOST"`
HOST="${HOST:-$DB_HOST}"

PORT=`printenv "DB_${ENV}_PORT"`
PORT="${PORT:-$DB_PORT}"

USER=`printenv "DB_${ENV}_USER"`
USER="${USER:-$DB_USER}"

PASSWORD=`printenv "DB_${ENV}_PASSWORD"`
PASSWORD="${PASSWORD:-$DB_PASSWORD}"

NAME=`printenv "DB_${ENV}_NAME"`
NAME="${NAME:-$DB_NAME}"

psql "postgres://${USER}:${PASSWORD}@${HOST}:${PORT}/${NAME}" "$@"
