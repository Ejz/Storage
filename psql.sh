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

ENVS="$DB_ENVS"
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
