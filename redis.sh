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

"$sudo" docker run -ti --link phpunit_redis:host \
    redis redis-cli -h host -p 6379 "$@"
