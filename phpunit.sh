#!/usr/bin/env bash

this=`readlink -fe "$0"`
this_dir=`dirname "$this"`
cd "$this_dir"

sudo=""
test `id -u` -eq 0 || sudo="sudo"

function cleanup {
    for container in `$sudo docker ps -a | grep phpunit_ | awk '{print $1}'`; do
        $sudo docker rm -f "$container"
    done
    rm -f "$dotenv"
}

dotenv=.env.phpunit
in="127.0.0.1"
action="$1"

if [ "$action" = "begin" -o "$action" = "start" ]; then
    cleanup
    #
    $sudo docker pull redis
    $sudo docker pull ejzspb/bitmap
    $sudo docker pull postgres
    #
    $sudo docker run --name phpunit_REDIS -p "$in":6380:6379 -d redis
    echo REDIS_HOST="$in" >>"$dotenv"
    echo REDIS_PORT=6380 >>"$dotenv"
    #
    $sudo docker run --name phpunit_BITMAP -p "$in":61001:61000 -d ejzspb/bitmap
    echo BITMAP_HOST="$in" >>"$dotenv"
    echo BITMAP_PORT=61001 >>"$dotenv"
    #
    DB_ENVS="TEST0,TEST1,TEST2"
    echo DB_ENVS="$DB_ENVS" >>"$dotenv"
    i=0
    for DB in `echo "$DB_ENVS" | tr ',' ' '`; do
        i=$((i + 1))
        p=$((i + 5432))
        $sudo docker run --name phpunit_DB_"$DB" -p "$in":"$p":5432 \
            -e POSTGRES_PASSWORD=1 -d postgres
        echo DB_"$DB"_HOST="$in" >>"$dotenv"
        echo DB_"$DB"_PORT="$p" >>"$dotenv"
        echo DB_"$DB"_USER=postgres >>"$dotenv"
        echo DB_"$DB"_NAME=postgres >>"$dotenv"
        echo DB_"$DB"_PASSWORD=1 >>"$dotenv"
    done
    sleep 5
    #
    exit
fi

if [ "$action" = "end" -o "$action" = "finish" -o "$action" = "stop" ]; then
    cleanup
    exit
fi

if [ -f "$dotenv" ]; then
    export $(cat "$dotenv" | xargs)
    ./vendor/bin/phpunit "$@"
else
    echo Run: "`basename "$this"`" start
fi