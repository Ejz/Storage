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
    $sudo docker run --name phpunit_REDIS -p "$in":6380:6379 -d redis
    echo REDIS_HOST="$in" >>"$dotenv"
    echo REDIS_PORT=6380 >>"$dotenv"
    echo >>"$dotenv"
    #
    $sudo docker pull ejzspb/bitmap
    BITMAP_ENVS="TEST0,TEST1,TEST2"
    echo BITMAP_ENVS="$BITMAP_ENVS" >>"$dotenv"
    i=0
    for BITMAP in `echo "$BITMAP_ENVS" | tr ',' ' '`; do
        i=$((i + 1))
        p=$((i + 61000))
        $sudo docker run --name phpunit_BITMAP_"$BITMAP" -p "$in":"$p":61000 -d ejzspb/bitmap
        echo BITMAP_"$BITMAP"_HOST="$in" >>"$dotenv"
        echo BITMAP_"$BITMAP"_PORT="$p" >>"$dotenv"
    done
    echo >>"$dotenv"
    #
    $sudo docker pull postgres:11
    DB_ENVS="TEST0,TEST1,TEST2"
    echo DB_ENVS="$DB_ENVS" >>"$dotenv"
    i=0
    for DB in `echo "$DB_ENVS" | tr ',' ' '`; do
        i=$((i + 1))
        p=$((i + 5432))
        $sudo docker run --name phpunit_DB_"$DB" -p "$in":"$p":5432 \
            -e POSTGRES_PASSWORD=1 -d postgres:11
        echo DB_"$DB"_HOST="$in" >>"$dotenv"
        echo DB_"$DB"_PORT="$p" >>"$dotenv"
        echo DB_"$DB"_USER=postgres >>"$dotenv"
        echo DB_"$DB"_NAME=postgres >>"$dotenv"
        echo DB_"$DB"_PASSWORD=1 >>"$dotenv"
    done
    echo >>"$dotenv"
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
