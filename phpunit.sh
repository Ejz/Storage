#!/usr/bin/env bash

this=`readlink -fe "$0"`
this_dir=`dirname "$this"`
cd "$this_dir"

sudo=""
test `id -u` -eq 0 || sudo="sudo"
do_pull=""

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
    [ -n "$do_pull" ] && $sudo docker pull redis
    $sudo docker run --name phpunit_redis -p "$in":6380:6379 -d redis
    echo REDIS_HOST="$in" >>"$dotenv"
    echo REDIS_PORT=6380 >>"$dotenv"
    echo >>"$dotenv"
    #
    [ -n "$do_pull" ] && $sudo docker pull ejzspb/bitmap
    BITMAP_POOL="TEST0"
    echo BITMAP_POOL="$BITMAP_POOL" >>"$dotenv"
    i=0
    for BITMAP in `echo "$BITMAP_POOL" | tr ',' ' '`; do
        i=$((i + 1))
        p=$((i + 61000))
        $sudo docker run --name phpunit_bitmap_"$BITMAP" -p "$in":"$p":61000 -d ejzspb/bitmap
        echo BITMAP_"$BITMAP"_HOST="$in" >>"$dotenv"
        echo BITMAP_"$BITMAP"_PORT="$p" >>"$dotenv"
    done
    echo >>"$dotenv"
    #
    [ -n "$do_pull" ] && $sudo docker pull postgres:11
    DATABASE_POOL="TEST0,TEST1,TEST2"
    echo DATABASE_POOL="$DATABASE_POOL" >>"$dotenv"
    i=0
    for DATABASE in `echo "$DATABASE_POOL" | tr ',' ' '`; do
        i=$((i + 1))
        p=$((i + 5432))
        $sudo docker run --name phpunit_database_"$DATABASE" -p "$in":"$p":5432 \
            -e POSTGRES_PASSWORD=1 -d postgres:11
        echo DATABASE_"$DATABASE"_HOST="$in" >>"$dotenv"
        echo DATABASE_"$DATABASE"_PORT="$p" >>"$dotenv"
        echo DATABASE_"$DATABASE"_USER=postgres >>"$dotenv"
        echo DATABASE_"$DATABASE"_NAME=postgres >>"$dotenv"
        echo DATABASE_"$DATABASE"_PASSWORD=1 >>"$dotenv"
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
