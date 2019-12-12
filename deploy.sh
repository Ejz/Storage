#!/usr/bin/env bash

this=`readlink -fe "$0"`
this_dir=`dirname "$this"`
cd "$this_dir"

test -f .env && source .env

sudo="sudo"
test `id -u` -eq 0 && sudo=""

$sudo docker rm -f postgres0 postgres1 postgres2 redis0 bitmap0
$sudo docker run --name postgres0 -p 0.0.0.0:"$POSTGRES_DB0_PORT":5432 -e POSTGRES_PASSWORD="$POSTGRES_DB0_PASSWORD" -d postgres
$sudo docker run --name postgres1 -p 0.0.0.0:"$POSTGRES_DB1_PORT":5432 -e POSTGRES_PASSWORD="$POSTGRES_DB1_PASSWORD" -d postgres
$sudo docker run --name postgres2 -p 0.0.0.0:"$POSTGRES_DB2_PORT":5432 -e POSTGRES_PASSWORD="$POSTGRES_DB2_PASSWORD" -d postgres
$sudo docker run --name redis0 -p 0.0.0.0:"$REDIS_DB0_PORT":6379 -d redis
$sudo docker run --name bitmap0 -p 0.0.0.0:"$BITMAP_DB0_PORT":61000 -d ejzspb/bitmap
