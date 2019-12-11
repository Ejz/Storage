#!/usr/bin/env bash

this=`readlink -fe "$0"`
this_dir=`dirname "$this"`
cd "$this_dir"

test -f .env && source .env

sudo="sudo"
test `id -u` -eq 0 && sudo=""

$sudo docker rm -f postgres0 postgres1 postgres2 redis0
$sudo docker run --name postgres0 -p 0.0.0.0:"$POSTGRES0_PORT":5432 -e POSTGRES_PASSWORD="$POSTGRES0_PASSWORD" -d postgres
$sudo docker run --name postgres1 -p 0.0.0.0:"$POSTGRES1_PORT":5432 -e POSTGRES_PASSWORD="$POSTGRES1_PASSWORD" -d postgres
$sudo docker run --name postgres2 -p 0.0.0.0:"$POSTGRES2_PORT":5432 -e POSTGRES_PASSWORD="$POSTGRES2_PASSWORD" -d postgres
$sudo docker run --name redis0 -p 0.0.0.0:"$REDIS0_PORT":6379 -d redis
