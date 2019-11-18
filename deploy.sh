#!/usr/bin/env bash

this=`readlink -fe "$0"`
this_dir=`dirname "$this"`
cd "$this_dir"

docker run -e POSTGRES_PASSWORD=password -d postgres
