#!/bin/sh
cd "$(dirname "$0")"

p1=`find sansa-debian-spark-cli/target | grep '\.deb$'`

sudo dpkg -i "$p1"

