#!/bin/sh
cd "$(dirname "$0")"

p1=`find sansa-pkg-parent/sansa-pkg-deb-cli/target | grep '\.deb$'`

sudo dpkg -i "$p1"

