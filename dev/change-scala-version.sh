#!/bin/bash
# This script allows running maven goals over multiple sansa maven projects
# Usage exmples: [this-script.sh] clean install deploy
# This script goes to the parent directory and then recursively searches for all
# sansa git repos.
# The keywords in the 'order' variable control in which order these repos are to be processed.
# Matching is does against the remote URL in the repo's git config - so this process is
# independent from local folder names.

set -e

VALID_VERSIONS=( 2.11 2.12 )

usage() {
  echo "Usage: $(basename $0) [-h|--help] <version>
where :
  -h| --help Display this help text
  valid version values : ${VALID_VERSIONS[*]}
" 1>&2
  exit 1
}

if [[ ($# -ne 1) || ( $1 == "--help") ||  $1 == "-h" ]]; then
  usage
fi

TO_VERSION=$1

check_scala_version() {
  for i in ${VALID_VERSIONS[*]}; do [ $i = "$1" ] && return 0; done
  echo "Invalid Scala version: $1. Valid versions: ${VALID_VERSIONS[*]}" 1>&2
  exit 1
}

check_scala_version "$TO_VERSION"

if [ $TO_VERSION = "2.12" ]; then
  FROM_VERSION="2.11"
else
  FROM_VERSION="2.12"
fi

sed_i() {
  sed -e "$1" "$2" > "$2.tmp" && mv "$2.tmp" "$2"
}

export -f sed_i


#order=(parent rdf)
order=(parent rdf owl query inference ml examples)
#order=(parent rdf owl query ml examples)

BASEDIR=`pwd`
cd ..
cwd=`pwd`

configs=`find . -name config | grep '\.git'` || true

folders=()

for x in "${order[@]}"; do
  for y in $configs; do
    match=`grep -c -i "git@github.com:SANSA-Stack/.*$x.*.git" "$y"` || true
    if [ "$match" -ne 0 ]; then
      f=`dirname $(dirname "$y")`
      folders+=("$f")
      break
    fi
  done
done


echo "folders tp process: ${folders[@]}"

for f in "${folders[@]}"; do
  find "$f" -name 'pom.xml' -not -path '*target*' -print \
  -exec bash -c "sed_i 's/\(artifactId.*\)_'$FROM_VERSION'/\1_'$TO_VERSION'/g' {}" \;

  xx=$?
  test $xx -ne 0 && exit $xx
done

echo "changing scala binary version in parent"
sed_i '1,/<scala\.binary\.version>[0-9]*\.[0-9]*</s/<scala\.binary\.version>[0-9]*\.[0-9]*</<scala.binary.version>'$TO_VERSION'</' \
  "$BASEDIR/pom.xml"
