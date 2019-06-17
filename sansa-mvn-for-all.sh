#!/bin/bash
# This script allows running maven goals over multiple sansa maven projects
# Usage exmples: [this-script.sh] clean install deploy
# This script goes to the parent directory and then recursively searches for all
# sansa git repos.
# The keywords in the 'order' variable control in which order these repos are to be processed.
# Matching is does against the remote URL in the repo's git config - so this process is
# independent from local folder names.

order=(parent rdf owl query inference ml examples)
#order=(parent rdf owl query ml examples)


cd ..
configs=`find . -name config | grep '\.git'`

folders=()

for x in "${order[@]}"; do
  echo "$x"
  for y in $configs; do
    match=`grep -c -i "git@github.com:SANSA-Stack/.*$x.*.git" "$y"`
    if [ ! "$match" -eq "0" ]; then
      f=`dirname $(dirname "$y")`
      folders+=("$f")
      break
    fi
  done
done


echo "${folders[@]}"

for f in "${folders[@]}"; do
  (cd "$f" && git pull && mvn $@)
done

