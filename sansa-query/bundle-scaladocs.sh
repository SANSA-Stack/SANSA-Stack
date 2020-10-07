#!/bin/bash

targetFolder="target/scaladocs-bundle"
mkdir -p "$targetFolder"

for srcFolder in `find . -type d -name scaladocs`; do
  moduleName=$(basename $(dirname $(dirname $(dirname "$srcFolder"))))

  if [[ "$moduleName" == "." ]]; then
    moduleName="sansa-parent"
  fi

  cp -rf "$srcFolder" "$targetFolder/$moduleName"
#  echo "$ --- $moduleName";
done

