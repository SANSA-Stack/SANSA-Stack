#!/bin/bash

MAX=${1:-10}

base="test/conjure"
for i in `seq 1 $MAX`; do
  dir="$base"
#  dir="$base/dataset$i"
  mkdir -p "$dir"

  for j in `seq 1 $i`; do
    echo "<urn:s> <urn:p$j> <urn:o> ."
  done > "$base/data$i.nt"
done

