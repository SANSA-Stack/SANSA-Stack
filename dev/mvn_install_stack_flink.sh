#!/bin/bash

layers=(rdf owl query inference ml)

printf -v modules ",:sansa-%s-flink_2.12" "${layers[@]}"
modules=${modules:1}

mvn -am -DskipTests clean install -pl $modules
