#!/bin/bash

layers=(rdf owl query inference datalake ml)

printf -v modules ",:sansa-%s-spark_2.12" "${layers[@]}"
modules=${modules:1}

mvn -am -DskipTests clean install -pl $modules
