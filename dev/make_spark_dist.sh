#!/bin/bash

./dev/mvn_install_stack_spark.sh

cd sansa-stack/sansa-stack-spark

mvn -Pdist package -Dskip -DskipTests