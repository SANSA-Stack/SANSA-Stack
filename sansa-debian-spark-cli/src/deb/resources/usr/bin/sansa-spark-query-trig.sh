#!/bin/sh

#WORK_DIR=`dirname "$0"`

LIB_DIR="/usr/share/lib/sansa-spark-cli/"
MAIN_CLASS="net.sansa_stack.spark.cli.main.MainCliSansaSpark"

#java -cp "$LIB_DIR:$LIB_DIR/lib/*" "-Dloader.main=${MAIN_CLASS}" "org.springframework.boot.loader.PropertiesLauncher" "$@"
java $JAVA_OPTS -cp "$LIB_DIR:$LIB_DIR/lib/*" "$MAIN_CLASS" "$@"

