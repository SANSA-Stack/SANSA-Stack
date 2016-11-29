#!/bin/bash
# Assumption: SPARK_HOME is set
# Task is deployed locally on 4 cores
# Input: N-Triple file 
# Output: inferred graph


usage()
{
cat << EOF
usage: $0 options

This script runs the RDFS materialization locally.

OPTIONS:
   -h      Show this message
   -i      Input file in N-Triple syntax
   -o      Output directory
EOF
}

INPUT=
OUTPUT=
while getopts “hi:o:” OPTION
do
 case $OPTION in
         h)
             usage
             exit 1
             ;;
         i)
             INPUT=$OPTARG
             ;;
         o)
             OUTPUT=$OPTARG
             ;;
         ?)
             usage
             exit
             ;;
     esac
done

if [[ -z $INPUT ]] || [[ -z $OUTPUT ]]
then 
     usage
     echo "empty"
     exit 1
fi

$SPARK_HOME/bin/spark-submit \
  --class net.sansa_stack.inference.spark.RDFGraphMaterializer \
  --master "local[4]" \
  /home/me/work/projects/scala/SANSA-Inference/sansa-inference-spark/target/uber-sansa-inference-spark-0.1-SNAPSHOT.jar \
  $INPUT $OUTPUT

