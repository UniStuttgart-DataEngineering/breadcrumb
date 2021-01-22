#!/usr/bin/env bash
export LIB_BASE_DIR=/data/diesterf/libs
echo $1 $2 $3 $4 $5 $6 $7 $8
spark-submit --deploy-mode cluster  --jars ${LIB_BASE_DIR}/nested-why-not-0.1-SNAPSHOT.jar --num-executors 25 --executor-memory 32G --executor-cores 2  --class de.uni_stuttgart.ipvs.provenance.evaluation.TestExecution  ${LIB_BASE_DIR}/nested-why-not-0.1-SNAPSHOT.jar  $1 $2 $3 $4 $5 $6 $7 $8
