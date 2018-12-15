#!/usr/bin/env bash

class=$1
cores=$2
master=spark://alemi-1:7077
jar="$PWD/target/subgraph-mining-1.0-jar-with-dependencies.jar"
input=$3
k=$4
partitions=$5
pm=$6
kCoreIterations=$7

file_name=`date +%Y-%m-%d.%H.%M.%S`
log_file="$logs/$class/$input/$file_name.log"

$SPARK_HOME/bin/spark-submit --class $class --total-executor-cores $cores --master $master $jar $input $k $cores $partitions $pm $kCoreIterations
echo "Duration = $SECONDS"
