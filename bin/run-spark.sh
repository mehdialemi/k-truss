#!/usr/bin/env bash

class=$1
cores=120

master=spark://alemi-1:7077
jar="$PWD/target/k-truss-1.0-jar-with-dependencies.jar"
input="hdfs://alemi-1/graph-data/$2"
k=$3
partitions=$4
pm=$5
kCoreIterations=$6

file_name=`date +%Y-%m-%d.%H.%M.%S`
log_file="$logs/$class/$input/$file_name.log"

$SPARK_HOME/bin/spark-submit --class $class --total-executor-cores $cores --master $master $jar $input $k $cores $partitions $pm $kCoreIterations
echo "Duration = $SECONDS"
