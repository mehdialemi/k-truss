#!/usr/bin/env bash

class=$1
cores=$2

master=spark://alemi-1:7077
jar="$PWD/target/k-truss-1.0-jar-with-dependencies.jar"
input="hdfs://alemi-1/graph-data/$3"
k=$4
k_plus=$5
partitions=$6
kCoreIterations=$7

file_name=`date +%Y-%m-%d.%H.%M.%S`
log_file="$logs/$class/$input/$file_name.log"

$SPARK_HOME/bin/spark-submit --class $class --total-executor-cores $cores --master $master $jar $input $k $k_plus $cores $partitions $kCoreIterations
echo "Duration = $SECONDS"
