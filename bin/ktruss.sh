#!/usr/bin/env bash

CONF_FILE="$PWD/bin/ktruss.conf"

function ktruss {
    input=$1
    k=$2
    p=$3
    pm=$4
    bin/run-spark.sh "ir.ac.sbu.graph.spark.ktruss.KTrussTSet" 120 $input $k $p $pm 1000
    bin/run-spark.sh "ir.ac.sbu.graph.spark.ktruss.KTrussTSet" 120 $input $k $p $pm 0
}
echo "youtube" > bin/inputs
ktruss  4   20
ktruss  19  20
##
echo "cit-patents" > bin/inputs
ktruss  4   34
ktruss  36  34
##
echo "soc-liveJournal" > bin/inputs
ktruss  4 100
ktruss  40 100
ktruss  80 100
ktruss  160 100
ktruss  362 100
##
echo "orkut" > bin/inputs
ktruss  4   159
ktruss  40  159
ktruss 78  159
##
echo "friendster" > bin/inputs
ktruss  4   492
ktruss  39   492

echo "twitter" > bin/inputs
ktruss  1998    1157
ktruss  4   1157
ktruss  40  1157
ktruss  80  1157
ktruss  160 1157