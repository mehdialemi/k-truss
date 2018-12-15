#!/usr/bin/env bash

function ktruss {
    input=$1
    k=$2
    p=$3
    pm=$4
    bin/run-spark.sh "ir.ac.sbu.sbm.KTruss" 120 $input $k $p $pm 1000
    bin/run-spark.sh "ir.ac.sbu.sbm.KTruss" 120 $input $k $p $pm 0
}

ktruss  youtube 4   20  1
ktruss  youtube 19  20  1
ktruss  cit-patents 4   34  1
ktruss  cit-patents 36   34  1
ktruss  soc-liveJournal 4 100   1
ktruss  soc-liveJournal 40 100   1
ktruss  soc-liveJournal 80 100   1
ktruss  soc-liveJournal 160 100   1
ktruss  soc-liveJournal 362 100   1
ktruss  orkut   4   160 2
ktruss  orkut   40   160 2
ktruss  orkut   78   160 2
ktruss  friendster  4   492     2
ktruss  friendster  39   492    2
ktruss  twitter 1998    1157    6
ktruss  twitter 4    1157   6
ktruss  twitter 40    1157  6
ktruss  twitter 80    1157  6
