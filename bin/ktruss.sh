#!/usr/bin/env bash

function ktruss {
    input=$1
    k=$2
    p=$3
    pm=$4
    bin/run-spark.sh "ir.ac.sbu.sbm.KTruss" 120 $input $k $p $pm 1000
    bin/run-spark.sh "ir.ac.sbu.sbm.KTruss" 120 $input $k $p $pm 0
}

ktruss  youtube 4   20  2
ktruss  youtube 19  20  2
ktruss  cit-patents 4   34  2
ktruss  cit-patents 36   34  2
ktruss  soc-liveJournal 4 100   3
ktruss  soc-liveJournal 40 100   3
ktruss  soc-liveJournal 80 100   3
ktruss  soc-liveJournal 160 100   3
ktruss  soc-liveJournal 362 100   3
ktruss  orkut   4   160 3
ktruss  orkut   40   160 3
ktruss  orkut   78   160 3
ktruss  friendster  4   492     3
ktruss  friendster  39   492    3
ktruss  twitter 1998    1157    6
ktruss  twitter 4    1157   6
ktruss  twitter 40    1157  6
ktruss  twitter 80    1157  6
