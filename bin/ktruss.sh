#!/usr/bin/env bash

function ktruss {
    input=$1
    k=$2
    p=$3
    pm=$4
    bin/run-spark.sh "ir.ac.sbu.sbm.KTruss" 120 $input $k $p 1000
    bin/run-spark.sh "ir.ac.sbu.sbm.KTruss" 120 $input $k $p 0
}

ktruss  youtube 4   20
ktruss  youtube 19  20
ktruss  cit-patents 4   68
ktruss  cit-patents 36   68
ktruss  soc-liveJournal 4 99
ktruss  soc-liveJournal 40 99
ktruss  soc-liveJournal 80 99
ktruss  soc-liveJournal 160 99
ktruss  soc-liveJournal 362 99
ktruss  orkut   4   530
ktruss  orkut   39   530
ktruss  orkut   78   530
ktruss  friendster  4   1476
ktruss  friendster  39   1476
ktruss  twitter 1998    9256
ktruss  twitter 4    9256
ktruss  twitter 40    9256
ktruss  twitter 80    9256
