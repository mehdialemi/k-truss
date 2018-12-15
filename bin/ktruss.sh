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
ktruss  cit-patents 4   34
ktruss  cit-patents 36   34
ktruss  soc-liveJournal 4 100
ktruss  soc-liveJournal 40 100
ktruss  soc-liveJournal 80 100
ktruss  soc-liveJournal 160 100
ktruss  soc-liveJournal 362 100
ktruss  orkut   4   160
ktruss  orkut   40   160
ktruss  orkut   78   160
ktruss  friendster  4   492
ktruss  friendster  39   492
ktruss  twitter 1998    1157
ktruss  twitter 4    1157
ktruss  twitter 40    1157
ktruss  twitter 80    1157
