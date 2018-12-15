#!/usr/bin/env bash

function ktruss {
    input=$1
    k=$2
    k_plus=$3
    p=$4
    bin/run-spark.sh "ir.ac.sbu.sbm.KTruss" 120 $input $k $k_plus $p 1000
    bin/run-spark.sh "ir.ac.sbu.sbm.KTruss" 120 $input $k $k_plus $p 0
}

ktruss  youtube 4   10  20
ktruss  youtube 19  10  20
ktruss  cit-patents 4   10  68
ktruss  cit-patents 36   10 68
ktruss  soc-liveJournal 4 10    99
ktruss  soc-liveJournal 40 10   99
ktruss  soc-liveJournal 80 10   99
ktruss  soc-liveJournal 160 10  99
ktruss  soc-liveJournal 362 10  99
ktruss  orkut   4   10  212
ktruss  orkut   39   10 212
ktruss  orkut   78   10  212
ktruss  friendster  4   10  1476
ktruss  friendster  39   10 1476
ktruss  twitter 1998    10  9256
ktruss  twitter 4    10 9256
ktruss  twitter 40    10    9256
ktruss  twitter 80    10    9256
