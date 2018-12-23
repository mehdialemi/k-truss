#!/usr/bin/env bash

function ktruss {
    input=$1
    k=$2
    p=$3
    pm=$4
    bin/run-spark.sh "ir.ac.sbu.sbm.KTruss" $input $k $p $pm 1000
    bin/run-spark.sh "ir.ac.sbu.sbm.KTruss" $input $k $p $pm 0
}

ktruss  youtube 4   20  1
ktruss  cit-patents 4   60  1
ktruss  soc-liveJournal 4 33    3
ktruss  orkut   4   100  3
ktruss  friendster  4   200  4
ktruss  twitter 1998    2000  5
ktruss  twitter 1998    2000  5
