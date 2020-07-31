#!/usr/bin/env bash
repetitions="3"
testMask="127"
twitterPath="/user/hadoop/diesterf/data/twitter/logs/"
dblpPath="/user/hadoop/diesterf/data/dblp/json/big/"
provQuery="none"
warmup="false"

for testSuite in "twitter"; do
    if [ $testSuite = "twitter" ]; then
        dataPath=$twitterPath
    else
        dataPath=$dblpPath
    fi
    for reference in "true" "false"; do
        for size in "100"; do
            for iteration in "1"; do
                ./submit.sh $testSuite $reference $size $repetitions $warmup $testMask $dataPath
            done
        done
    done
done

