#!/usr/bin/env bash
repetitions="3"
testMask="1"
twitterPath="/user/hadoop/diesterf/data/twitter/logs/"
dblpPath="/user/hadoop/diesterf/data/dblp/json/big/"
warmup="true"
saSize="1"

for testSuite in "dblp"; do
    if [ $testSuite = "twitter" ]; then
        dataPath=$twitterPath
    else
        dataPath=$dblpPath
    fi
    for reference in "3"; do
        for size in "400" "500"; do
            for iteration in "1"; do
                ./submit.sh $testSuite $reference $size $repetitions $warmup $testMask $dataPath $saSize
            done
        done
    done
done

