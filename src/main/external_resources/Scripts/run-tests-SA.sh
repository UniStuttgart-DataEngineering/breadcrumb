#!/usr/bin/env bash
repetitions="3"
testMask="127"
twitterPath="/user/hadoop/diesterf/data/twitter/logs/"
dblpPath="/user/hadoop/diesterf/data/dblp/json/big/"
warmup="true"


for testSuite in "dblp" "twitter"; do
    if [ $testSuite = "twitter" ]; then
        dataPath=$twitterPath
    else
        dataPath=$dblpPath
    fi
    for reference in "5"; do
        for size in "100"; do
            for saSize in "1" "2" "3" "4" "5"; do
                ./submit.sh $testSuite $reference $size $repetitions $warmup $testMask $dataPath $saSize
            done
        done
    done
done

