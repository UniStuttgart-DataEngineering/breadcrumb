#!/usr/bin/env bash
repetitions="3"
testMask="127"
twitterPath="/user/hadoop/diesterf/data/twitter/logs/"
dblpPath="/user/hadoop/diesterf/data/dblp/json/big/"
warmup="true"
#saSize="1"

for testSuite in "dblp" "twitter"; do
    if [ $testSuite = "twitter" ]; then
        dataPath=$twitterPath
    else
        dataPath=$dblpPath
    fi
    for reference in "3" "5"; do
        for size in "100"; do
            for saSize in "0" "1" "2" "3" "4"; do
                ./submit.sh $testSuite $reference $size $repetitions $warmup $testMask $dataPath $saSize
            done
        done
    done
done

