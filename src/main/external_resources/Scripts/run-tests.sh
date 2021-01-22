#!/usr/bin/env bash
repetitions="1"
testMask="127"
twitterPath="/user/hadoop/diesterf/data/twitter/logs/"
dblpPath="/user/hadoop/diesterf/data/dblp/json/big/"
tpchPath="/user/hadoop/diesterf/data/tpch_small/"
warmup="false"
#saSize="1"
for saSize in "1"; do #"1" "2" "3" "4"
  for testSuite in "tpch"; do #"dblp" "twitter"
      if [ $testSuite = "twitter" ]; then
          dataPath=$twitterPath
      fi
      if [ $testSuite = "dblp" ]; then
          dataPath=$dblpPath
      fi
      if [ $testSuite = "tpch" ]; then
          dataPath=$tpchPath
      fi
      for reference in "4"; do #"3" "5"
          for size in "100"; do
                ./submit.sh $testSuite $reference $size $repetitions $warmup $testMask $dataPath $saSize
          done
      done
  done
done
