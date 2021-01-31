#!/usr/bin/env bash
repetitions="5"
testMask= 128  #"16383"
twitterPath="/user/hadoop/diesterf/data/twitter/logs/"
dblpPath="/user/hadoop/diesterf/data/dblp/json/big/"
tpchPath="/user/hadoop/diesterf/data/tpch/"
warmup="true"
#saSize="1"
for saSize in "5" "6" "7"; do #"0" "1" "2" "3" "4" "5" "6" "7"
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
      for reference in "4" "5"; do #"3" "5"
          for size in "200"; do
                ./submit.sh $testSuite $reference $size $repetitions $warmup $testMask $dataPath $saSize
          done
      done
  done
done
