rm /data/diesterf/measurements/aggregated_runs.csv
hdfs dfs -copyToLocal /user/hadoop/diesterf/measurements/aggregated_runs.csv /data/diesterf/measurements/aggregated_runs.csv
rm /data/diesterf/measurements/runs.csv
hdfs dfs -copyToLocal /user/hadoop/diesterf/measurements/runs.csv /data/diesterf/measurements/runs.csv