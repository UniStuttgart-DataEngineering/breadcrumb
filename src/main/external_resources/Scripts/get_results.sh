rm /data/diesterf/measurements/aggregated_runs.csv
hdfs dfs -copyToLocal /user/hadoop/diesterf/data/measurements/aggregated_runs.csv /data/diesterf/measurements/aggregated_runs.csv
rm /data/diesterf/measurements/runs.csv
hdfs dfs -copyToLocal /user/hadoop/diesterf/data/measurements/runs.csv /data/diesterf/measurements/runs.csv