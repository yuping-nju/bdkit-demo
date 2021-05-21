#!/bin/bash
hdfs dfs -rm -r /output
hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.10.1.jar \
-D stream.non.zero.exit.is.failure=false \
-D mapreduce.job.name=python_wc \
-files  mapper.py,reducer.py \
-mapper  mapper.py \
-reducer reducer.py \
-input /input/wordcount_data.txt \
-output /output
