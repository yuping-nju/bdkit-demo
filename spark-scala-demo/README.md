# Spark Scala Demo

- WordCount：统计文件中各个单词出现的次数


- 运行WordCount
   ```
     spark-submit \
     --class "ScalaWordCount" \
     --master spark://demo-master:7077 \
     target/scala-2.11/spark-scala-demo_2.11-1.0.jar \
     hdfs://demo-master:9000/wordcount/input/wordcount_data.txt
   ```