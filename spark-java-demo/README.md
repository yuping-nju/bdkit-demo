# Spark Java Demo


- 运行JavaWordCount
   ```
     spark-submit \
     --class "wc.JavaWordCount" \
     --master local \
     target/spark-java-demo-1.0-SNAPSHOT.jar \
     <input path> \
     <output path>
   ```