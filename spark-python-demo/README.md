# Spark Python Demo

- wordcount.py：统计文件中各个单词出现的次数


## 运行说明

- 运行wordcount
  - Usage：wordcount &lt;file&gt;
   ```
     spark-submit \
     --master spark://demo-master:7077 \
     src/wordcount.py \
     <file path or url>
   ```
