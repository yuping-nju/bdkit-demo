# Python程序实现单词计数

- 统计文件中各个单词出现的次数
- Hadoop Streaming提供了一个便于进行MapReduce编程的工具包，使用它可以基于一些可执行命令、脚本语言或其他编程语言来实现Mapper和 Reducer，从而充分利用Hadoop并行计算框架的优势和能力，来处理大数据。Hadoop Streaming负责从标准输入依次读取文件的每一行，执行函数，把标准输出转化成key-value对或者key-null对。

## 运行说明

 - 输入参数
    - jar：指定hadoop-streaming-xxx.jar所在位置
    - files：python源代码文件
    - mapper：指定map程序
    - reducer：指定reduce程序
    - input：输入路径，该路径下可以有多个文件
    - output：输出路径（再次执行时要确保该目录尚不存在）
   ```
     hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.10.1.jar \
      -D stream.non.zero.exit.is.failure=false \
      -D mapreduce.job.name=python_wc \
      -files  mapper.py,reducer.py \
      -mapper  mapper.py \
      -reducer reducer.py \
      -input /input/wordcount_data.txt \
      -output /output
   ```