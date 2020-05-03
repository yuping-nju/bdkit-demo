# 单词计数1.0

- 统计文件中各个单词出现的次数
- 打包的jar文件已经指定了main class，若打包时不指定main class，则需要加上main_class_name作为运行参数(比如wc.WordCount)

## 运行说明

 - 输入参数
    - input_path：输入路径，该路径下可以有多个文件
    - output_path：输出路径（再次执行时要确保该目录尚不存在）
   ```
     hadoop jar target/wordcount-1.0.jar input_path output_path
   ```