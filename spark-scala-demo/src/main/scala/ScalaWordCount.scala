import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object ScalaWordCount {
   def main(args: Array[String]) {
     if (args.length < 1) {
       System.err.println("Usage: <file>")
       System.exit(1)
     }

     val conf = new SparkConf().setAppName("Scala_WordCount_Demo")
     val sc = new SparkContext(conf)
     val line = sc.textFile(args(0))
 
     val counts = line.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_)
     
     counts.collect()
    
     counts.saveAsTextFile("output")
  
     sc.stop()
   }
}
