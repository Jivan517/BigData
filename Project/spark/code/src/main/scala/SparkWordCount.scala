package cs522.sparkproject

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SparkWordCount {
  
  def main(args: Array[String]) {
    
    //spark configuration
    val conf = new SparkConf().setAppName("Spark WordCount")
    
    //create spark context
    val sc = new SparkContext(conf)
    
    // for recursive file search within directory
    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive","true")
    
    
    //create words splitted by space(s)
    val tokenized = sc.textFile(args(0)).flatMap(_.split("\\s+"))
    
    //create (word, 1) pairs or we can create like .map((_,1))
    val wordsMapped = tokenized.map { word => (word, 1) }
    
    //create word counts 
    val wordCounts = wordsMapped.reduceByKey(_+_)
    
    //save it in HDFS
    wordCounts.saveAsTextFile(args(1))
    
    println(wordCounts.collect())
    
    
    
    
  }
}