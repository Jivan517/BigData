package cs522.sparkproject

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import scala.util.matching.Regex
import java.util.regex.Pattern
import org.apache.spark.sql.SQLContext

/**
 * **
 * =====================================
 * ===	LogAnalyzer 1.0.0 ===============
 * ===	Author: Jivan Nepali, 985095 ====
 * =====================================
 * Features:
 *
 * 1. extract top selling products
 * 2. extract top client IPs
 * 3. extract top selling product categories
 *
 * **
 */

object LogAnalyzer {

  def main(args: Array[String]) {

    println("==Operation Started==")

    // spark configuration
    val sparkConf = new SparkConf().setAppName("Log Analyzer")

    // delete the existing output directory
    val hadoopConf = new Configuration()
    val hdfs = FileSystem.get(new java.net.URI("hdfs:///quickstart.cloudera:8888"), hadoopConf)
    try {

      hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true)

    } catch {
      case _:
        Throwable => {}
    }

    // spark context
    val sc = new SparkContext(sparkConf)

    // recursively search for file
    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")
    val sqlContext = new SQLContext(sc)
    
    //text file
    val rdd = sc.textFile(args(0))

    //parse the file
    val logRdd = rdd.flatMap(_.split("\""))

    //regex to find the pattern
    val pattern1 = ".+itemId=".r

    //map filtered word - for finding the top selling products
    val filteredProductsRdd = logRdd.filter { word => word.contains(".com/cart.do?action=purchase") }
      .map { item => (pattern1.replaceAllIn(item, ""), 1) }

    val pattern2 = "(.+categoryId=)|(&JSESSIONID.+)".r
    // filtered categories 
    val filteredCategories = logRdd.filter { word => (word.contains("categoryId=") && word.contains("&JSESSIONID")) }
      .map { item => (pattern2.replaceAllIn(item, ""), 1) }.filter(filterNullCategory)

    val pattern3 = " - -.+".r
    
    // filetered IPs
    val filteredIps = logRdd.filter { word => (Pattern.matches("(?:[0-9]{1,3}.){3}[0-9]{1,3} - - .+", word)) }
      .map { item => (pattern3.replaceAllIn(item, ""), 1) }

    //reduce  by itemId, sorted in desc order
    val productsRdd = filteredProductsRdd.reduceByKey(_ + _).map(item => item.swap).sortByKey(false, 1).map(item => item.swap).map(showProduct)

    val categoriesRdd = filteredCategories.reduceByKey(_ + _).map(item => item.swap).sortByKey(false, 1).map(item => item.swap).map(showProdCategory)

    val ipsRdd = filteredIps.reduceByKey(_ + _).map(item => item.swap).sortByKey(false, 1).map(item => item.swap).map(showClientIp)

    //save result to HDFS
    productsRdd.saveAsTextFile(args(1) + "/topproducts")

    // save results to HDFS
    categoriesRdd.saveAsTextFile(args(1) + "/topcategories")

    // save results to HDFS
    ipsRdd.saveAsTextFile(args(1) + "/topclientIPs")

    println("==Operation completed==")

  }

  def checkForValidCategory(httpStatus: String, categoryUrl: String): Boolean = {

    // status = OK, i.e. 200
    if (httpStatus.contains(" 200 ") && categoryUrl.contains("categoryId=") && categoryUrl.contains("&JSESSIONID"))
      return true

    return false

  }
  
  //filter NULL category
  def filterNullCategory(category: (String, Int)): Boolean = {

    if (category._1.toUpperCase().contains("NULL"))
      return false

    return true

  }
  
  //make product IDs readable  
  def showProduct(pair : (String, Int)) : (String, String) = {
    
    return new Tuple2("ProductID = " + pair._1, "Purchase Frequency = " + pair._2.toString() + " times")
  }
  
  //make Product Category readable
  def showProdCategory(pair : (String, Int)) : (String, String) = {
    
    return new Tuple2("Prod Category = " + pair._1, "Purchase Frequency = " + pair._2.toString() + " times")
  }
  
  //make client IPs readable
  def showClientIp(pair : (String, Int)) : (String, String) = {
    
    return new Tuple2("Client IP ADDR = " + pair._1, "Site Visited = " + pair._2.toString() + " times")
  }

}