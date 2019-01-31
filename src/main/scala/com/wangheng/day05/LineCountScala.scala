package com.wangheng.day05

import org.apache.spark.{SparkConf, SparkContext}

object LineCountScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LineCount").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("hdfs://localhost:9000/spark_test_data.txt", 1)
    val pairs = lines.map(line=>(line, 1))
    val linecounts = pairs.reduceByKey( _ + _)
    linecounts.foreach(linecount => println(linecount._1 + " appear " + linecount._2 + " times."))
  }
}
