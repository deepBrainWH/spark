package com.wangheng.core.day03

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wordcount").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val lines = sc.textFile("hdfs://dell:9000/spark_test_data.txt", 1)
    val words = lines.flatMap(line=>line.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)
    wordCounts.foreach(wordCount => println(wordCount._1 + "Appear : " + wordCount._2))
    sc.stop()
  }
}
