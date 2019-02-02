package com.wangheng.core.day05

import org.apache.spark.{SparkConf, SparkContext}

object TextLengthScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Text Length").setMaster("local")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("hdfs://localhost:9000/spark_test_data.txt", 1)
    val count = lines.map(line=>line.length).reduce(_ + _)
    println("File's length is : "+count)
  }

}
