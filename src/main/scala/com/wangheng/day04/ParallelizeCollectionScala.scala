package com.wangheng.day04

import org.apache.spark.{SparkConf, SparkContext}

object ParallelizeCollectionScala {
  def main(args: Array[String]): Unit={
    val conf = new SparkConf().setAppName("ParallelizeCollection")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val numbers = Array(1,2,3,4,5,6,7,8,9,10)
    val numberRDD = sc.parallelize(numbers, 10)
    val sum = numberRDD.reduce(_ + _)
    println(sum)
  }
}
