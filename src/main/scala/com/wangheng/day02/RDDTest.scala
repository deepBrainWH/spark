package com.wangheng.day02

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Spark programming model.
  *
  */
object RDDTest {
  def main(args: Array[String]): Unit = {
    if (args.length == 0){
      System.err.println("Usage: RDD Test Error")
      System.exit(0)
    }
    val conf = new SparkConf()
    conf.setAppName("RDD One Test")
    val sc = new SparkContext(conf)

    sc.stop()
  }
}
