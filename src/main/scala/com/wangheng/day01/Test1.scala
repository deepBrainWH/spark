package com.wangheng.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * word count.
  */
object Test1{
  def main(args: Array[String]): Unit = {
    //配置信息类
    val conf = new SparkConf().setAppName("SparkWC").setMaster("local[1]")
    val sc: SparkContext = new SparkContext(conf)

    //reading data.
    val lines = sc.textFile(args(0))
    //processing data
    val words: RDD[String] = lines.flatMap(_.split(" "))
    val paired: RDD[(String, Int)] = words.map((_, 1))
    val reduced: RDD[(String, Int)] = paired.reduceByKey(_+_)
    val res: RDD[(String , Int)] = reduced.sortBy(_._2, ascending = false)
    //saving
    //
    println(res.collect().toBuffer)
    //stop task
    sc.stop()
  }
}
