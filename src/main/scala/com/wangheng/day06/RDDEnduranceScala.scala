package com.wangheng.day06

import org.apache.spark.{SparkConf, SparkContext}

object RDDEnduranceScala {
  def main(args: Array[String]): Unit = {
//    persist()
//    broadCastVariable()
    accumulator_operation()
  }

  def persist(): Unit={
    val conf = new SparkConf().setMaster("local").setAppName("rdd endurance")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("/home/wangheng/Desktop/test_data/test_data2.txt", 1).cache()
    println(rdd.count())
  }

  /**
    * scala share variable: broadCast.
    */
  def broadCastVariable():Unit={
    val conf = new SparkConf().setMaster("local").setAppName("broadcast")
    val sc = new SparkContext(conf)
    val dataRDD = sc.textFile("/home/wangheng/Desktop/test_data/test_data3.txt",1)
    val broadcast_variable = sc.broadcast(5)
    val unit = dataRDD.flatMap(line=>line.split(" "))
    val result = unit.map(num=>num.toInt*broadcast_variable.value)
    result.foreach(result_num=>println(result_num))
  }

  /**
    * Accumulator operation.
    */
  def accumulator_operation(): Unit={
    val conf = new SparkConf().setMaster("local").setAppName("broadcast")
    val sc = new SparkContext(conf)
    val dataRDD = sc.textFile("/home/wangheng/Desktop/test_data/test_data3.txt",1)
    val accu = sc.accumulator(0)
    val num_strs = dataRDD.flatMap(line=>line.split(" "))
    num_strs.foreach(num=>accu.add(num.toInt))
    println(accu.value)
  }
}
