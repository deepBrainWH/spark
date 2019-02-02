package com.wangheng.core.day06

import org.apache.spark.{SparkConf, SparkContext}

object ActionScala {
  def main(args: Array[String]): Unit = {
//    reduce()
//    collect()
//    count()
//    take()
    countByKey()
  }

  def reduce(): Unit = {
    val conf = new SparkConf().setAppName("reduce").setMaster("local")
    val sc = new SparkContext(conf)

    val numbersRDD = sc.parallelize(Array(1,2,3,4,5,6,7,8,9,10))
    val result = numbersRDD.reduce(_ + _)
    println(result)
  }

  def collect(): Unit = {
    val conf = new SparkConf().setAppName("reduce").setMaster("local")
    val sc = new SparkContext(conf)
    val numbersRDD = sc.parallelize(Array(1,2,3,4,5,6,7,8,9,10))
    val resultRDD = numbersRDD.map(num=>num*2)
    val resultCollect = resultRDD.collect()
    for (num <- resultCollect){
      println(num)
    }
  }

  def count(): Unit={
    val conf = new SparkConf().setAppName("reduce").setMaster("local")
    val sc = new SparkContext(conf)
    val numbersRDD = sc.parallelize(Array(1,2,3,4,5,6,7,8,9,10))
    val result = numbersRDD.count()
    println(result)
  }

  def take(): Unit = {
    val conf = new SparkConf().setAppName("reduce").setMaster("local")
    val sc = new SparkContext(conf)
    val numbersRDD = sc.parallelize(Array(1,2,3,4,5,6,7,8,9,10))
    val taking = numbersRDD.take(3)
    for(num<-taking){
      println(num)
    }
  }

  def countByKey():Unit={
    val conf = new SparkConf().setAppName("reduce").setMaster("local")
    val sc = new SparkContext(conf)
    var studentRDD = sc.parallelize(Array(
      Tuple2("class1", "wnagheng"),
      Tuple2("class1", "huanhuan"),
      Tuple2("class2", "liukaowen"),
      Tuple2("class3", "kkkkk")
    ))
    val result = studentRDD.countByKey()
    result.foreach(student=>println(student._1 + " : " + student._2))
  }
}
