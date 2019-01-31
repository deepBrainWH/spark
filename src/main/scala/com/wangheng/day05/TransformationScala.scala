package com.wangheng.day05

import com.sun.xml.internal.ws.util.Pool.TubePool
import org.apache.spark.{SparkConf, SparkContext}

object TransformationScala {
  def main(args: Array[String]): Unit = {
//    map()
//    filter()
//    flatMap()
    groupByKey()
  }


  def map(): Unit ={
    val conf = new SparkConf().setMaster("local").setAppName("Map")
    val sc = new SparkContext(conf)
    val numbers = Array(2,3,4,5,6,7,8)
    val numbersRDD = sc.parallelize(numbers)
    val multipleNumberRDD = numbersRDD.map(num=>num*2)
    multipleNumberRDD.foreach(num=>println(num))
  }

  def filter(): Unit={
    val conf = new SparkConf().setMaster("local").setAppName("Map")
    val sc = new SparkContext(conf)
    val numbers = Array(2,3,4,5,6,7,8)
    val numbersRDD = sc.parallelize(numbers)
    val numfilter = numbersRDD.filter(num=>num%2==0)
    numfilter.foreach(num=>println(num))
  }

  def flatMap(): Unit={
    val conf = new SparkConf().setMaster("local").setAppName("Map")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("/home/wangheng/Desktop/test_data/spark_test_data.txt",1)
    val words = lines.flatMap(line=>line.split(" "))
    words.foreach(word=>println(word))
  }

  def groupByKey(): Unit={
    val conf = new SparkConf().setMaster("local").setAppName("Map")
    val sc = new SparkContext(conf)
    val scores = Array(Tuple2("class1", 78), Tuple2("class2", 89), Tuple2("class2", 87), Tuple2("class1", 99))
    val rdd = sc.parallelize(scores,1)
    val groupScores = rdd.groupByKey()
    groupScores.foreach(score=>{
      println(score._1)
      score._2.foreach(singlescore=>println(singlescore))
      println("======================")
    })
  }
}
