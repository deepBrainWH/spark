package com.wangheng.core.day06

import org.apache.spark.{SparkConf, SparkContext}

object SecondarySortScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("secondarySort").setMaster("local")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("/home/wangheng/Desktop/test_data/test_data5.txt", 1)
    val pair = lines.map(line=>(
      new SecondarySortKeyScala(line.split(" ")(0).toInt, line.split(" ")(1).toInt), line
    ))
    val result = pair.sortByKey(ascending = false)
    result.foreach(pair=>println(pair._2))
  }

}
