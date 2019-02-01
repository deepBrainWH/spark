package com.wangheng.day06

import org.apache.spark.{SparkConf, SparkContext}

object SortWordCountScala {
  def main(args: Array[String]): Unit = {
    sortWordCount()
  }
  def sortWordCount(): Unit={
    val conf = new SparkConf().setMaster("local").setAppName("sorted_wordcount")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("/home/wangheng/Desktop/test_data/test_data4.txt", 1)
    val unit = lines.flatMap(line=>line.split(" "))
    val wordmap = unit.map(word=>(word, 1))
    val wordcount = wordmap.reduceByKey(_ + _)
    val count_word = wordcount.map(word=>(word._2, word._1))
    val result = count_word.sortByKey(ascending = false)
    result.foreach(word=>println(word._2 + " : " + word._1))
  }

}
