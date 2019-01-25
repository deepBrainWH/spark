package com.wangheng.sparkRDD

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]):Unit = {
    val conf = new SparkConf()
    conf.setAppName("Word Count")
    var sc = new SparkContext()
    val textFile = sc.textFile("/home/wangheng/word/word.txt")
    var wordcount = textFile.flatMap(line=>line.split("\\s+")).map(word=>(word, 1)).reduceByKey(_+_)
    wordcount.collect.foreach(println)
    println(wordcount.collect)
    println("===================================")
    println(wordcount.collect)
    println("have already output")
    sc.stop()
  }
}
