package com.wangheng.sparkRDD

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]):Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("Word Count")
    var sc = new SparkContext()
    val textFile = sc.textFile("/home/wangheng/word/word.txt")
    var wordcount = textFile.flatMap(line=>line.split("\t"))
    println(wordcount.context)
  }
}
