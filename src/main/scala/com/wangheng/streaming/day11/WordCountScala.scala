package com.wangheng.streaming.day11

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

object WordCountScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("scala_word_count")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(5))

    val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(line=>line.split(" "))
    val pair = words.map(word=>(word, 1))

    val result = pair.reduceByKey(_ + _)

    Thread.sleep(5000)
    result.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
