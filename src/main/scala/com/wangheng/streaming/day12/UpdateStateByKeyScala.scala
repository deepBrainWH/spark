package com.wangheng.streaming.day12

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object UpdateStateByKeyScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("updateStateByKey").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(8))

    ssc.checkpoint("hdfs://localhost:9000/word_count_checkpoint")
    val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(line=>line.split(" "))
    val pairs = words.map(word=>(word, 1))
    val wordCounts = pairs.updateStateByKey((values: Seq[Int], state: Option[Int]) => {
      var newValue = state.getOrElse(0)
      for(value<-values){
        newValue += value
      }
      Option(newValue)
    })

    wordCounts.print()

  }

}
