package com.wangheng.streaming.day12

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SlideWindowScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("slide window").setMaster("local[2]")
    val jssc = new StreamingContext(conf, Seconds(2))
    val lines = jssc.socketTextStream("localhost", 9999)
    val pair = lines.map(line=>(line.split(" ")(1), 1))
    val searchWordCountsDStream = pair.reduceByKeyAndWindow(
      (v1:Int, v2:Int)=>v1 + v2,
      Seconds(60), Seconds(10))

    val finalDStream = searchWordCountsDStream.transform(pair=>{
      val countSearchWordsRDD = pair.map(tuple=>(tuple._2, tuple._1))
      val result = countSearchWordsRDD.sortByKey(ascending = false)
      val result_ = result.map(tuple=>(tuple._2, tuple._1))
      val top3 = result_.take(3)
      for(a<-top3){
        println(a._1 + ":"+a._2)
      }
      result_
    })
    finalDStream.print()


    jssc.start()
    jssc.awaitTermination()
  }

}
