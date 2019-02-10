package com.wangheng.streaming.day11

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TransformBlackListScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TransformBlackList").setMaster("local[2]")
    val jssc = new StreamingContext(conf, Seconds(8))

    val blackListRDD = jssc.sparkContext.parallelize(Array(
      ("tom", true),
      ("wangheng", true),
      ("zhanghuan", true)
    ))



    val lines = jssc.socketTextStream("localhost", 9999)
    val pair = lines.map(line=>(line.split(" ")(1), line))

    val validRES = pair.transform(userClickedLogRDD => {
      val joinedRDD = userClickedLogRDD.leftOuterJoin(blackListRDD)
      val filtered = joinedRDD.filter(tuple=>{
        if(tuple._2._2.getOrElse(false)){
          false
        }else{
          true
        }
      })
      val validResult = filtered.map(tuple=>tuple._2._1)
      validResult
    })

    validRES.print()
    jssc.start()
    jssc.awaitTermination()

  }

}
