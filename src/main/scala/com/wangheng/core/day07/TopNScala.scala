package com.wangheng.core.day07

import org.apache.spark.{SparkConf, SparkContext}
import scala.util.control.Breaks._

object TopNScala {
  def main(args: Array[String]): Unit = {
//    simpleTopN()
    groupTopN()
  }

  /**
    * Simple top N
    */
  def simpleTopN(): Unit={
    val conf = new SparkConf().setAppName("topN").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val lines = sc.textFile("hdfs://dell:9000/test_data/test_data6.txt", 1)
    val pair = lines.map(num=>(num.toInt, num))
    val result = pair.sortByKey(ascending = false)
    val top3 = result.take(3)
    for(a<-top3){
      println(a._2)
    }
  }

  def groupTopN(): Unit={
    val conf = new SparkConf().setAppName("topN").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val lines = sc.textFile("hdfs://dell:9000/test_data/test_data7.txt", 1)
    val pair = lines.map(line=>(line.split(" ")(0), line.split(" ")(1), line.split(" ")(2)))
    val result = pair.sortBy(student=>student._3.toInt, ascending = false, numPartitions = 1)
    val result2 = result.groupBy(student=>student._1)
    result2.foreach(group=>{
      println(group._1)
      var i = 0
      breakable(
        for(stu<-group._2){
          if(i==3){
            break()
          }else{
            println(stu._2 + " : " + stu._3)
          }
          i += 1
        }
      )
      println("=================================")
    })
  }

}
