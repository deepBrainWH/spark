package com.wangheng.sparkRDD

import org.apache.spark.{SparkConf, SparkContext}

object RDDOne {
  def main(args: Array[String]): Unit = {
    var conf = new SparkConf()
    conf.set("fs.defaultFS","hdfs://localhost:9000")
    conf.setAppName("RDDOne").setMaster("spark://dell:7077")
    var sc = new SparkContext(conf)

    var textFile = sc.textFile("hdfs://localhost:9000/data/word/")
    val worldCounts = textFile.flatMap(line=>line.split(" ")).map((word=>(word, 1))).reduceByKey(_+_)
    worldCounts.collect.foreach(println)
    sc.stop()
  }

}
