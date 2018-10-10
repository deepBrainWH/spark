package com.wangheng.sparkRDD

import org.apache.spark.{SparkConf, SparkContext}

object RDDOne {
  def main(args: Array[String]): Unit = {
    var conf = new SparkConf()
    conf.set("fs.defaultFS","hdfs://localhost:9000")
    conf.setAppName("RDDOne").setMaster("spark://dell:7077")
    var sc = new SparkContext(conf)

    var textFile = sc.textFile("hdfs://localhost:9000/data/word/world.txt")
    var num = textFile.flatMap(x=>x.split(" ")).filter(_.contains("a")).count()
    println("words with a: %s".format(num))
    sc.stop()
  }

}
