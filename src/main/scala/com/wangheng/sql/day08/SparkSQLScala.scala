package com.wangheng.sql.day08

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object SparkSQLScala{
  def main(args: Array[String]): Unit = {
    create_dataFrame()
  }

  def create_dataFrame():Unit={
    val conf = new SparkConf().setMaster("local").setAppName("create_dataFrame")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read.json("hdfs://localhost:9000/test_data/test_data8.txt")
    df.show()
    df.printSchema()
    df.select(df("name")).show()
    df.select(df("name"), df("age") + 100).show()
    df.filter(df("age")>19).show()
    df.groupBy(df("age")).count().show()
  }
}
