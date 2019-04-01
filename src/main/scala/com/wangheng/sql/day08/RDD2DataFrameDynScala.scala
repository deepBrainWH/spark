package com.wangheng.sql.day08

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object RDD2DataFrameDynScala extends App {

  val conf = new SparkConf().setMaster("local").setAppName("rdd2datafrme")
  val sc = new SparkContext(conf)
  val sqlcontext = new SQLContext(sc)
  val lines = sc.textFile("hdfs://localhost:9000/test_data/test_data9.txt", 1)
  //第一步，构造出row的RDD
  val studentRDD = lines.map(line=>Row(line.split(" ")(0).toInt,
    line.split(" ")(1), line.split(" ")(2).toInt))
  //第二步，编程方式动态构造元素
  val structType = StructType(Array(
    StructField("id", IntegerType, nullable = true),
    StructField("name", StringType, nullable = true),
    StructField("age", IntegerType, nullable = true)
  ))
  //第三步，进行rdd到Dataframe的转换
  val teenagerDF = sqlcontext.createDataFrame(studentRDD, structType)
  teenagerDF.registerTempTable("students")
  private val resultDF: DataFrame = sqlcontext.sql("select * from students")
  resultDF.rdd.collect().foreach(row=>println(row))

}
