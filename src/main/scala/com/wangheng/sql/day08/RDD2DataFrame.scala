package com.wangheng.sql.day08

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

object RDD2DataFrame extends App {
  case class Student(id: Int, name:String, age:Int)
  val conf = new SparkConf().setMaster("local").setAppName("reflect")
  val sc = new SparkContext(conf)
  val lines = sc.textFile("hdfs://localhost:9000/test_data/test_data9.txt", 1)
  val students = lines.map(line=>line.split(" "))
  val rdd = students.map(tuple=>Student(tuple(0).toInt, tuple(1), tuple(2).toInt))
  val sqlcontext = new SQLContext(sc)
  //直接使用rdd的toDF()
  import sqlcontext.implicits._
  val student_DF = rdd.toDF()
  student_DF.registerTempTable("students")
  private val teenagerDF: DataFrame = sqlcontext.sql("select * from students")
  private val teenagerRDD: RDD[Row] = teenagerDF.rdd
  teenagerRDD.foreach(row=>println(row.get(0) + ":"+row.get(1)+":"+row.get(2)))
}
