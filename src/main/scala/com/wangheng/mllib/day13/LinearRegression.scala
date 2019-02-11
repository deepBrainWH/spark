package com.wangheng.mllib.day13

import org.apache.spark.sql.SparkSession


case class Station(sid: String, lat:Double, lon:Double, elev:Double, name:String)
object LinearRegression{
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("LinearRegression").getOrCreate()
    import spark.implicits._

//    spark.sparkContext.setLogLevel("WARN")

    // NOAA data from ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/  in the by_year directory
    val lines = spark.read.textFile("hdfs://localhost:9000/ml_data/ghcnd-stations.txt")
    val stations = lines.map(line=>{
      val id = line.substring(0, 11).trim
      val lat = line.substring(12, 20).trim.toDouble
      val lon = line.substring(21, 30).trim.toDouble
      val elev = line.substring(31, 37).trim.toDouble
      val name = line.substring(41, 71).trim
      Station(id, lat, lon, elev, name)
    }).cache()

  }
}
