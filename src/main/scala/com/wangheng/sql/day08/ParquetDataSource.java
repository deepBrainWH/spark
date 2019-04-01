package com.wangheng.sql.day08;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

public class ParquetDataSource {
    public static void main(String[] args) {
//        SparkConf conf = new SparkConf().setAppName("data sources").setMaster("local");
//        JavaSparkContext sc = new JavaSparkContext(conf);
//        SQLContext sqlContext = new SQLContext(sc);
//        parquetDataSource(sqlContext);
//        sc.close();

        SparkSession spark = SparkSession.builder()
                .appName("datasource")
                .master("local")
                .getOrCreate();
        parquetDataSourceWithSparkSession(spark);


    }

    private static void parquetDataSource(SQLContext sc){
        Dataset<Row> json = sc.read().json("hdfs://localhost:9000/test_data/test_data8.txt");
        json.write().parquet("/home/wangheng/Desktop/parquet_data/test_data8.parquet");
    }

    private static void parquetDataSourceWithSparkSession(SparkSession spark){
        Dataset<Row> json = spark.read().json("hdfs://localhost:9000/test_data/test_data8.txt");
        json.show();
    }

}
