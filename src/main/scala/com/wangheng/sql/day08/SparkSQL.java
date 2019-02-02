package com.wangheng.sql.day08;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class SparkSQL {
    public static void main(String[] args) {
        createDataFrame();
    }

    private static void createDataFrame(){
        /**
         * creating dataFrame using JSON file.
         */
        SparkConf conf = new SparkConf().setMaster("local").setAppName("createDataFrame");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        Dataset<Row> json = sqlContext.read().json("hdfs://localhost:9000/test_data/test_data8.txt");
        json.show();

        sc.close();
    }
}
