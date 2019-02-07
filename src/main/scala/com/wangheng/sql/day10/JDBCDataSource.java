package com.wangheng.sql.day10;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class JDBCDataSource {
    public static void main(String[] args) throws ClassNotFoundException {
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("jdbc_data_sources")
                .getOrCreate();
        runJDBCDatasetExample(spark);


    }

    /**
     * 注意，需要写shell文件启动。～/workspace/
     * @param spark
     * @throws ClassNotFoundException
     */
    private static void runJDBCDatasetExample(SparkSession spark) throws ClassNotFoundException {
        Dataset<Row> jdbcDF = spark.read().format("jdbc")
                .option("url", "jdbc:mysql://localhost:3306/program1")
//                .option("dbtable", "Course")
                .option("user", "root")
                .option("password", "mysql673")
                .load();
        jdbcDF.write().format("jdbc").option("dbtable", "student");
        jdbcDF.show();
    }
}
