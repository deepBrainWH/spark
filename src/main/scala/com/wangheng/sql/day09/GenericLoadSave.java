package com.wangheng.sql.day09;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.hive.HiveContext;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * 通用load and save 操作。
 */
public class GenericLoadSave {
    public static void main(String[] args) {
        if(args.length == 0){
            System.err.println("参数错误！");
            System.exit(-1);
        }else if("1".equals(args[0])){
            hiveDataSource();
        }else if("2".equals(args[0])){
            hiveDatasource_2();
        }
//        save2parquet();
//        readfromparquet();
//
    }

    //save to parquet.;
    private static void save2parquet(){
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setMaster("local").setAppName("save2parquet"));
        SQLContext sqlContext = new SQLContext(sc);
        List<Student> students = Arrays.asList(
                new Student(1, 19, "wangheng"),
                new Student(2, 21, "zhanghuan"));
        JavaRDD<Student> rdd = sc.parallelize(students, 1);
        Dataset<Row> dataFrame = sqlContext.createDataFrame(rdd, Student.class);
        dataFrame.show();
        dataFrame.write().parquet("/home/wangheng/Desktop/parquet_demo.parquet");
        sc.close();
    }

    //read from parquet.
    private static void readfromparquet(){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("save2parquet");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        Dataset<Row> mergeSchema = sqlContext.read().option("mergeSchema", true).parquet("/home/wangheng/Desktop/parquet_demo.parquet/part-00000-4301f174-0554-48d1-9bbc-1eebe2f3c1e3-c000.snappy.parquet");
        mergeSchema.show();
        sc.close();
    }

    //hive数据源
    private static void hiveDataSource(){
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark Hive Example")
                .enableHiveSupport()
                .getOrCreate();
        spark.sql("use hive_test");
        spark.sql("drop table if exists src");
        spark.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)");
        spark.sql("LOAD DATA LOCAL INPATH '/usr/app/hive/examples/files/kv1.txt' INTO TABLE src");
        spark.sql("SELECT * FROM src").show();
    }

    private static void hiveDatasource_2(){
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setMaster("local").setAppName("hive_context"));
        HiveContext hiveContext = new HiveContext(sc);
        hiveContext.sql("use hive_test");
        hiveContext.sql("select * from src limit 10").show();
        sc.close();
    }

    public static class Student implements Serializable {
        private int id;
        private int age;
        private String name;

        Student(int id, int age, String name) {
            this.id = id;
            this.age = age;
            this.name = name;
        }

        public Student() {
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }

}
