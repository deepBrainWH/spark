package com.wangheng.sql.day08;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class SparkSQL {
    public static void main(String[] args) {
        createDataFrame();
//        rdd2dataframe_1();
    }


    private static void createDataFrame(){
        /**
         * creating dataFrame using JSON file.
         */
        SparkConf conf = new SparkConf().setMaster("local").setAppName("createDataFrame");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        Dataset<Row> df = sqlContext.read().json("hdfs://localhost:9000/test_data/test_data8.txt");
        df.show();
        df.select(df.col("id"), df.col("age").plus(100)).show();
        df.filter(df.col("age").gt(19)).show();
        df.groupBy(df.col("age")).count().show();
        sc.close();
    }

    private static void rdd2dataframe_1(){
        /**
         * 第一种转换方式，利用反射来推断包含特定数据的RDD.前提：知道RDD的元数据
         */
        SparkConf conf = new SparkConf().setMaster("local").setAppName("reflect");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        JavaRDD<String> lines = sc.textFile("hdfs://localhost:9000/test_data/test_data9.txt", 1);
        JavaRDD<Student> map2student = lines.map(new Function<String, Student>() {
            @Override
            public Student call(String s) throws Exception {
                String[] stu = s.split(" ");
                return new Student(Integer.valueOf(stu[0].trim()), stu[1].trim(), Integer.valueOf(stu[2].trim()));
            }
        });

        Dataset<Row> df = sqlContext.createDataFrame(map2student, Student.class);
        //拿到一个dataframe后将其注册成为一个临时表。
        df.registerTempTable("students");
        Dataset<Row> result = sqlContext.sql("select * from students where age<=20");
        //讲查询出来的dataframe映射为javaRDD
        JavaRDD<Row> rowJavaRDD = result.javaRDD();
        //将rdd中的数据映射为student
        JavaRDD<Student> result_student = rowJavaRDD.map(new Function<Row, Student>() {
            @Override
            public Student call(Row row) throws Exception {
                return new Student(row.getInt(0), row.getString(2), row.getInt(1));
            }
        });

        rowJavaRDD.foreach(new VoidFunction<Row>() {
            @Override
            public void call(Row row) throws Exception {
                System.out.println(row.get(1)+":"+row.get(0)+":"+row.get(2));
            }
        });
        sc.close();

    }

    public static class Student implements Serializable {
        private int id;
        private String name;
        private int age;

        public Student(int id, String name, int age) {
            this.id = id;
            this.name = name;
            this.age = age;
        }

        public Student() {
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }

        @Override
        public String toString() {
            return this.id + ":" + this.name+":"+this.age;
        }
    }
}
