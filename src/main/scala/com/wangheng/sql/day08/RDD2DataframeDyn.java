package com.wangheng.sql.day08;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;

/**
 * 以编程的方式动态创建rdd
 */
public class RDD2DataframeDyn {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("dynamic").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        JavaRDD<String> lines = sc.textFile("hdfs://localhost:9000/test_data/test_data9.txt", 1);
        //第一步，创建一个普通的Javardd，但是必须将数据封装再row中。即：RDD(Row)这种格式的rdd.
        JavaRDD<Row> studentRDD = lines.map(new Function<String, Row>() {
            @Override
            public Row call(String s) throws Exception {
                String[] stus = s.split(" ");
                return RowFactory.create(Integer.valueOf(stus[0]), stus[1], Integer.valueOf(stus[2]));
            }
        });
        //第二步：动态构造元数据
        ArrayList<StructField> struct_fields = new ArrayList<StructField>();
        struct_fields.add(DataTypes.createStructField("id", DataTypes.IntegerType, true));
        struct_fields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        struct_fields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
        StructType structType = DataTypes.createStructType(struct_fields);

        Dataset<Row> df = sqlContext.createDataFrame(studentRDD, structType);
        df.registerTempTable("students");
        sqlContext.sql("select * from students").show();
        Dataset<Row> result = sqlContext.sql("select * from students where age <=20");
        result.javaRDD().saveAsTextFile("/home/wangheng/Desktop/result");

        sc.close();
    }
}
