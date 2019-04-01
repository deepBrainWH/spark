package com.wangheng.core.day03;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;
import java.util.Arrays;
import java.util.Iterator;

/**
 * 本地测试wordcount程序
 */
public class WordCountLocal {
    public static void main(String[] args) {
        //Write spark application.
        //1.first step: create 'SparkConf' object, setting the configuration of spark application.
        //Using setMaster() to set the url of spark cluster's master,if using local, run in local host.
        SparkConf conf = new SparkConf().setAppName("WordCountLocal")
                .setMaster("local");
        //2.Second step, create JavaSparkContext object.
        //In Spark, SparkContext is the entire object of all function, weather you use java,
        //scala, or python.
        JavaSparkContext sc = new JavaSparkContext(conf);
        //3.Third step. create a RDD object for input source.
        //The data of input source will be shuffle, which being allocated each partition and
        //
        JavaRDD<String> lines = sc.textFile("/home/wangheng/Desktop/spark_test_data.txt", 1);
        //4.forth step, transformation operation.

        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });
        JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word, 1);
            }
        });

        //以单词作为key，统计每个key出现的次数
        final JavaPairRDD<String, Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        //使用action操作，foreach，触发程序的执行
        wordCounts.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> wordCount) throws Exception {
                System.out.println(wordCount._1+" appeared " + wordCount._2 + " times.");
            }
        });
        sc.close();
    }
}
