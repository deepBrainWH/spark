package com.wangheng.streaming.day11;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * 实时wordcount程序
 */
public class WordCount {
    public static void main(String[] args) throws InterruptedException {
//        wordCountStreaming();
        wordCountHDFS();

    }

    /**
     * 基于socket的实时统计
     * @throws InterruptedException
     */
    private static void wordCountStreaming() throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("wordcount");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });
        JavaPairDStream<String, Integer> pair = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });
        JavaPairDStream<String, Integer> result = pair.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });
        result.print();
        Thread.sleep(5000);

        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }

    /**
     * 基于HDFS的，实际工作原理：监控一个hdfs文件目录，如果有新的文件出现，就执行计算。
     */
    private static void wordCountHDFS() throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("HDFS spark_streaming").setMaster("local[*]");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));

        JavaDStream<String> lines = jsc.textFileStream("hdfs://localhost:9000/word_count_dir");
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });
        JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        JavaPairDStream<String, Integer> result_pair = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        result_pair.print();

        jsc.start();
        jsc.awaitTermination();
        jsc.stop();
    }
}
