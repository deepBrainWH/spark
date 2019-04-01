package com.wangheng.streaming.day12;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

public class WordCountKafka {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("WordCountKafka");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(8));

//        KafkaUtils.createDirectStream(jssc,"localhost:2181", "1", "top1 top2");


        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }
}
