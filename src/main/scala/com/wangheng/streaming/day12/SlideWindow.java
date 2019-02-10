package com.wangheng.streaming.day12;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.List;

/**
 * Spark Streaming 滑动窗口机制统计热点词汇
 */
public class SlideWindow {
    public static void main(String[] args) throws InterruptedException {
        hotWord();
    }

    private static void hotWord() throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("slide window").setMaster("local[2]");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        /**
         * tom hello
         * wanheng word
         * zhanghuan kkei
         */
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);
        JavaPairDStream<String, Integer> pair = lines.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s.split(" ")[1], 1);
            }
        });
        JavaPairDStream<String, Integer> result = pair.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        }, Durations.seconds(60), Durations.seconds(10));
        //找到排名前三的热点词
        JavaPairDStream<String, Integer> result_ = result.transformToPair(new Function<JavaPairRDD<String, Integer>, JavaPairRDD<String, Integer>>() {
            @Override
            public JavaPairRDD<String, Integer> call(JavaPairRDD<String, Integer> searchWordCountRDD) throws Exception {
                JavaPairRDD<Integer, String> countSearchWord = searchWordCountRDD.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
                    @Override
                    public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                        return new Tuple2<>(stringIntegerTuple2._2, stringIntegerTuple2._1);
                    }
                });
                JavaPairRDD<Integer, String> sortedRDD = countSearchWord.sortByKey(false);
                //再次执行翻转
                JavaPairRDD<String, Integer> final_result = sortedRDD.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(Tuple2<Integer, String> integerStringTuple2) throws Exception {
                        return new Tuple2<>(integerStringTuple2._2, integerStringTuple2._1);
                    }
                });
                List<Tuple2<String, Integer>> take = final_result.take(3);
                for (Tuple2<String, Integer> a : take) {
                    System.out.println(a._1 + ":" + a._2);
                }
                return final_result;
            }
        });
        result_.print();//这行代码无关紧要，只是为了触发job,所以必须要output操作。wanghengddkd
        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }
}
