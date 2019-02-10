package com.wangheng.streaming.day11;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.ArrayList;

/**
 * 基于transform的实时黑名单过滤
 */
public class TransformBlackList {
    public static void main(String[] args) throws InterruptedException {
        /**
         * 用户对于我们网站上的广告可以进行点击
         * 每点击一次要进行实时计费，但是对于那些帮助无良商家刷广告的人，那么我们有一个黑名单
         * 只要是黑名单中的用户电机的广告，我们就给过滤掉。
         */
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("transform_black_list");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);

        /**
         * 首先模拟出一份黑名单。
         */
        ArrayList<Tuple2<String, Boolean>> blackList = new ArrayList<>();
        blackList.add(new Tuple2<>("tom", true));
        blackList.add(new Tuple2<>("wangheng", true));
        blackList.add(new Tuple2<>("zhanghuan", true));
        blackList.add(new Tuple2<>("zhouaowei", true));
        blackList.add(new Tuple2<>("majunlong", true));
        JavaPairRDD<String, Boolean> blackListRDD = jssc.sparkContext().parallelizePairs(blackList);

        JavaPairDStream<String, String> pair = lines.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                return new Tuple2<>(s.split(" ")[1], s);
            }
        });
        JavaDStream<String> transform = pair.transform(new Function<JavaPairRDD<String, String>, JavaRDD<String>>() {
            @Override
            public JavaRDD<String> call(JavaPairRDD<String, String> userAdsClickLogRDD) throws Exception {
                JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> joinedRDD = userAdsClickLogRDD.leftOuterJoin(blackListRDD);
                JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> filterResult = joinedRDD.filter(new Function<Tuple2<String, Tuple2<String, Optional<Boolean>>>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, Tuple2<String, Optional<Boolean>>> tuple) throws Exception {
                        return !tuple._2._2().isPresent() || !tuple._2._2.get();
                    }
                });
                JavaRDD<String> map = filterResult.map(new Function<Tuple2<String, Tuple2<String, Optional<Boolean>>>, String>() {
                    @Override
                    public String call(Tuple2<String, Tuple2<String, Optional<Boolean>>> stringTuple2Tuple2) throws Exception {
                        return stringTuple2Tuple2._2._1;
                    }
                });
                return map;

            }
        });
        transform.print();

        /**
         * 对输入的数据进行一下转换操作，变成（username, date username)
         * 以便于后面对每个batchRDD与定义好的黑名单ＲＤＤ进行join操作。
         */
        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }
}
