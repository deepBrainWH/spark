package com.wangheng.core.day07;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;
import scala.Tuple3;

import java.util.List;

/**
 * 取最大前三个数值。
 */
public class TopN {
    public static void main(String[] args) {
//        simpleTopN();
        groupedTopN(4);

    }

    private static void simpleTopN(){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("topN");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("/home/wangheng/Desktop/test_data/test_data6.txt", 1);
        JavaPairRDD<Integer, Integer> pair = lines.mapToPair(new PairFunction<String, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(String s) throws Exception {
                return new Tuple2<Integer, Integer>(Integer.valueOf(s), Integer.valueOf(s));
            }
        });
        JavaPairRDD<Integer, Integer> sorted = pair.sortByKey(false);
        List<Tuple2<Integer, Integer>> result = sorted.take(3);
        for(Tuple2<Integer, Integer> a: result){
            System.out.println(a._1);
        }
        sc.close();
    }

    private static void groupedTopN(int n){
        /**
         * 取出每个班级前三名的成绩。
         */
        SparkConf conf = new SparkConf().setMaster("local").setAppName("groupTopN");
        JavaSparkContext sc = new JavaSparkContext(conf);
        final Broadcast<Integer> broadcast = sc.broadcast(n);
        JavaRDD<String> lines = sc.textFile("/home/wangheng/Desktop/test_data/test_data7.txt", 1);
        JavaRDD<Tuple3<String, String, Integer>> pair = lines.map(new Function<String, Tuple3<String, String, Integer>>() {
            @Override
            public Tuple3<String, String, Integer> call(String s) throws Exception {
                return new Tuple3<String, String, Integer>(s.split(" ")[0], s.split(" ")[1], Integer.valueOf(s.split(" ")[2]));
            }
        });
        JavaRDD<Tuple3<String, String, Integer>> result = pair.sortBy(new Function<Tuple3<String, String, Integer>, Integer>() {
            @Override
            public Integer call(Tuple3<String, String, Integer> stringStringIntegerTuple3) throws Exception {
                return stringStringIntegerTuple3._3();
            }
        }, false, 1);
        JavaPairRDD<String, Iterable<Tuple3<String, String, Integer>>> result1 = result.groupBy(new Function<Tuple3<String, String, Integer>, String>() {
            @Override
            public String call(Tuple3<String, String, Integer> stringStringIntegerTuple3) throws Exception {
                return stringStringIntegerTuple3._1();
            }
        });
        result1.foreach(new VoidFunction<Tuple2<String, Iterable<Tuple3<String, String, Integer>>>>() {
            @Override
            public void call(Tuple2<String, Iterable<Tuple3<String, String, Integer>>> stringIterableTuple2) throws Exception {
                System.out.println(stringIterableTuple2._1);
                int a = 0;
                for (Tuple3<String, String, Integer> iterable: stringIterableTuple2._2){
                    if(a==broadcast.value())break;
                    System.out.println(iterable._2() +" : "+iterable._3());
                    a++;
                }
                System.out.println("===================================");
            }
        });
        sc.close();
    }
}