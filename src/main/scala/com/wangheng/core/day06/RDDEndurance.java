package com.wangheng.core.day06;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * RDD持久化操作。
 */
public class RDDEndurance {
    public static void main(String[] args) {
//        rddPersist();
//        shareVariable_1();
        accumulator();
    }

    private static void rddPersist(){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("rdd endurance");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //注意：cache()只能再创建RDD的同一行执行cache()或者persist(),不能另起一行lines.cache().
        JavaRDD<String> lines = sc.textFile("/home/wangheng/Desktop/test_data/test_data2.txt").cache();
        long beginTime = System.currentTimeMillis();
        long count = lines.count();
        long endTime = System.currentTimeMillis();
        System.out.println(count);
        System.out.println("cost : " + (endTime - beginTime) + " ms");

        beginTime = System.currentTimeMillis();
        lines.count();
        endTime = System.currentTimeMillis();
        System.out.println("cost : " + (endTime - beginTime) + " ms");
        sc.close();
    }

    /**
     * spark share variable -1 : brodCast
     */
    private static void shareVariable_1(){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("rdd endurance");
        JavaSparkContext sc = new JavaSparkContext(conf);
        final JavaRDD<String> lines = sc.textFile("/home/wangheng/Desktop/test_data/test_data3.txt", 1);
        final int factor = 30;
        final Broadcast<Integer> share_variable = sc.broadcast(factor);

        JavaRDD<Integer> integerJavaRDD = lines.flatMap(new FlatMapFunction<String, Integer>() {
            @Override
            public Iterator<Integer> call(String s) throws Exception {
                String[] number_str = s.split(" ");
                List<Integer> list = new ArrayList<Integer>();
                for (String num : number_str) {
                    list.add(Integer.parseInt(num));
                }
                return list.iterator();
            }
        });
        JavaRDD<Integer> map = integerJavaRDD.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer integer) throws Exception {
                return integer * share_variable.value();
            }
        });
        map.saveAsTextFile("/home/wangheng/Desktop/park-1111");
        map.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });
        sc.close();
    }

    /**
     * Spark accumulator. accumulator 累加，每个task只能对这个元素进行累加，不能读取它的值。
     * 只有driver能够读取它的值。
     */
    private static void accumulator(){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("accumulator");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("/home/wangheng/Desktop/test_data/test_data3.txt", 1);
        JavaRDD<Integer> numbersRDD = lines.flatMap(new FlatMapFunction<String, Integer>() {
            @Override
            public Iterator<Integer> call(String s) throws Exception {
                String[] str_num = s.split(" ");
                ArrayList<Integer> num = new ArrayList<Integer>();
                for (String num_ : str_num) {
                    num.add(Integer.parseInt(num_));
                }
                return num.iterator();
            }
        });
        final Accumulator<Integer> accumulator = sc.accumulator(0);
        numbersRDD.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                accumulator.add(integer);
            }
        });

        System.out.println(accumulator.value());

        sc.close();
    }
}
