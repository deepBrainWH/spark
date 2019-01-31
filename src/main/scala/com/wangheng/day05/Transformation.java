package com.wangheng.day05;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Transformation 操作实战
 */
public class Transformation {
    public static void main(String[] args) {
//        map();
//        filter();
//        flatMap();
        groupByKey();
    }

    /**
     * map算子案例额，将集合中每个元素都乘以2
     */
    private static void map(){
        SparkConf conf = new SparkConf()
                .setAppName("map option")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> numbers = Arrays.asList(1,2,3,4,5,6,7);
        JavaRDD<Integer> numberRDD = sc.parallelize(numbers);
        //使用map算子将集合中的每个元素都乘以二
        //map算子，是对任何类型的RDD都可以调用的。
        //再java中，map算子接受的参数
        JavaRDD<Integer> multipleNumberRDD = numberRDD.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer integer) throws Exception {
                return integer * 2;
            }
        });
        //print new RDD
        multipleNumberRDD.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });
        sc.close();
    }

    /**
     *filter算子案例，过滤掉集合中的偶数。
     */
    private static void filter(){
        SparkConf conf = new SparkConf()
                .setAppName("map option")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> numbers = Arrays.asList(1,2,3,4,5,6,7);
        JavaRDD<Integer> numberRDD = sc.parallelize(numbers);

        //使用filter算子过滤掉集合中的偶数。
        //filter传入的也是Function，
        //如果想保留这个元素，return true, else return false.
        JavaRDD<Integer> filterRDD = numberRDD.filter(new Function<Integer, Boolean>() {
            @Override
            public Boolean call(Integer integer) throws Exception {
                return integer % 2 == 0;
            }
        });
        filterRDD.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });
        sc.close();
    }

    /**
     * 将文本行拆分为多个单词
     */
    private static void flatMap(){
        SparkConf conf = new SparkConf()
                .setAppName("map option")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("/home/wangheng/Desktop/test_data/spark_test_data.txt", 1);
        //对RDD执行flatMap算子，讲每一行文本拆分为多个单词。
        JavaRDD<String> stringJavaRDD = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });
        stringJavaRDD.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
        sc.close();
    }

    /**
     * groupByKey, 按照班级对成绩进行分组。
     */
    private static void groupByKey(){
        SparkConf conf = new SparkConf()
                .setAppName("map option")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        final List<Tuple2<String, Integer>> score = Arrays.asList(
                new Tuple2<String, Integer>("class1", 90),
                new Tuple2<String, Integer>("class2", 78),
                new Tuple2<String, Integer>("class1", 67),
                new Tuple2<String, Integer>("class3", 99));

        JavaPairRDD<String, Integer> scoresrdd = sc.parallelizePairs(score);
        //针对scoresRDD执行groupBykey
        JavaPairRDD<String, Iterable<Integer>> group_scores = scoresrdd.groupByKey();

        //println group scores.
        group_scores.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            @Override
            public void call(Tuple2<String, Iterable<Integer>> stringIterableTuple2) throws Exception {
                System.out.println("class: "+ stringIterableTuple2._1);
                for (Integer integer : stringIterableTuple2._2()) {
                    System.out.println(integer);
                }
            }
        });

        sc.close();
    }
}
