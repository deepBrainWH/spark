package com.wangheng.core.day05;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
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
//        groupByKey();
//        reduceByKey();
//        sortByKey();
        join();
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

    /**
     * reduceByKey: static each class's all score.
     */
    private static void reduceByKey(){
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
        /**
         * reduceBykey接受的参数是Functon2类型，它有三个泛型参数，实际上代表了三个只
         * 第一个和第二个代表的是原始RDD的输入值
         * 第三个反省参数类型代表的是返回值的类型。
         */
        JavaPairRDD<String, Integer> totalscores = scoresrdd.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        totalscores.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                System.out.println(stringIntegerTuple2._1() + " all scores is :" + stringIntegerTuple2._2());
            }
        });
    }

    /**
     * SortByKey
     */
    private static void sortByKey(){
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("SortByKey");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Tuple2<Integer, String>> scores = Arrays.asList(new Tuple2<Integer, String>(89, "xiaoming"),
                new Tuple2<Integer, String>(84, "wangheng"),
                new Tuple2<Integer, String>(99, "huanhuan"));
        JavaPairRDD<Integer, String> scoresRDD = sc.parallelizePairs(scores);
        JavaPairRDD<Integer, String> sorted_scores = scoresRDD.sortByKey(true);
        sorted_scores.foreach(new VoidFunction<Tuple2<Integer, String>>() {
            @Override
            public void call(Tuple2<Integer, String> integerStringTuple2) throws Exception {
                System.out.println(integerStringTuple2._1 + " : " + integerStringTuple2._2);
            }
        });
        sc.close();
    }

    /**
     * Join 关联两个RDD
     * join example.
     */
    private static void join(){
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("SortByKey");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Tuple2<Integer, String>> student_list1 = Arrays.asList(
                new Tuple2<Integer, String>(1, "s1"),
                new Tuple2<Integer, String>(2, "s2"),
                new Tuple2<Integer, String>(3, "s3"),
                new Tuple2<Integer, String>(4, "s4"));

        List<Tuple2<Integer, Integer>> student_list2 = Arrays.asList(
                new Tuple2<Integer, Integer>(1, 100),
                new Tuple2<Integer, Integer>(2, 89),
                new Tuple2<Integer, Integer>(3, 87),
                new Tuple2<Integer, Integer>(5, 88));

        //并行化两个RDD
        JavaPairRDD<Integer, String> student = sc.parallelizePairs(student_list1);
        JavaPairRDD<Integer, Integer> scores = sc.parallelizePairs(student_list2);

        //join会根据key进行join返回javaPairRDD
        JavaPairRDD<Integer, Tuple2<String, Integer>> join = student.join(scores);
        join.foreach(new VoidFunction<Tuple2<Integer, Tuple2<String, Integer>>>() {
            @Override
            public void call(Tuple2<Integer, Tuple2<String, Integer>> integerTuple2Tuple2) throws Exception {
                System.out.println("student's id:" + integerTuple2Tuple2._1 + " student's name : " + integerTuple2Tuple2._2._1
                + " student's grade : " + integerTuple2Tuple2._2._2);
            }
        });
        sc.close();
    }

    /**
     * cogroup example.
     */
    private static void cogroup(){

    }

}
