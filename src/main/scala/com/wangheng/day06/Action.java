package com.wangheng.day06;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Action operation 实战
 */
public class Action {
    public static void main(String[] args) {
//        reduce();
//        collect();
//        count();
//        take();
//        saveAsTextFile();
        countByKey();
    }


    private static void reduce(){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("reduce");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> numbers = Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> numbersRDD = sc.parallelize(numbers);
        Integer result = numbersRDD.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });
        System.out.println(result + "\n========================");
        sc.close();
    }

    /**
     * 不用foreach aciton操作，在远程集群上遍历rdd中的元素
     * 使用collection操作，讲分布在远程集群上的doubleNumber Rdd的数据
     * 拉取到本地。这种方式一般不建议使用。因为如果rdd中的数据量较大，超过
     * 数万条时，性能会比较差，因为要从远程走大量网络传输，讲数据拉取到本地。
     *
     * 此外甚至会出现oom内存溢出异常。
     */
    private static void collect(){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("collect");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> numbers = Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> numbersRDD = sc.parallelize(numbers);
        //使用map将集合中的所有元素乘以2
        JavaRDD<Integer> result = numbersRDD.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer integer) throws Exception {
                return integer * 2;
            }
        });
        System.out.println(result.collect());
        sc.close();
    }

    /**
     * count action,对rdd进行count操作
     */
    private static void count(){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("count");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> numbers = Arrays.asList(1,2,3,4,5, 3, 4, 6);
        JavaRDD<Integer> numbersRDD = sc.parallelize(numbers);
        long result = numbersRDD.count();
        System.out.println(result);
        sc.close();
    }

    /**
     * take operation.take operation is similar to collect() operation.
     */
    private static void take(){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("take");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> numbers = Arrays.asList(1,2,3,4,5, 3, 4, 6);
        JavaRDD<Integer> numbersRDD = sc.parallelize(numbers);
        List<Integer> takes = numbersRDD.take(4);
        for ( Integer num: takes){
            System.out.println(num);
        }
        sc.close();
    }

    /**
     * Save as text file.,需要打包到集群上运行
     */
    private static void saveAsTextFile(){
        SparkConf conf = new SparkConf().setAppName("saveAsTextFile");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> numbers = Arrays.asList(1,2,3,4,5, 3, 4, 6);
        JavaRDD<Integer> numbersRDD = sc.parallelize(numbers, 1);
        JavaRDD<Integer> resultRDD = numbersRDD.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer integer) throws Exception {
                return integer * 2;
            }
        });
        resultRDD.saveAsTextFile("hdfs://localhost:9000/double_number");
        sc.close();
    }

    /**
     * countByKey action.
     */
    private static void countByKey(){
        SparkConf conf = new SparkConf().setAppName("saveAsTextFile").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<String, String>> students = Arrays.asList(
                new Tuple2<String, String>("class1", "wangheng"),
                new Tuple2<String, String>("class1", "huanhuan"),
                new Tuple2<String, String>("class2", "liukaowen"),
                new Tuple2<String, String>("class3", "fangyong")
        );
        JavaPairRDD<String, String> studentRDD = sc.parallelizePairs(students, 1);
        Map<String, Long> result = studentRDD.countByKey();
        for(Map.Entry<String, Long> studentCount: result.entrySet()){
            System.out.println(studentCount.getKey() + " : " + studentCount.getValue());
        }
        sc.close();
    }

}
