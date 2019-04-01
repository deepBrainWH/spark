package com.wangheng.core.day06;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

public class SecondarySort {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("second_sort");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("/home/wangheng/Desktop/test_data/test_data5.txt", 1);
        JavaPairRDD<SecondarySortKey, String> pair = lines.mapToPair(new PairFunction<String, SecondarySortKey, String>() {
            @Override
            public Tuple2<SecondarySortKey, String> call(String s) throws Exception {
                String[] num = s.split(" ");
                SecondarySortKey secondarySortKey = new SecondarySortKey(
                        Integer.valueOf(num[0]), Integer.valueOf(num[1]));
                return new Tuple2<SecondarySortKey, String>(secondarySortKey, s);
            }
        });

        JavaPairRDD<SecondarySortKey, String> result = pair.sortByKey(false);
        result.foreach(new VoidFunction<Tuple2<SecondarySortKey, String>>() {
            @Override
            public void call(Tuple2<SecondarySortKey, String> secondarySortKeyStringTuple2) throws Exception {
                System.out.println(secondarySortKeyStringTuple2._2);
            }
        });
        sc.close();
    }
}
