package com.wangheng.streaming.day12;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Update state by key, 为每一个key维护一份state,并不断更新该state
 */
public class UpdataStateByKeyOperation {
    public static void main(String[] args) throws InterruptedException {
        updateStateByKeyWordCount();
    }

    private static void updateStateByKeyWordCount() throws InterruptedException {
        SparkConf conf =  new SparkConf().setAppName("updateStateBykey");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        jssc.checkpoint("hdfs://localhost:9000/word_count_checkpoint");

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
                return new Tuple2<>(s, 1);
            }
        });

        /**
         * 参数说明：Function2()第一个参数是传进来的batch中key的值，可能有多个，比如一个hello可能有两个1（hello , 1), (hello , 1)
         * 第二个参数是key之前的状态，state,其中的泛型为自己指定。
         *
         */
        JavaPairDStream<String, Integer> result = pair.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
            @Override
            public Optional<Integer> call(List<Integer> values, Optional<Integer> state) throws Exception {
                //首先定义一个全局的单词计数
                Integer newValue = 0;

                //其次判断state是否存在,如果不存在，说明是一个key第一次出现
                //如果存在，则说明这个key之前已经统计过全局计数了。
                if (state.isPresent()) {
                    newValue = state.get();
                }
                //接着将本次出现的值都累加到newValue上去，就是一个key目前的全局统计次数。（等价于使用structured Streaming)
                for (Integer v : values) {
                    newValue += v;
                }
                return Optional.of(newValue);
            }
        });
        result.print();

        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }
}
