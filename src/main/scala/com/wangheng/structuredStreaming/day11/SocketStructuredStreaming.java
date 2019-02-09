package com.wangheng.structuredStreaming.day11;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.Arrays;

public class SocketStructuredStreaming {
    public static void main(String[] args) throws StreamingQueryException {
        SparkSession spark = SparkSession.builder()
                .appName("JavaStructuredNetwordWordCount")
                .master("local[2]")
                .getOrCreate();

        Dataset<Row> lines = spark.readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", 9999)
                .load();
        Dataset<String> words = lines.as(Encoders.STRING()).flatMap((FlatMapFunction<String, String>) x ->
                Arrays.asList(x.split(" ")).iterator(), Encoders.STRING());
        Dataset<Row> wordCounts = words.groupBy("value").count();

        StreamingQuery query = wordCounts.writeStream()
                .outputMode("complete")
                .format("console")
                .start();

        query.awaitTermination();
    }

}
