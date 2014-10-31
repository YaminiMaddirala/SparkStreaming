package com.spark.streaming.kafka;

import com.google.common.collect.Lists;
import kafka.consumer.ConsumerConfig;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Reads messages from locally installed kafka broker and prints the word count from a topic.
 *
 * Created by ymaddirala on 10/13/14.
 */

public class WordCount {
    private static ConsumerConfig consumerConfig;
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args){


        // Local streaming context with working nodes and the batch interval of 5sec.
        // AppName: Setting application as kafkaStreaming.
        // Master url to connect {locally with 2 cores}
        SparkConf sparkConf = new SparkConf().setAppName("KafkaStreaming ").setMaster("local[2]");
        final JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, new Duration(5000));


        // Map includes the topic created and used in kafka broker.
        Map<String, Integer> topicMap = new HashMap<String, Integer>();
        topicMap.put("YaminiTest", 1);

        JavaPairReceiverInputDStream<String, String> messages =
                KafkaUtils.createStream(streamingContext, "localhost:2181", "sampler", topicMap);


        // Redoing the DStreams set which contains only the messages (Value part of teh key value combination.)
        JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> stringStringTuple2) throws Exception {
                return stringStringTuple2._2();
            }
        });

        //
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Lists.newArrayList(SPACE.split(s));
            }
        });


        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String s) {
                        return new Tuple2<String, Integer>(s, 1);
                    }
                }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
            }
        });

        wordCounts.print();
        streamingContext.start();
        streamingContext.awaitTermination();

    }
}



