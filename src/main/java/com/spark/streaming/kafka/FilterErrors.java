package com.spark.streaming.kafka;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;
import java.util.HashMap;
import java.util.Map;

/**
 * Class reads streaming messages from kafka broker and prints the lines which have word "error" in them.
 *
 * Created by ymaddirala on 10/30/14.
 */
public class FilterErrors {


    public static void main(String[] args){


        // Local streaming context with working nodes and the batch interval of 5sec.
        // AppName: Setting application as kafkaStreaming.
        // Master url to connect {locally with 2 cores}
        SparkConf sparkConf = new SparkConf().setAppName("KafkaStreaming ").setMaster("local[2]");
        final JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, new Duration(5000));


        // Map includes the topic created and used in kafka broker.
        Map<String, Integer> topicMap = new HashMap<String, Integer>();
        topicMap.put("YaminiTest", 1);

        // Reading messages from kafka broker.
        JavaPairReceiverInputDStream<String, String> messages =
                KafkaUtils.createStream(streamingContext, "localhost:2181", "sampler", topicMap);


        JavaDStream<String> actualMessages = messages.map(new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> stringStringTuple2) throws Exception {
                return stringStringTuple2._2().toString();
            }
        });

        // Filtering lines with error words in them
        JavaDStream<String> errorDStreams = actualMessages.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                return s.contains("error");
            }
        });

        errorDStreams.print();
        errorDStreams.count();

        streamingContext.start();
        streamingContext.awaitTermination();

    }
}


