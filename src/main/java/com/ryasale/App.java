package com.ryasale;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * start point
 *
 * @author Alexander Ryasnyanskiy
 *         created on 20.02.17
 */
@Slf4j
public class App {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("SparkHttpReceiver");

        int port = 8080;
        String servletPatter = "/event/*";

        JavaStreamingContext jsc = new JavaStreamingContext(sparkConf, Durations.seconds(5));

        JavaReceiverInputDStream<String> events = jsc.receiverStream(new HttpReceiver(port, servletPatter)); // initialize receiver
        events.print(); // spark action

        jsc.start(); // starts the computation
        try {
            jsc.awaitTermination();
        } catch (InterruptedException e) {
            log.error("app error", e);
        }

    }

}
