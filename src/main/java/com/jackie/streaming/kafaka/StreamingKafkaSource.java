package com.jackie.streaming.kafaka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class StreamingKafkaSource {

    public static void main(String args[]) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //    env.addSource(new Kafa)

        String topic = "test";
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.3.128:9092,192.168.3.129:9092,192.168.3.127:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG ,"test") ;


        FlinkKafkaConsumer<String> myconsumer = new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), props);

        DataStreamSource<String> text = env.addSource(myconsumer);
        text.print();

        env.execute("add");
    }
}
