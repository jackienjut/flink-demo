package com.jackie.streaming.kafaka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import sun.java2d.pipe.SpanShapeRenderer;

import java.util.Properties;

public class StreamingKafkaSink {

    public static void main(String args[]) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String hostname = "192.168.3.130";
        int port;
        try {
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            port = parameterTool.getInt("port");
        } catch (Exception e) {
            System.err.println("not set the port , use the default port -- java");
            port = 9000;
        }

        DataStreamSource<String> text = env.socketTextStream(hostname, port, "\n");

        String topic = "test";
        String brokerKList = "192.168.3.128:9092,192.168.3.129:9092,192.168.3.127:9092";

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.3.128:9092,192.168.3.129:9092,192.168.3.127:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test");

        FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<String>(brokerKList, topic, new SimpleStringSchema());


        text.print();
        text.addSink(myProducer);

        env.execute("add");
    }
}
