package com.jackie.flinkclean;

import com.jackie.flinkclean.flinksource.RedisSource;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class DataClean {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String topic = "allData";
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.3.128:9092,192.168.3.129:9092,192.168.3.127:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test1");

        FlinkKafkaConsumer fkc = new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), props);

        //{"dt","","countryCode":""}
        DataStreamSource<String> data = env.addSource(fkc);

        DataStreamSource<Map<String, String>> mapSource = env.addSource(new RedisSource());
        data.connect(mapSource).flatMap(new CoFlatMapFunction<String, Map<String, String>, String>() {
            private Map<String, String> mapSource = new HashMap<>();

            // data中的数据
            @Override
            public void flatMap1(String value, Collector<String> out) throws Exception {

            }

            //map 类型中的数据
            @Override
            public void flatMap2(Map<String, String> value, Collector<String> out) throws Exception {
                this.mapSource = value;
            }
        });


        env.execute("Data Clean");
    }
}
