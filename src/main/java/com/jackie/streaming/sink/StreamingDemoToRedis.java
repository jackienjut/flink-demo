package com.jackie.streaming.sink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * 接受socket数据数据保存在redis中。
 * <p>
 * list
 * <p>
 * lpush list_key
 */
public class StreamingDemoToRedis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> text = env.socketTextStream("192.168.3.130", 9000, "\n");

        //lput 1_word

        // String 转为tuple2
        DataStream<Tuple2<String, String>> data = text.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                return new Tuple2<>("1_words", value);
            }
        });

        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("47.94.138.118").setPort(6379).build();

        RedisSink sink = new RedisSink<Tuple2<String, String>>(conf, new MyRedisMapper());

        data.addSink(sink);
        env.execute("redis sink");
    }

    public static class MyRedisMapper implements RedisMapper<Tuple2<String, String>> {
        //
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.LPUSH);
        }

        //get redis key
        @Override
        public String getKeyFromData(Tuple2<String, String> stringStringTuple2) {
            return stringStringTuple2.f0;
        }

        //get redis value
        @Override
        public String getValueFromData(Tuple2<String, String> stringStringTuple2) {
            return stringStringTuple2.f1;
        }
    }
}
