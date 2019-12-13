package com.jackie.flinkclean.flinksource;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.HashMap;
import java.util.Map;

/**
 * redis 中进行数据初始化。
 */
public class RedisSource implements SourceFunction<Map<String, String>> {
    private Logger logger = LoggerFactory.getLogger(RedisSource.class);

    private boolean isRunning = true;
    private Jedis jedis = null;
    private final long sleep = 60 * 1000;

    @Override
    public void run(SourceContext<Map<String, String>> ctx) throws Exception {
        Map<String, String> map = new HashMap<>();

        jedis = new Jedis("47.94.138.118", 6379);

        while (isRunning) {
            try {
                map.clear();
                Map<String, String> ares = jedis.hgetAll("ares");

                for (Map.Entry<String, String> entry : ares.entrySet()) {
                    String key = entry.getKey();
                    String value = entry.getValue();
                    String[] splits = value.split(",");

                    for (String split : splits) {
                        map.put(split, key);
                    }
                }
                if (map.size() > 0) {
                    ctx.collect(map);
                } else {

                }
                Thread.sleep(sleep);
            } catch (JedisConnectionException e) {
                jedis = new Jedis("47.94.138.118", 6379);
            } catch (Exception e) {
                logger.warn("get data from redis is null");
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
        if (jedis != null) {
            jedis.close();
        }
    }
}
