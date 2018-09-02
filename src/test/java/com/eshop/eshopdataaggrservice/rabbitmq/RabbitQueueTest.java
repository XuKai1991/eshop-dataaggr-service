package com.eshop.eshopdataaggrservice.rabbitmq;

import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.junit4.SpringRunner;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @author: Xukai
 * @description:
 * @createDate: 2018/8/29 22:39
 * @modified By:
 */
@RunWith(SpringRunner.class)
@SpringBootTest
@Slf4j
public class RabbitQueueTest {

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Test
    public void test() {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(100);
        config.setMaxIdle(5);
        config.setMaxWaitMillis(1000 * 10);
        config.setTestOnBorrow(true);
        JedisPool jedisPool = new JedisPool(config, "192.168.1.10", 1111);
        Jedis jedis = jedisPool.getResource();
        jedis.set("a", "av1");
        String value = jedis.get("a");
        log.info("value:{}", value);
    }

    @Test
    public void test2(){
        // redisTemplate.opsForValue().set("b","bv1");
        String value = redisTemplate.opsForValue().get("b");
        log.info("value:{}", value);
    }

}