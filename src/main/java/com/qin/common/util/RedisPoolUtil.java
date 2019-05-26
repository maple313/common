package com.qin.common.util;


import org.springframework.beans.factory.annotation.Value;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @author: WangZB
 * @date: 2019/5/26 19:53
 * @description:
 * @version: 1.0
 */
public class RedisPoolUtil {

    @Value("${spring.redis.host}")
    static String host;
    @Value("${spring.redis.port}")
    static String port;
    @Value("${spring.redis.timeout}")
    static String timeout;
    @Value("${spring.redis.password}")
    static String password;

    private static JedisPool jedisPool;

    static{
        JedisPoolConfig jedisPoolConfig=new JedisPoolConfig();
        int intPort = Integer.parseInt(port);
        int intTimeout = Integer.parseInt(timeout);
        jedisPool=new JedisPool(jedisPoolConfig,host,intPort,intTimeout,password);
    }

    public static Jedis getJedis(){
        Jedis jedis=jedisPool.getResource();
        return jedis;
    }

    public static void cloiseJedis(Jedis jedis){
        jedis.close();
    }
}
