package com.qin.common.util;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.HashSet;
import java.util.Set;

/**
 * @author: WangZB
 * @date: 2019/5/26 20:42
 * @description:
 * @version: 1.0
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class RedisTemplateTest {

    @Autowired
    RedisTemplate redisTemplate;

    @Test
    public void redisTemplateTest(){
        int i=0;
        redisTemplate.setKeySerializer(RedisSerializer.string());
        redisTemplate.setValueSerializer(RedisSerializer.java());
        Set<String> set=new HashSet<>();
        set.add("1");
        set.add("2");
        set.add("3");
        redisTemplate.opsForSet().intersect("set1",set);
        System.out.println(redisTemplate);
    }
}
