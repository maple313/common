package com.qin.common.redis;

import org.apache.logging.log4j.core.Logger;
import org.redisson.Redisson;
import org.redisson.api.RLock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * @author: WZB
 * @date: 2019/7/25 22:43
 * @description:
 * @version: 1.0
 */
public class RedisDistributeLock {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;
    @Autowired
    private Redisson redisson;
    @Autowired
    private Logger logger;

    /**
     *redis分布式锁的实现
     * 缺点: 无法合适的设置锁的超时时间
     *
     */
    public String distributeLockTest() {
        String lockKey = "product_id_lock";
        String threadId = UUID.randomUUID().toString();
        try {
            //1.设置redis的分布式锁，同时设置过期时间————原子性操作
            //threadId: 避免一个线程释放另一个线程设置的锁
            Boolean flag = stringRedisTemplate.opsForValue()
                    .setIfAbsent(lockKey, threadId, 10, TimeUnit.SECONDS);
            //2.如果锁已存在，等待
            if (!flag) {
                System.out.println("请等待......");
                return "failed";
            }
            //3.获取库存
            int productNumber = Integer.parseInt(stringRedisTemplate.opsForValue().get("storageNumber"));
            if (productNumber > 0) {
                int surplusNumber = productNumber - 1;
                stringRedisTemplate.opsForValue().set("storageNumber", surplusNumber + "");
                System.out.println("抢购失败");
                return "success";
            } else {
                System.out.println("抢购失败");
            }

        } catch (Exception e) {
            logger.error("抢购失败:", e.getMessage());
            return "failed";
        } finally {
            //4.释放锁
            if (threadId.equals(stringRedisTemplate.opsForValue().get("lockKey"))) {
                stringRedisTemplate.delete(lockKey);
            }
        }

        return "failed";
    }

    /**
     *redis分布式锁的实现
     * redisson:  redis的分布式开源框架
     *
     */
    public String distributeLockByRedissonTest() {
        String lockKey = "product_id_lock";
        //1.获取锁对象
        RLock rLock = redisson.getLock(lockKey);
        try {
            //2.加锁————获取库存
            rLock.tryLock(10,TimeUnit.SECONDS);
            int productNumber = Integer.parseInt(stringRedisTemplate.opsForValue().get("storageNumber"));
            if (productNumber > 0) {
                int surplusNumber = productNumber - 1;
                stringRedisTemplate.opsForValue().set("storageNumber", surplusNumber + "");
                System.out.println("抢购成功");
                return "success";
            } else {
                System.out.println("抢购失败");
            }

        } catch (Exception e) {
            logger.error("抢购失败:", e.getMessage());
            return "failed";
        } finally {
            //3.释放锁
           rLock.unlock();
        }

        return "failed";
    }

}
