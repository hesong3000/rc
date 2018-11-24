package com.example.demo.task;

import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.TimeUnit;

@Component
public final class RedisUtils {

    public static boolean expire(RedisTemplate<String, Object> redisTemplate, String key, long time){
        try{
            if (time > 0) {
                redisTemplate.expire(key, time, TimeUnit.SECONDS);
            }
            return true;
        }catch (Exception e){
            e.printStackTrace();
            return false;
        }
    }

    public static boolean hasKey(RedisTemplate redisTemplate, String key){
        try{
            return redisTemplate.hasKey(key);
        }catch (Exception e){
            e.printStackTrace();
            return false;
        }
    }

    public static void delKey(RedisTemplate redisTemplate, String key){
        redisTemplate.delete(key);
    }

    public static boolean set(RedisTemplate redisTemplate, String key, Object value){
        try{
            redisTemplate.opsForValue().set(key, value);
            return true;
        }catch (Exception e){
            e.printStackTrace();
            return false;
        }
    }

    public static boolean set(RedisTemplate redisTemplate, String key, Object value, long time) {
        try {
            if (time > 0) {
                redisTemplate.opsForValue().set(key, value, time, TimeUnit.SECONDS);
                return true;
            } else {
                return set(redisTemplate, key, value);
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public static Object get(RedisTemplate redisTemplate, String key){
        return redisTemplate.opsForValue().get(key);
    }

    public static Object hget(RedisTemplate redisTemplate, String key, String item){
        return redisTemplate.opsForHash().get(key, item);
    }

    public static Map<Object, Object> hmget(RedisTemplate redisTemplate, String key){
        return redisTemplate.opsForHash().entries(key);
    }

    public static long hsize(RedisTemplate redisTemplate, String key){
        return redisTemplate.opsForHash().size(key);
    }

    public static boolean hset(RedisTemplate redisTemplate, String key, String item, Object value){
        try{
            redisTemplate.opsForHash().put(key, item, value);
            return true;
        }catch (Exception e){
            e.printStackTrace();
            return false;
        }
    }

    public static void hdel(RedisTemplate redisTemplate, String key, String item){
        redisTemplate.opsForHash().delete(key,item);
    }

    public static boolean hHasKey(RedisTemplate redisTemplate, String key, String item){
        return redisTemplate.opsForHash().hasKey(key, item);
    }
}
