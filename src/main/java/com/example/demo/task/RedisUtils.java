package com.example.demo.task;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.SetOperations;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
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

    public static Set<Object> sGet(RedisTemplate redisTemplate, String key){
        try{
            return redisTemplate.opsForSet().members(key);
        }catch (Exception e){
            e.printStackTrace();
            return null;
        }
    }

    public static boolean sRemoveMember(RedisTemplate redisTemplate, String key, Object...values){
        redisTemplate.opsForSet().remove(key,values);
        return true;
    }

    public static boolean sHasMember(RedisTemplate redisTemplate, String key, Object value){
        try{
            return redisTemplate.opsForSet().isMember(key, value);
        }catch (Exception e){
            e.printStackTrace();
            return false;
        }
    }

    public static long sSet(RedisTemplate redisTemplate, String key, Object...values){
        try{
            redisTemplate.opsForSet().add(key,values);
            return values.length;
        }catch (Exception e){
            e.printStackTrace();
            return 0;
        }
    }

    public static Set<Object> sScan(RedisTemplate redisTemplate, String key, String valueMatch){
        try{
            Set<Object> matchValues = new HashSet<>();
            Cursor<Object> cursor = redisTemplate.opsForSet().scan(key,
                    new ScanOptions.ScanOptionsBuilder().match(valueMatch).count(1).build());
            while (cursor.hasNext()){
                matchValues.add(cursor.next());
            }
            return matchValues;
        }catch (Exception e){
            e.printStackTrace();
            return null;
        }
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
        //System.out.println("hHasKey: "+redisTemplate.opsForHash().hasKey(key, item)+" end");
        return redisTemplate.opsForHash().hasKey(key, item);
    }
}
