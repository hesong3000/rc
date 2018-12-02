package com.example.demo.config;

import com.alibaba.fastjson.parser.ParserConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import com.alibaba.druid.spring.boot.autoconfigure.DruidDataSourceBuilder;
import javax.sql.DataSource;
import java.sql.SQLException;

@Configuration
public class RedisConfiguration {

    @Bean
    public RedisSerializer<Object> fastJson2JsonRedisSerializer() {
        ParserConfig.getGlobalInstance().setAutoTypeSupport(true);
        return new FastJson2JsonRedisSerializer<Object>(Object.class);
    }

    @Bean
    public RedisTemplate<String, Object> redisTemplate(RedisConnectionFactory redisConnectionFactory) {
        RedisTemplate<String, Object> redisTemplate = new RedisTemplate<String, Object>();
        redisTemplate.setConnectionFactory(redisConnectionFactory);
        redisTemplate.setValueSerializer(fastJson2JsonRedisSerializer());
        redisTemplate.setKeySerializer(new StringRedisSerializer());
        redisTemplate.setHashKeySerializer(new StringRedisSerializer());
        redisTemplate.setHashValueSerializer(fastJson2JsonRedisSerializer());
        redisTemplate.afterPropertiesSet();
        //开启事务可能会报错，解决不了
        //redisTemplate.setEnableTransactionSupport(true);//打开事务支持
        return redisTemplate;
    }

    @Bean
    public PlatformTransactionManager transactionManager(DataSource dataSource) throws SQLException {
        return new DataSourceTransactionManager(dataSource);
    }

    @Bean
    public DataSource dataSource() {
        return DruidDataSourceBuilder.create().build();
    }


}
