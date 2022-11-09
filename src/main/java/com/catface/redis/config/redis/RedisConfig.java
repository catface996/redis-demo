package com.catface.redis.config.redis;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializer;

/**
 * @author catface
 * @since 2022/11/8
 */
@Configuration
public class RedisConfig {

  @Autowired
  private RedisConnectionFactory redisConnectionFactory;

  @Bean
  public RedisTemplate<String,byte[]> byteRedisTemplate(){
    RedisTemplate<String,byte[]> redisTemplate = new RedisTemplate<>();
    redisTemplate.setConnectionFactory(redisConnectionFactory);
    redisTemplate.setKeySerializer(RedisSerializer.string());
    redisTemplate.setValueSerializer(RedisSerializer.byteArray());
    return redisTemplate;
  }

}
