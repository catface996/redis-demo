package com.catface.redis.service;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;

/**
 * @author catface
 * @since 2022/11/8
 */
@Slf4j
@SpringBootTest
public class TestMemberToGroup {

  @Autowired
  private StringRedisTemplate stringRedisTemplate;

  @Test
  public void test() {
    int round = 0;
    String value = new String(new byte[1000]);
    for (int i = 0; i < 15000000; i++) {
      String key = (1000000000 + i) + ":t";
      stringRedisTemplate.opsForValue().set(key, value);
      int cRound = i / 100000;
      if (cRound > round) {
        round = cRound;
        log.info("保存到第{}个", i);
      }
    }
    log.info("finish");
  }

  @Test
  public void testClean() {
    int round = 0;
    String value = new String(new byte[1000]);
    for (int i = 0; i < 15000000; i++) {
      String key = (1000000000 + i) + ":t";
      stringRedisTemplate.delete(key);
      int cRound = i / 100000;
      if (cRound > round) {
        round = cRound;
        log.info("删除到第{}个", i);
      }
    }
    log.info("finish");
  }
}
