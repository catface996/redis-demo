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
public class TestGroupToMemberBit {


  @Autowired
  private StringRedisTemplate stringRedisTemplate;

  @Test
  public void test_1500_0000() {
    int byteNums = 1500 * 10000 / 8;
    byte[] data = new byte[byteNums];
    for (int i = 0; i < byteNums; i++) {
      data[i] = 0;
    }
    data[0] = 7;
    String memberIndexBitArr = new String(data);
    for (int i = 0; i < 100; i++) {
      String key = "group-" + i;
      stringRedisTemplate.opsForValue().set(key, memberIndexBitArr);
    }
  }

  @Test
  public void test_1_0000_0000() {
    int byteNums = 10000 * 10000 / 8;
    byte[] data = new byte[byteNums];
    for (int i = 0; i < byteNums; i++) {
      data[i] = 0;
    }
    data[0] = 7;
    String memberIndexBitArr = new String(data);
    for (int i = 0; i < 100; i++) {
      String key = "group-" + i;
      stringRedisTemplate.opsForValue().set(key, memberIndexBitArr);
    }
  }

  @Test
  public void test_2() {
    for (int i = 0; i < 16; i++) {
      Boolean result = stringRedisTemplate.opsForValue().getBit("group-12", i);
      log.info("result:{}", result);
    }

  }

}
