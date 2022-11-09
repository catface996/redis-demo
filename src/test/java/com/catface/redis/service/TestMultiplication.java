package com.catface.redis.service;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

/**
 * @author catface
 * @since 2022/11/9
 */
@Slf4j
public class TestMultiplication {

  @Test
  public void test() {
    int a = 12345;
    int b = 0;
    long start = System.currentTimeMillis();
    for (long i = 0; i < 10000L * 10000 * 100; i++) {
      b = a * 10;
    }
    long duration = System.currentTimeMillis() - start;
    log.info("duration:{}", duration);

    start = System.currentTimeMillis();
    for (long i = 0; i < 10000L * 10000 * 100; i++) {
      b = (a << 1 + a << 3);
    }
    duration = System.currentTimeMillis() - start;
    log.info("duration:{}", duration);
  }

}
