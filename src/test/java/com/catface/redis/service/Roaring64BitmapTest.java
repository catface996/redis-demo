package com.catface.redis.service;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.roaringbitmap.longlong.Roaring64Bitmap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;

/**
 * @author catface
 * @since 2022/11/8
 */
@Slf4j
@SpringBootTest
public class Roaring64BitmapTest {

  @Autowired
  private StringRedisTemplate stringRedisTemplate;

  @Test
  public void test_1500_0000() throws Exception {

    Roaring64Bitmap roaring64Bitmap = new Roaring64Bitmap();
    for (int i = 0; i < 1500 * 10000; i++) {
      long memberIndex = 100000000 + i;
      roaring64Bitmap.add(memberIndex);
    }
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    roaring64Bitmap.serialize(new DataOutputStream(bos));
    String value = bos.toString();
    long total = 0;
    for (int i = 0; i < 100; i++) {
      long start = System.currentTimeMillis();
      stringRedisTemplate.opsForValue().set("group-" + i, value);
      long end = System.currentTimeMillis();
      long duration = end - start;
      log.info("save duration {}", duration);
      total += duration;
    }
    log.info("avg duration {}", total / 100);
  }


}
