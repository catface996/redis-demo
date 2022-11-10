package com.catface.redis.service;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.util.Assert;

/**
 * @author catface
 * @since 2022/11/8
 */
@Slf4j
@SpringBootTest
public class RoaringBitmapToRedisTest {

  @Autowired
  private RoaringBitmapToRedis roaringBitmapToRedis;

  @Autowired
  private StringRedisTemplate stringRedisTemplate;

  @Autowired
  private RedisTemplate<String, byte[]> byteRedisTemplate;

  @Test
  public void testClean() {
    for (int g = 0; g < 100; g++) {
      for (long s = 0; s < 100; s++) {
        String segmentKey = "group-" + g + ":" + s;
        String segmentBatchKey = stringRedisTemplate.opsForValue().get(segmentKey);
        if (segmentBatchKey != null) {
          try {
            stringRedisTemplate.delete(segmentBatchKey);
            stringRedisTemplate.delete(segmentKey);
          } catch (Exception e) {
            log.error("删除失败", e);
          }
        }
      }
    }
  }

  @Test
  public void testCleanG() {
    for (int i = 0; i < 100; i++) {
      String groupKey = "group-" + i;
      stringRedisTemplate.delete(groupKey);
    }
  }


  @Test
  public void testSaveToRedis() {
    String memberIndexArrStr = buildMemberIndexStr(1500 * 10000);
    long total = 0L;
    for (int i = 0; i < 100; i++) {
      long start = System.currentTimeMillis();
      roaringBitmapToRedis.saveToRedis("group-" + i, memberIndexArrStr);
      long duration = System.currentTimeMillis() - start;
      total += duration;
      log.info("process duration {}", duration);
    }
    log.info("ave duration {}", total / 100);
  }

  @Test
  public void testTestInGroup() {
    int baseNum = 10000 * 10000;
    Random random = new Random();
    long total = 0;
    int memberIndexMax = 10000 * 10;
    for (int memberIndex = 1; memberIndex < memberIndexMax; memberIndex++) {
      String memberId = (memberIndex + baseNum) + "";
      String group = "group-" + random.nextInt(100);
      long start = System.currentTimeMillis();
      Boolean in = roaringBitmapToRedis.inGroup(group, memberId);
      long end = System.currentTimeMillis();
      long duration = end - start;
      total += duration;
      Assert.state(in, "不存在是不对的");
    }
    log.info("avg duration: {} 毫秒", total / memberIndexMax);

  }

  @Test
  public void testTestInGroupBatch() {
    int baseNum = 10000 * 10000;
    Random random = new Random();
    long total = 0;
    int memberIndexMax = 1000;
    for (int memberIndex = 1; memberIndex < memberIndexMax; memberIndex++) {
      String memberId = (memberIndex + baseNum) + "";
      Set<String> groups = new HashSet<>();
      int groupNum = random.nextInt(5) + 5;
      for (int i = 0; i < groupNum; i++) {
        groups.add("group-" + random.nextInt(100));
      }
      long start = System.currentTimeMillis();
      Map<String, Boolean> in = roaringBitmapToRedis.inGroups(groups, memberId);
      long end = System.currentTimeMillis();
      long duration = end - start;
      total += duration;
      for (Boolean value : in.values()) {
        Assert.state(value, "不存在是不对的");
      }
    }
    log.info("avg duration: {} 毫秒", total / memberIndexMax);

  }


  private String buildMemberIndexStr(int size) {
    List<String> data = new ArrayList<>();
    data.add("[" + 0);
    int baseNum = 10000 * 10000;
    for (int i = 1; i < size - 1; i++) {
      data.add((baseNum + i) + "");
    }
    data.add((size - 1) + "]");
    return String.join(",", data);
  }

}
