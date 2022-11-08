package com.catface.redis.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;

/**
 * @author catface
 * @since 2022/11/7
 */
@Slf4j
@SpringBootTest
public class RedisServiceTest {

  @Autowired
  private StringRedisTemplate stringRedisTemplate;

  private final Integer RANGE = 1000000;
  private final Integer SEGMENTS = 100;

  @Test
  public void clear() {
    for (long i = 0; i < 20; i++) {
      for (long segId = 0; segId < 100; segId++) {
        String segKey = buildSegKey("group-" + i, segId);
        String batchKey = stringRedisTemplate.opsForValue().get(segKey);
        if (batchKey != null) {
          stringRedisTemplate.delete(batchKey);
          stringRedisTemplate.delete(segKey);
        }
      }
    }
  }


  @Test
  public void test_batch() {
    long groupNum = 1;
    long total = 0L;
    for (int i = 0; i < groupNum; i++) {
      String groupName = "group-" + i;
      long duration = saveToRedis(15000000, groupName);
      total += duration;
      get(groupName);
    }
    log.info("total:{},average:{}", total, total / groupNum);
  }

  @Test
  public void get(String group) {
    String memberIndex = "98766";
    boolean in = inGroup(memberIndex, group);
    log.info("member:{} in group:{} is {}", memberIndex, group, in);
  }

  private boolean inGroup(String memberIndex, String group) {
    long segId = calculateSegId(memberIndex);
    String segKey = buildSegKey(group, segId);
    String segBatchKey = stringRedisTemplate.opsForValue().get(segKey);
    return Boolean.TRUE.equals(stringRedisTemplate.opsForSet().isMember(segBatchKey, memberIndex));
  }

  private long saveToRedis(int size, String group) {
    List<String> data = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      data.add(i + "");
    }
    Map<Long, List<String>> segMap = new HashMap<>();
    for (long i = 0; i < SEGMENTS; i++) {
      segMap.put(i, new ArrayList<>(RANGE));
    }
    String str = String.join(",", data);

    // split成数组
    long startTime = System.currentTimeMillis();
    String[] strArr = str.split(",");
    long endTime = System.currentTimeMillis();
    log.info("split duration:{}", endTime - startTime);

    // 转成segment并存储到redis
    startTime = System.currentTimeMillis();
    for (String s : strArr) {
      long segId = calculateSegId(s);
      segMap.get(segId).add(s);
    }
    segMap.forEach((k, v) -> {
      if (k != null && v.size() > 0) {
        // 构建segment的key
        String segKey = buildSegKey(group, k);
        // 获取segment已经存在的有效批次的key
        String oldSegBatchKey = stringRedisTemplate.opsForValue().get(segKey);
        // 构建segment对应的有效批次的key
        String newSegBatchKey = buildSegBatchKey(group, k);
        // 将segment保存到redis
        String[] vArr = v.toArray(new String[0]);
        long tStart = System.currentTimeMillis();
        stringRedisTemplate.opsForSet().add(newSegBatchKey, vArr);
        long tEnd = System.currentTimeMillis();
        log.info("save segment time:{}",tEnd-tStart);
        // 设置segment的过期时间为1天
        stringRedisTemplate.expire(newSegBatchKey, 1L, TimeUnit.DAYS);
        // 将新的有效批次,设置到segment的key对应的value中
        stringRedisTemplate.opsForValue().set(segKey, newSegBatchKey);
        // 老版本的segment的批次存在的话,进行删除
        if (oldSegBatchKey != null) {
          stringRedisTemplate.delete(oldSegBatchKey);
        }
      }
    });
    endTime = System.currentTimeMillis();
    long duration = endTime - startTime;
    log.info("save to redis duration:{}", duration);
    return duration;
  }

  private String buildSegKey(String group, Long segId) {
    return group + ":" + segId;
  }

  private String buildSegBatchKey(String group, Long segId) {
    long batchId = System.currentTimeMillis();
    return buildSegKey(group, segId) + ":" + batchId;
  }

  private long calculateSegId(String memberIndex) {
    return (Long.parseLong(memberIndex) / RANGE) % SEGMENTS;
  }
}
