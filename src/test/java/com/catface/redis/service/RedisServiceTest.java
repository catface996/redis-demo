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
    for (long i = 0; i < 2; i++) {
      for (long segId = 0; segId < 1000; segId++) {
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

    // split?????????
    long startTime = System.currentTimeMillis();
    String[] strArr = str.split(",");
    long endTime = System.currentTimeMillis();
    log.info("split duration:{}", endTime - startTime);

    // ??????segment????????????redis
    startTime = System.currentTimeMillis();
    for (String s : strArr) {
      long segId = calculateSegId(s);
      segMap.get(segId).add(s);
    }
    segMap.forEach((k, v) -> {
      if (k != null && v.size() > 0) {
        // ??????segment???key
        String segKey = buildSegKey(group, k);
        // ??????segment??????????????????????????????key
        String oldSegBatchKey = stringRedisTemplate.opsForValue().get(segKey);
        // ??????segment????????????????????????key
        String newSegBatchKey = buildSegBatchKey(group, k);
        // ???segment?????????redis
        String[] vArr = v.toArray(new String[0]);
        long tStart = System.currentTimeMillis();
        stringRedisTemplate.opsForSet().add(newSegBatchKey, vArr);
        long tEnd = System.currentTimeMillis();
        log.info("save segment time:{}",tEnd-tStart);
        // ??????segment??????????????????1???
        stringRedisTemplate.expire(newSegBatchKey, 1L, TimeUnit.DAYS);
        // ?????????????????????,?????????segment???key?????????value???
        stringRedisTemplate.opsForValue().set(segKey, newSegBatchKey);
        // ????????????segment?????????????????????,????????????
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
