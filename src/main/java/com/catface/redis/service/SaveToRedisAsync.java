package com.catface.redis.service;

import java.util.LinkedList;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Service;

/**
 * @author catface
 * @since 2022/11/8
 */
@Slf4j
@Service
public class SaveToRedisAsync {

  @Value("${member.segmentNum}")
  private Integer segmentNum;

  @Autowired
  private StringRedisTemplate stringRedisTemplate;

  @Async("threadPool")
  public Future<Long> saveToRedisAsync(String group, Long segmentId,
      LinkedList<String> memberIndexArr) {
    Long saveNum = 0L;
    try {
      // 构建segment的key
      String segKey = buildSegKey(group, segmentId);
      // 获取segment已经存在的有效批次的key
      String oldSegBatchKey = stringRedisTemplate.opsForValue().get(segKey);
      // 构建segment对应的有效批次的key
      String newSegBatchKey = buildSegBatchKey(group, segmentId);

      // 将segment保存到redis
      String[] vArr = memberIndexArr.toArray(new String[0]);
      long tStart = System.currentTimeMillis();
      saveNum = stringRedisTemplate.opsForSet().add(newSegBatchKey, vArr);
      long tEnd = System.currentTimeMillis();
      log.info("save segment time:{}", tEnd - tStart);
      // 设置segment的过期时间为1天
      stringRedisTemplate.expire(newSegBatchKey, 1L, TimeUnit.DAYS);
      // 将新的有效批次,设置到segment的key对应的value中
      stringRedisTemplate.opsForValue().set(segKey, newSegBatchKey, 1L, TimeUnit.DAYS);
      // 老版本的segment的批次存在的话,进行删除
      if (oldSegBatchKey != null) {
        stringRedisTemplate.delete(oldSegBatchKey);
      }
    } catch (Exception e) {
      log.error("异步保存memberIndex到redis异常", e);
    }
    if (saveNum == null) {
      saveNum = 0L;
    }
    return AsyncResult.forValue(saveNum);
  }


  private String buildSegKey(String group, Long segId) {
    return group + ":" + segId;
  }

  private String buildSegBatchKey(String group, Long segId) {
    long batchId = System.currentTimeMillis();
    return buildSegKey(group, segId) + ":" + batchId;
  }

  private long calculateSegId(String memberIndex) {
    return Long.parseLong(memberIndex) % segmentNum;
  }

}
