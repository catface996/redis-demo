package com.catface.redis.service;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.roaringbitmap.longlong.Roaring64Bitmap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
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

  @Autowired
  private StringRedisTemplate stringRedisTemplate;

  @Autowired
  private RedisTemplate<String, byte[]> byteRedisTemplate;


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
      stringRedisTemplate.expire(newSegBatchKey, 1L, TimeUnit.HOURS);
      // 将新的有效批次,设置到segment的key对应的value中
      stringRedisTemplate.opsForValue().set(segKey, newSegBatchKey, 1L, TimeUnit.HOURS);
      // 老版本的segment的批次存在的,设置过期时间,避免删除后,已经获取到老版本批次ID的请求穿透缓存
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

  @Async("threadPool")
  public Future<Void> saveToRedisRoaring64BitmapAsync(String group, Long segmentId, byte[] bitArr) {
    try {
      // 构建segment的key
      String segKey = buildSegKey(group, segmentId);
      // 获取segment已经存在的有效批次的key
      String oldSegBatchKey = stringRedisTemplate.opsForValue().get(segKey);
      // 构建segment对应的有效批次的key
      String newSegBatchKey = buildSegBatchKey(group, segmentId);
      // 将segment保存到redis
      stringRedisTemplate.opsForValue().set(newSegBatchKey, new String(bitArr, 0, bitArr.length));
      // 设置segment的过期时间为1天
      stringRedisTemplate.expire(newSegBatchKey, 1L, TimeUnit.HOURS);
      // 将新的有效批次,设置到segment的key对应的value中
      stringRedisTemplate.opsForValue().set(segKey, newSegBatchKey, 1L, TimeUnit.HOURS);
      // 老版本的segment的批次存在的,设置过期时间,避免删除后,已经获取到老版本批次ID的请求穿透缓存
      if (oldSegBatchKey != null) {
        stringRedisTemplate.expire(oldSegBatchKey, 10, TimeUnit.SECONDS);
      }
    } catch (Exception e) {
      log.error("异步保存memberIndex到redis异常", e);
    }
    return new AsyncResult<>(null);
  }

  @Async("threadPool")
  public Future<Long> saveToRedisRoaring64BitmapAsync(String group, Long segmentId, Roaring64Bitmap bitmap) {
    try {
      // 构建segment的key
      String segKey = buildSegKey(group, segmentId);
      // 构建segment对应的有效批次的key
      String newSegBatchKey = buildSegBatchKey(group, segmentId);

      // 将Roaring64Bitmap转换成byte[],以便保存到redis
      byte[] bitmapBytes = convertToBytes(bitmap);
      // 将segment保存到redis,设置segment的过期时间为1天,大于定时任务的周期即可
      byteRedisTemplate.opsForValue().set(newSegBatchKey, bitmapBytes, 1L, TimeUnit.HOURS);

      // 获取segment已经存在的有效批次的key,避免batch滚动时被覆盖,无法获得
      String oldSegBatchKey = stringRedisTemplate.opsForValue().get(segKey);

      // 将新的有效批次,设置到segment的key对应的value中,设置同样的过期时间
      stringRedisTemplate.opsForValue().set(segKey, newSegBatchKey, 1L, TimeUnit.HOURS);

      // 待新的batch缓存生效后,再操作老版本的batch缓存,避免提前清理了缓存
      // 老版本的segment的批次存在的,设置过期时间,避免删除后,已经获取到老版本批次ID的请求穿透缓存
      if (oldSegBatchKey != null) {
        stringRedisTemplate.expire(oldSegBatchKey, 10, TimeUnit.SECONDS);
      }
    } catch (Exception e) {
      log.error("异步保存memberIndex到redis异常,group:{},segmentId:{}", group, segmentId, e);
      // 因为segmentId是通过取余数获得,所以-1一定是一个无效的segmentId
      return new AsyncResult<>(-1L);
    }
    return new AsyncResult<>(segmentId);
  }

  private static byte[] convertToBytes(Roaring64Bitmap bitmap) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    bitmap.serialize(new DataOutputStream(bos));
    byte[] data = bos.toByteArray();
    return data;
  }


  private String buildSegKey(String group, Long segId) {
    return group + ":" + segId;
  }

  private String buildSegBatchKey(String group, Long segId) {
    long batchId = System.currentTimeMillis();
    return buildSegKey(group, segId) + ":" + batchId;
  }

}
