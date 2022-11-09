package com.catface.redis.service;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import lombok.extern.slf4j.Slf4j;
import org.roaringbitmap.longlong.Roaring64Bitmap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

/**
 * @author catface
 * @since 2022/11/8
 */
@Slf4j
@Service
public class RoaringBitmapToRedis {

  @Value("${member.segmentNum}")
  private Integer segmentNum;

  @Autowired
  private SaveToRedisAsync saveToRedisAsync;

  @Autowired
  private RedisTemplate<String, byte[]> byteRedisTemplate;

  @Autowired
  private StringRedisTemplate stringRedisTemplate;

  /**
   * 保存会员下标到redis
   *
   * @param group             组别
   * @param memberIndexArrStr 会员下标
   * @return 消耗的时长
   */
  public long saveToRedis(String group, String memberIndexArrStr) {
    long processStart = System.currentTimeMillis();
    String[] memberIdArr = splitToArr(memberIndexArrStr);

    Map<Long, Roaring64Bitmap> segmentMap = convertRoaring64Bitmap(memberIdArr);

    List<Future<Void>> futures = new ArrayList<>();
    segmentMap.forEach((segmentId, bitmap) -> {
      try {
        Future<Void> future = saveToRedisAsync.saveToRedisAsyncBit(group, segmentId, bitmap);
        futures.add(future);
        segmentMap.put(segmentId, null);
      } catch (Exception e) {
        log.error("序列化bitmap异常", e);
      }
    });
    for (Future<Void> future : futures) {
      try {
        future.get();
      } catch (Exception e) {
        log.error("异步保存bitmap到redis异常", e);
      }
    }
    long processEnd = System.currentTimeMillis();
    return processEnd - processStart;
  }

  /**
   * 是否在组中
   *
   * @param group          待检查的组
   * @param memberIndexStr 会员下标字符串
   * @return true:在组中,false:不在组中;null:不确定
   */
  public Boolean inGroup(String group, String memberIndexStr) {
    long memberIndex = Long.parseLong(memberIndexStr);
    long segmentId = memberIndex % segmentNum;
    String segmentKey = buildSegKey(group, segmentId);
    String segmentBatchKey = stringRedisTemplate.opsForValue().get(segmentKey);
    if (segmentBatchKey != null) {
      byte[] data = byteRedisTemplate.opsForValue().get(segmentBatchKey);
      Roaring64Bitmap rBitmap = new Roaring64Bitmap();
      try {
        rBitmap.deserialize(new DataInputStream(new ByteArrayInputStream(data)));
      } catch (Exception e) {
        log.error("segmentBatchKey:{},对应的字节码无法反序列化成bitmap", segmentBatchKey, e);
        return null;
      }
      return rBitmap.contains(memberIndex);
    }
    return null;
  }

  private String[] splitToArr(String memberIndexArrStr) {
    // split成数组,并替换首个字符串中的左中括号和最后一个字符串的右中括号
    String[] strArr = memberIndexArrStr.split(",");
    // 字符串中不存在任何memberId,直接返回
    if (strArr.length <= 1) {
      return null;
    }
    // 对首个字符串,替换左中括号,对最后一个字符串,替换右中括号
    strArr[0] = strArr[0].replaceFirst("\\[", "");
    strArr[strArr.length - 1] = strArr[strArr.length - 1].replaceFirst("]", "");
    return strArr;
  }


  private Map<Long, Roaring64Bitmap> convertRoaring64Bitmap(String[] memberIndexArr) {
    Map<Long, Roaring64Bitmap> segmentMap = new HashMap<>();
    for (long i = 0; i < segmentNum; i++) {
      segmentMap.put(i, new Roaring64Bitmap());
    }
    for (String memberIndexStr : memberIndexArr) {
      Long memberIndex = Long.parseLong(memberIndexStr);
      Long segmentId = memberIndex % segmentNum;
      segmentMap.get(segmentId).add(memberIndex);
    }
    return segmentMap;
  }

  private String buildSegKey(String group, Long segId) {
    return group + ":" + segId;
  }

}
