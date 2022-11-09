package com.catface.redis.service;

import com.catface.redis.service.convert.Roaring64BitmapConvert;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
   */
  public void saveToRedis(String group, String memberIndexArrStr) {

    // 字符串解析,并分段到bitmap
    Map<Long, Roaring64Bitmap> segmentMap = Roaring64BitmapConvert.convertFromMemberIndexArrStr(
        memberIndexArrStr, segmentNum);

    // 逐个分段异步保存到redis,并同步阻塞
    List<Future<Long>> futures = new ArrayList<>();
    segmentMap.forEach((segmentId, bitmap) -> {
      Future<Long> future = saveToRedisAsync.saveToRedisRoaring64BitmapAsync(group, segmentId, bitmap);
      futures.add(future);
      // 此处是为了尽快释放内存
      segmentMap.put(segmentId, null);
    });

    // 同步检查所有的异步保存是否完成
    checkFailSegment(group, futures);
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


  private String buildSegKey(String group, Long segId) {
    return group + ":" + segId;
  }

  /**
   * 检查保存失败的segment并记录日志
   *
   * @param group   分组名称
   * @param futures 异步保存segment的结果(segmentId)
   */
  private void checkFailSegment(String group, List<Future<Long>> futures) {
    Set<Long> successSegments = new HashSet<>();
    for (Future<Long> future : futures) {
      try {
        successSegments.add(future.get());
      } catch (Exception e) {
        log.error("异步保存bitmap到redis异常", e);
      }
    }
    for (long segmentId = 0; segmentId < segmentNum; segmentId++) {
      if (!successSegments.contains(segmentId)) {
        log.warn("group:{},segmentId:{} save to redis fail", group, segmentId);
      }
    }
  }

}
