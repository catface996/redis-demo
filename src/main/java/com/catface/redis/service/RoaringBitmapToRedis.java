package com.catface.redis.service;

import com.catface.redis.service.convert.Roaring64BitmapConvert;
import com.catface.redis.service.convert.SegmentBuilder;
import com.catface.redis.service.model.GroupRoaring64BitmapSerialize;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.util.ArrayList;
import java.util.HashMap;
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
import org.springframework.util.CollectionUtils;

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
  private StringRedisTemplate stringRedisTemplate;

  @Autowired
  private RedisTemplate<String, Object> objectRedisTemplate;

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
      Future<Long> future = saveToRedisAsync.saveToRedisRoaring64BitmapAsync(group, segmentId,
          bitmap);
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

    // 计算segmentId并生成group的segmentKey
    long memberIndex = Long.parseLong(memberIndexStr);
    long segmentId = memberIndex % segmentNum;
    String segmentKey = SegmentBuilder.buildSegKey(group, segmentId);

    String segmentBatchKey = stringRedisTemplate.opsForValue().get(segmentKey);
    if (segmentBatchKey == null) {
      log.warn("group分段的batchKey不存在于Redis中,即group段未缓存到Redis,segmentKey:{}",
          segmentKey);
      return null;
    }

    GroupRoaring64BitmapSerialize serialize = (GroupRoaring64BitmapSerialize) objectRedisTemplate.opsForValue()
        .get(segmentBatchKey);
    if (serialize == null) {
      log.warn("从Redis中取出的group段的Roaring64Bitmap为空,segmentKey:{}", segmentKey);
      return null;
    }

    // 从Roaring64Bitmap中检查会员是否存在于对应的group中
    try {
      Roaring64Bitmap rBitmap = new Roaring64Bitmap();
      ByteArrayInputStream bis = new ByteArrayInputStream(serialize.getBitmapBytes());
      rBitmap.deserialize(new DataInputStream(bis));
      return rBitmap.contains(memberIndex);
    } catch (Exception e) {
      log.error("segmentBatchKey:{},对应的字节码无法反序列化成bitmap", segmentBatchKey, e);
    }
    return null;
  }

  /**
   * 查询指定的会员,是否在多个group中
   *
   * @param groups         group列表
   * @param memberIndexStr 会员下标字符串
   * @return group --> true:存在;false:不存在;null:未知
   */
  public Map<String, Boolean> inGroups(Set<String> groups, String memberIndexStr) {

    // 提前构建结果集,默认是未知
    Map<String, Boolean> result = initDefaultResult(groups);

    // 批量构建会员所在的段的key
    long memberIndex = Long.parseLong(memberIndexStr);
    Long segmentId = memberIndex % segmentNum;
    Set<String> segmentKeys = SegmentBuilder.buildSegKeys(groups, segmentId);

    // 一次获取多个段的有效批次的key
    List<String> batchKeys = stringRedisTemplate.opsForValue().multiGet(segmentKeys);
    if (CollectionUtils.isEmpty(batchKeys)) {
      log.warn("groups:{},memberIndex:{},未在redis中发现缓存的segment keys", groups,
          memberIndexStr);
      return result;
    }

    // 根据batchKey获取Roaring64Bitmap列表的
    Map<String, Roaring64Bitmap> segmentMap = getAndConvertRoaring64Bitmap(batchKeys);
    if (CollectionUtils.isEmpty(segmentMap)) {
      log.warn("groups:{},memberIndex:{},batchKeys:{},未在redis中发现缓存的Roaring64Bitmap",
          groups, memberIndexStr, batchKeys);
      return result;
    }

    // 检查会员是否出现在对应的group中,如果命中了Redis的缓存,结果要么是true,要么是false
    // 默认为null的说明group的分段未加载到Redis中缓存,需要到ClickHouse中查询
    for (String group : result.keySet()) {
      String segmentKey = SegmentBuilder.buildSegKey(group, segmentId);
      Roaring64Bitmap bitmap = segmentMap.get(segmentKey);
      if (bitmap != null) {
        result.put(group, bitmap.contains(memberIndex));
      }
    }
    return result;
  }

  /**
   * 根据segmentBatchKey 获取分段的Roaring64Bitmap
   *
   * @param batchKeys 批次key的列表
   * @return segmentKey -> 分段的 Roaring64Bitmap
   */
  private Map<String, Roaring64Bitmap> getAndConvertRoaring64Bitmap(List<String> batchKeys) {

    Map<String, Roaring64Bitmap> segmentMap = new HashMap<>(batchKeys.size());

    List<Object> serializes = objectRedisTemplate.opsForValue().multiGet(batchKeys);
    if (CollectionUtils.isEmpty(serializes)) {
      return segmentMap;
    }
    for (Object object : serializes) {
      GroupRoaring64BitmapSerialize serialize = (GroupRoaring64BitmapSerialize) object;
      Roaring64Bitmap rBitmap = new Roaring64Bitmap();
      try {
        rBitmap.deserialize(new DataInputStream(new ByteArrayInputStream(
            serialize.getBitmapBytes())));
      } catch (Exception e) {
        log.error("segmentKey:{},对应的字节码无法反序列化成bitmap", serialize.getSegmentKey(), e);
        continue;
      }
      segmentMap.put(serialize.getSegmentKey(), rBitmap);
    }
    return segmentMap;
  }

  private static Map<String, Boolean> initDefaultResult(Set<String> groups) {
    Map<String, Boolean> result = new HashMap<>();
    for (String group : groups) {
      result.put(group, null);
    }
    return result;
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
