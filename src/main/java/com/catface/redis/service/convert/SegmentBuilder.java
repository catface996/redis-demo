package com.catface.redis.service.convert;

import java.util.HashSet;
import java.util.Set;

/**
 * @author catface
 * @since 2022/11/10
 */
public class SegmentBuilder {

  /**
   * 构建group的segmentKey
   *
   * @param group 组别
   * @param segId 分段ID
   * @return segmentKey, 在Redis中使用
   */
  public static String buildSegmentKey(String group, Long segId) {
    return "dc:" + group + ":" + segId;
  }

  /**
   * 构建group的segmentKey指向的batchKey
   *
   * @param group 组别
   * @param segId 分段ID
   * @return segmentBatchKey, 在Redis中使用
   */
  public static String buildSegmentBatchKey(String group, Long segId) {
    long batchId = System.currentTimeMillis();
    return buildSegmentKey(group, segId) + ":" + batchId;
  }

  /**
   * 构建group分段的key
   *
   * @param groups    组名集合
   * @param segmentId 分段ID,是会员下标对分段数取余获得
   * @return group分段
   */
  public static Set<String> buildSegmentKeys(Set<String> groups, Long segmentId) {
    Set<String> segmentKeys = new HashSet<>();
    for (String group : groups) {
      segmentKeys.add(buildSegmentKey(group, segmentId));
    }
    return segmentKeys;
  }
}
