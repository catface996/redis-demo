package com.catface.redis.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

/**
 * @author catface
 * @since 2022/11/8
 */
@Slf4j
@Service
public class QueryGroupMemberService {

  @Value("${member.segmentNum}")
  private Integer segmentNums;

  @Autowired
  private StringRedisTemplate stringRedisTemplate;

  /**
   * 判断会员是否在相应的group中
   *
   * @param group       组
   * @param memberIndex 会员下标
   * @return true:在group中,false:不在group中,null:不确定
   */
  public Boolean inGroup(String group, String memberIndex) {
    long segmentId = calculateSegmentId(memberIndex);
    String segmentKey = buildSegKey(group, segmentId);
    String segmentBatchKey = stringRedisTemplate.opsForValue().get(segmentKey);
    if (segmentBatchKey == null) {
      return null;
    }
    return stringRedisTemplate.opsForSet().isMember(segmentBatchKey, memberIndex);
  }

  private String buildSegKey(String group, Long segId) {
    return group + ":" + segId;
  }

  private String buildSegBatchKey(String group, Long segId) {
    long batchId = System.currentTimeMillis();
    return buildSegKey(group, segId) + ":" + batchId;
  }

  private Long calculateSegmentId(String memberIndex) {
    return Long.parseLong(memberIndex) % segmentNums;
  }
}
