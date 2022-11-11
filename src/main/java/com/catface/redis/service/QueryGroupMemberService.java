package com.catface.redis.service;

import com.catface.redis.service.convert.SegmentBuilder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

/**
 * @author catface
 * @since 2022/11/8
 */
@Slf4j
@Service
public class QueryGroupMemberService {

  @Autowired
  private StringRedisTemplate stringRedisTemplate;

  /**
   * 判断会员是否在相应的group中
   *
   * @param group          组
   * @param memberIndexStr 会员下标
   * @return true:在group中,false:不在group中,null:不确定
   */
  public Boolean inGroup(String group, String memberIndexStr) {
    long memberIndex = Long.parseLong(memberIndexStr);
    String segmentKey = SegmentBuilder.buildSegmentKey(group, memberIndex);
    String segmentBatchKey = stringRedisTemplate.opsForValue().get(segmentKey);
    if (segmentBatchKey == null) {
      return null;
    }
    return stringRedisTemplate.opsForSet().isMember(segmentBatchKey, memberIndex);
  }

}
