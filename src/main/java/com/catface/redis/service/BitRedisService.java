package com.catface.redis.service;

import com.catface.redis.service.model.Segment;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @author catface
 * @since 2022/11/8
 */
@Slf4j
@Service
public class BitRedisService {

  @Autowired
  private SaveToRedisAsync saveToRedisAsync;
  /**
   * 1000个segment,每个segment800万位
   */
  private Integer segmentNum = 1000;

  private byte[] op = new byte[8];

  public void init() {
    for (int i = 0; i < 8; i++) {
      op[i] = (byte) i;
    }
  }

  public void saveToRedis(String group, String memberIndexArrStr) {
    String[] memberIndexArr = splitToMemberIndexArr(memberIndexArrStr);
    Map<Long, Segment> segBitMap = convertSegBitMap(memberIndexArr);
    segBitMap.forEach((segmentId, segment) -> {
      saveToRedisAsync.saveToRedisRoaring64BitmapAsync(group, segmentId, segment.getValue());
    });
  }


  public Map<Long, Segment> convertSegBitMap(String[] memberIndexArr) {
    long convertStart = System.currentTimeMillis();
    Map<Long, Segment> segBitMap = new HashMap<>(segmentNum);
    for (long i = 0; i < segmentNum; i++) {
      segBitMap.put(i, new Segment(i, new byte[1000000]));
    }
    for (String memberIndexStr : memberIndexArr) {
      long memberIndex = Long.parseLong(memberIndexStr);
      long segmentId = memberIndex / segmentNum;
      long segmentIndex = memberIndex - (segmentId * segmentNum);
      int byteIndex = (int) segmentIndex / 8;
      short bitIndex = (short) (segmentIndex % 8);
      byte target = segBitMap.get(segmentId).getValue()[byteIndex];
      byte newTarget = (byte) (target & op[bitIndex]);
      segBitMap.get(segmentId).getValue()[byteIndex] = newTarget;
    }
    long convertEnd = System.currentTimeMillis();
    log.info("convert to bit duration:{}", convertEnd - convertStart);
    return segBitMap;
  }

  /**
   * 将会员数组字符串,拆分为数组
   *
   * @param memberIndexArrStr 格式为 "[123,456,23,8797]"
   * @return 拆分后的member index数组
   */
  private String[] splitToMemberIndexArr(String memberIndexArrStr) {
    // split成数组,并替换首个字符串中的左中括号和最后一个字符串的右中括号
    long splitStart = System.currentTimeMillis();
    String[] strArr = memberIndexArrStr.split(",");
    // 字符串中不存在任何memberId,直接返回
    if (strArr.length <= 1) {
      return null;
    }
    // 对首个字符串,替换左中括号,对最后一个字符串,替换右中括号
    strArr[0] = strArr[0].replaceFirst("\\[", "");
    strArr[strArr.length - 1] = strArr[strArr.length - 1].replaceFirst("]", "");
    long splitEnd = System.currentTimeMillis();
    // 计算split的时长,用于监控性能
    long splitDuration = splitEnd - splitStart;
    log.info("split duration:{}", splitDuration);
    return strArr;
  }

}
