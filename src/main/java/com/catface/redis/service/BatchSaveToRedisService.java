package com.catface.redis.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * @author catface
 * @since 2022/11/8
 */
@Slf4j
@Service
public class BatchSaveToRedisService {

  @Value("${member.segmentNum}")
  private Integer segmentNums;

  @Autowired
  private SaveToRedisAsync saveToRedisAsync;

  /**
   * 保存group对应的member下标到redis
   *
   * @param group             组名
   * @param memberIndexArrStr 会员下标的字符串
   */
  public void saveToRedis(String group, String memberIndexArrStr) {
    long saveStart = System.currentTimeMillis();
    // 解析为数组
    String[] memberIndexArr = splitToMemberIndexArr(memberIndexArrStr);

    if (memberIndexArr == null) {
      log.warn("指定group中无有效用户,group:{}", group);
      return;
    }
    // 分组为map
    List<Future<Long>> futureList = new ArrayList<>();
    Map<Long, LinkedList<String>> segmentMap = splitSegment(memberIndexArr, segmentNums);

    // 逐个segment异步保存到redis
    segmentMap.forEach((segmentId, segmentArr) -> {
      Future<Long> future = saveToRedisAsync.saveToRedisAsync(group, segmentId, segmentArr);
      futureList.add(future);
      // 避免map对segment arr的引用,保存到redis后,即可被垃圾回收
      segmentMap.put(segmentId, new LinkedList<>());
    });

    // 获取保存结果
    long totalNum = 0L;
    for (Future<Long> longFuture : futureList) {
      try {
        long saveNum = longFuture.get();
        totalNum += saveNum;
      } catch (Exception e) {
        log.error("异步保存segment到redis,获取返回结果异常", e);
      }
    }

    long saveEnd = System.currentTimeMillis();
    long saveDuration = saveEnd - saveStart;
    log.info("实际保存到redis的member数量为:{},耗时:{}毫秒", totalNum, saveDuration);
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

  /**
   * 将会员下标的数组,拆分成多个段
   *
   * @param memberIndexArr 会员下标数组
   * @param segmentNum     分段数量
   * @return 分段ID和分段数量
   */
  private Map<Long, LinkedList<String>> splitSegment(String[] memberIndexArr, int segmentNum) {
    Map<Long, LinkedList<String>> arrMap = new HashMap<>(segmentNum);
    for (long i = 0; i < segmentNum; i++) {
      arrMap.put(i, new LinkedList<>());
    }
    for (String memberIndexStr : memberIndexArr) {
      long memberIndex = Long.parseLong(memberIndexStr);
      arrMap.get(memberIndex % segmentNum).add(memberIndexStr);
    }
    return arrMap;
  }

}
