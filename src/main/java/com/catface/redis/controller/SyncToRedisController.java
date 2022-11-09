package com.catface.redis.controller;

import com.catface.redis.service.RoaringBitmapToRedis;
import com.catface.redis.service.convert.Roaring64BitmapConvert;
import com.catface.redis.util.ObjectSize;
import com.catface.redis.util.ObjectUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import lombok.extern.slf4j.Slf4j;
import org.roaringbitmap.longlong.Roaring64Bitmap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author catface
 * @since 2022/11/9
 */
@Slf4j
@RestController
public class SyncToRedisController {

  @Autowired
  private RoaringBitmapToRedis roaringBitmapToRedis;

  private Random RANDOM = new Random();


  /**
   * 模拟同步到redis
   *
   * @param memberNum 会员个数
   * @return success
   */
  @RequestMapping(value = "/syncToRedis")
  public String syncToRedis(@RequestParam("groupNum") Integer groupNum,
      @RequestParam("memberNum") Integer memberNum) {
    String memberIndexArrStr = buildMemberIndexStr(memberNum);
    new Thread(() -> {
      long total = 0L;
      for (int i = 0; i < groupNum; i++) {
        long start = System.currentTimeMillis();
        roaringBitmapToRedis.saveToRedis("group-" + i, memberIndexArrStr);
        long duration = System.currentTimeMillis() - start;
        total += duration;
        log.info("process duration {}", duration);
      }
      log.info("ave duration {}", total / 100);
    }).start();
    return "success";
  }

  @RequestMapping(value = "/inGroup")
  public Boolean inGroup() {
    int groupId = RANDOM.nextInt(100);
    int memberIndex = RANDOM.nextInt(1500 * 10000);
    assert  roaringBitmapToRedis.inGroup("group-" + groupId, (10000 * 10000 + memberIndex) + "");
    return true;
  }

  @RequestMapping(value = "/stringSize")
  public ObjectSize stringSize(@RequestParam("memberNum") Integer memberNum) throws Exception {
    String memberIndexArrStr = buildMemberIndexStr(memberNum);
    return ObjectUtil.size(memberIndexArrStr);
  }

  @RequestMapping(value = "/bitmapSize")
  public ObjectSize bitmapSize(@RequestParam("memberNum") Integer memberNum) throws Exception {
    String memberIndexArrStr = buildMemberIndexStr(memberNum);
    Map<Long, Roaring64Bitmap> segmentMap = Roaring64BitmapConvert.convertFromMemberIndexArrStr(
        memberIndexArrStr, 100);
    return ObjectUtil.size(segmentMap);
  }

  private String buildMemberIndexStr(int size) {
    List<String> data = new ArrayList<>();
    data.add("[" + 0);
    int baseNum = 10000 * 10000;
    for (int i = 1; i < size - 1; i++) {
      data.add((baseNum + i) + "");
    }
    data.add((size - 1) + "]");
    return String.join(",", data);
  }

}
