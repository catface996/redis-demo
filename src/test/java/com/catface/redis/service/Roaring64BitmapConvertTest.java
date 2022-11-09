package com.catface.redis.service;

import com.catface.redis.service.convert.Roaring64BitmapConvert;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

/**
 * @author catface
 * @since 2022/11/9
 */
@Slf4j
public class Roaring64BitmapConvertTest {

  @Test
  public void test() {

    int segmentNum = 100;
    int splitTimes = 20;
    int travelTimes = 40;
    String memberIndexArrStr = buildMemberIndexStr(1500 * 10000);
    long totalTravel = 0L;
    long totalSplit = 0L;
    for (int i = 0; i < splitTimes; i++) {
      long splitStart = System.currentTimeMillis();
      Roaring64BitmapConvert.convertFromMemberIndexArrStrSplit(memberIndexArrStr, segmentNum);
      long splitEnd = System.currentTimeMillis();
      long durationSplit = splitEnd - splitStart;
      totalSplit += durationSplit;
      log.info("durationSplit {}", durationSplit);
    }

    for (int i = 0; i < travelTimes; i++) {
      long travelStart = System.currentTimeMillis();
      Roaring64BitmapConvert.convertFromMemberIndexArrStr(memberIndexArrStr, segmentNum);
      long travelEnd = System.currentTimeMillis();
      long durationTravel = travelEnd - travelStart;
      log.info("durationTravel {}", durationTravel);
      totalTravel += durationTravel;
    }

    log.info("split avg {} 毫秒", totalSplit / splitTimes);
    log.info("travel avg {} 毫秒", totalTravel / travelTimes);

  }

  @Test
  public void test_2() {

    int segmentNum = 100;
    String memberIndexArrStr = buildMemberIndexStr(1500 * 10000);
    long totalTravel = 0L;

    for (int i = 0; i < 40; i++) {
      long travelStart = System.currentTimeMillis();
      Roaring64BitmapConvert.convertFromMemberIndexArrStr(memberIndexArrStr, segmentNum);
      long travelEnd = System.currentTimeMillis();
      long durationTravel = travelEnd - travelStart;
      log.info("durationTravel {}", durationTravel);
      totalTravel += durationTravel;
    }

    log.info("travel avg {} 毫秒", totalTravel / segmentNum);

  }

  @Test
  public void test_3() {
    int a = 2;
    int b = (a << 1) + (a << 3);
    log.info("b={}", b);
  }

  private String buildMemberIndexStr(int size) {
    List<String> data = new ArrayList<>();
    data.add("[" + 0);
    int base = 10000 * 10000;
    for (int i = 1; i < size - 1; i++) {
      data.add((base + i) + "");
    }
    data.add((size - 1) + "]");
    return String.join(",", data);
  }
}
