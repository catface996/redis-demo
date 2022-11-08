package com.catface.redis.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * @author catface
 * @since 2022/11/8
 */
@Slf4j
@SpringBootTest
public class BatchSaveTest {

  @Autowired
  private BatchSaveToRedisService batchSaveToRedisService;

  @Autowired
  private QueryGroupMemberService queryGroupMemberService;

  @Test
  public void test() throws Exception {
    String group = "group-1";
    String memberIndexArrStr = buildMemberIndexStr(15000000);
    batchSaveToRedisService.saveToRedis(group, memberIndexArrStr);
    Random random = new Random(10);
    for (int i = 0; i < 100; i++) {
      long queryStart = System.currentTimeMillis();
      String memberIndex = random.nextInt(80000000) + "";
      boolean exist = queryGroupMemberService.inGroup(group, memberIndex);
      long queryEnd = System.currentTimeMillis();
      log.info("memberIndex:{} exist:{},duration:{}", memberIndex, exist, queryEnd - queryStart);
    }
    TimeUnit.MINUTES.sleep(10);
  }

  private String buildMemberIndexStr(int size) {
    List<String> data = new ArrayList<>();
    data.add("[" + 0);
    for (int i = 1; i < size - 1; i++) {
      data.add(i + "");
    }
    data.add((size - 1) + "]");
    return String.join(",", data);
  }

}
