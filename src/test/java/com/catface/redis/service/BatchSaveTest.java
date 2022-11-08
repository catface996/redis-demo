package com.catface.redis.service;

import java.util.ArrayList;
import java.util.List;
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

  @Test
  public void test() throws Exception {
    String memberIndexArrStr = buildMemberIndexStr(15000000);
    batchSaveToRedisService.saveToRedis("group-1", memberIndexArrStr);
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
