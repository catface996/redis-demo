package com.catface.redis.service;

import com.catface.redis.util.ObjectUtil;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

/**
 * @author catface
 * @since 2022/11/9
 */
@Slf4j
public class TestObjectSize {

  @Test
  public void test() {
    String a = "12345678901234567890123456789012345678901234567890123456789012345678901234567890";
    String b = "11500000";
    log.info("a size:{}", ObjectUtil.size(a));
    log.info("b size:{}", ObjectUtil.size(b));
  }
}
