package com.catface.redis.util;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author catface
 * @since 2022/11/9
 */
@Data
@AllArgsConstructor
public class ObjectSize {

  private long selfSize;
  private long totalSize;
  private String totalSizeHumanRead;

}
