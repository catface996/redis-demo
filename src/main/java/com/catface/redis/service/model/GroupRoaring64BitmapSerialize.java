package com.catface.redis.service.model;

import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author catface
 * @since 2022/11/10
 */
@Data
@AllArgsConstructor
public class GroupRoaring64BitmapSerialize implements Serializable {

  private static final long serialVersionUID = -7726229608922308782L;

  private String segmentKey;

  private byte[] bitmapBytes;

}
