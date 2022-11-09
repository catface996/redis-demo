package com.catface.redis.util;

import org.apache.lucene.util.RamUsageEstimator;


/**
 * @author catface
 * @since 2022/11/9
 */
public class ObjectUtil {

  public static ObjectSize size(Object o) {
    //计算指定对象本身在堆空间的大小，单位字节
    long shallowSize = RamUsageEstimator.shallowSizeOf(o);
    //计算指定对象及其引用树上的所有对象的综合大小，单位字节
    long size = RamUsageEstimator.sizeOf(o);
    //计算指定对象及其引用树上的所有对象的综合大小，返回可读的结果，如：2KB
    String humanSize = RamUsageEstimator.humanSizeOf(o);
    return new ObjectSize(shallowSize, size, humanSize);
  }


}
