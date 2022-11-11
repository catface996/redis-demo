package com.catface.redis.service;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

/**
 * @author catface
 * @since 2022/11/11
 */
public interface GroupMemberCacheService {

  /**
   * 保存group对应的会员的索引信息到缓存中
   *
   * @param group             组
   * @param memberIndexArrStr 会员索引的字符串,格式为: [123,456,789]
   */
  void saveToCache(String group, String memberIndexArrStr);

  /**
   * 异步保存group对应的会员索引信息到缓存中
   *
   * @param group             组
   * @param memberIndexArrStr 会员索引的字符串,格式为: [123,456,789]
   * @return 用于异步转同步的阻塞
   */
  Future<Boolean> saveToCacheAsync(String group, String memberIndexArrStr);

  /**
   * 会员是否出现在组中
   *
   * @param group          组
   * @param memberIndexStr 会员下标
   * @return 结果分类
   * - true:出现在组中;
   * - false:未出现在组中;
   * - null:缓存中无该数据,无法判断
   */
  Boolean inGroup(String group, String memberIndexStr);

  /**
   * 判断会员是否出现在多个组中
   *
   * @param groups         组列表
   * @param memberIndexStr 会员索引的字符串
   * @return 结果描述
   * key: 组
   * value:
   * - true:出现在组中
   * - false:未出现在组中
   * - null:数据不在缓存中,无法判断
   */
  Map<String, Boolean> inGroup(Set<String> groups, String memberIndexStr);

}
