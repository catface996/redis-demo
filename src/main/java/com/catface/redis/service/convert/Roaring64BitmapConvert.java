package com.catface.redis.service.convert;

import java.util.HashMap;
import java.util.Map;
import org.roaringbitmap.longlong.Roaring64Bitmap;

/**
 * @author catface
 * @since 2022/11/9
 */
public class Roaring64BitmapConvert {

  private static final char SPLIT_CHAR = ',';
  private static final String SPLIT_STRING = ",";
  private static final char ZERO_CHAR = '0';


  /**
   * 从会员下标的数组转换成Roaring64Bitmap
   *
   * @param memberIndexArrStr 会员下标数组的字符串
   * @return 分段后的RoaringBitmap
   */
  public static Map<Long, Roaring64Bitmap> convertFromMemberIndexArrStr(String memberIndexArrStr,
      int segmentNum) {
    Map<Long, Roaring64Bitmap> segmentMap = new HashMap<>(segmentNum);

    if (memberIndexArrStr == null) {
      return segmentMap;
    }

    // 初始化map,避免后续每次get出Roaring64Bitmap都要做非空判断
    for (long i = 0; i < segmentNum; i++) {
      segmentMap.put(i, new Roaring64Bitmap());
    }

    long memberIndex = memberIndexArrStr.charAt(1) - ZERO_CHAR;

    // 跳过第一个字符和最后一个字符,第一个字符是[,最后一个字符是]
    int startIndex = 2;
    char currentChar;
    long segmentId;
    while (startIndex < memberIndexArrStr.length() - 2) {
      currentChar = memberIndexArrStr.charAt(startIndex);
      if (currentChar == SPLIT_CHAR) {
        // 发现分隔符,表示一个数字的结束,计算出的数字是一个会员下标,此时对会员下标做分段
        segmentId = memberIndex % segmentNum;
        segmentMap.get(segmentId).add(memberIndex);
        // 分段结束后,要重置会员喜爱宝,一遍下一轮的数字解析
        startIndex++;
        // 此处的++一定不会越界，while判断条件是 < length-2
        memberIndex = memberIndexArrStr.charAt(startIndex) - ZERO_CHAR;
      } else {
        // currentChar是个数字,currentChar - '0'是为了计算数字的大小
        memberIndex = memberIndex * 10 + currentChar - ZERO_CHAR;
      }
      startIndex++;
    }

    // 这样写,是为了避免在while里判断startIndex++执行两次后,是否越界
    // 因为while判断条件是 < length-2,故当前的下标对应的字符是一个有效字符,应计入memberIndex
    currentChar = memberIndexArrStr.charAt(startIndex);
    memberIndex = (memberIndex << 1) + (memberIndex << 3) + currentChar - ZERO_CHAR;
    segmentId = memberIndex % segmentNum;
    segmentMap.get(segmentId).add(memberIndex);

    return segmentMap;
  }

  /**
   * 从会员下标的数组转换成Roaring64Bitmap
   *
   * @param memberIndexArrStr 会员下标数组的字符串
   * @return 分段后的RoaringBitmap
   */
  public static Map<Long, Roaring64Bitmap> convertFromMemberIndexArrStrSplit(
      String memberIndexArrStr, int segmentNum) {
    Map<Long, Roaring64Bitmap> segmentMap = new HashMap<>(segmentNum);
    if (memberIndexArrStr == null) {
      return segmentMap;
    }

    String[] memberIndexStrArr = memberIndexArrStr.split(SPLIT_STRING);
    if (memberIndexArrStr.length() <= 1) {
      return segmentMap;
    }

    // 第一个字符串和最后一个字符串,替换掉中括号
    memberIndexStrArr[0] = memberIndexStrArr[0].replace("[", "");
    memberIndexStrArr[memberIndexStrArr.length - 1] = memberIndexStrArr[memberIndexStrArr.length
        - 1].replace("]", "");

    for (long i = 0; i < segmentNum; i++) {
      segmentMap.put(i, new Roaring64Bitmap());
    }

    for (String memberIndexStr : memberIndexStrArr) {
      long memberIndex = Long.parseLong(memberIndexStr);
      long segmentId = memberIndex % segmentNum;
      segmentMap.get(segmentId).add(memberIndex);
    }

    return segmentMap;
  }

}
