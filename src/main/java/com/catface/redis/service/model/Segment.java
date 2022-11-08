package com.catface.redis.service.model;

/**
 * @author catface
 * @since 2022/11/8
 */
public class Segment {
  private Long id;
  private  byte[] value;

  public Segment(Long id, byte[] value) {
    this.id = id;
    this.value = value;
  }

  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public byte[] getValue() {
    return value;
  }

  public void setValue(byte[] value) {
    this.value = value;
  }
}
