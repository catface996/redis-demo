package com.catface.redis;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = {
    "com.catface.redis"
})
public class RedisDemoApplication {

  public static void main(String[] args) {
    SpringApplication.run(RedisDemoApplication.class, args);
  }

}
