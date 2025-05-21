package io.zeebe.redis.exporter;

public class RedisEvent {

  String stream;
  long key;
  Object value;
  int memorySize;

  public RedisEvent(String stream, long key, Object value, int memorySize) {
    this.stream = stream;
    this.key = key;
    this.value = value;
    this.memorySize = memorySize;
  }
}
