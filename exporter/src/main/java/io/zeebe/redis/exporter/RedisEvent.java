package io.zeebe.redis.exporter;

public class RedisEvent {

    String stream;
    long key;
    Object value;

    public RedisEvent(String stream, long key, Object value) {
        this.stream = stream;
        this.key = key;
        this.value = value;
    }
}
