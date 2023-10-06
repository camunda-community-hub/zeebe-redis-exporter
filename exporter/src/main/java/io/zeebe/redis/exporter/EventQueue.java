package io.zeebe.redis.exporter;

import org.apache.commons.lang3.tuple.ImmutablePair;

import java.util.concurrent.ConcurrentLinkedQueue;

public class EventQueue {

    private final ConcurrentLinkedQueue<ImmutablePair<Long, RedisEvent>> queue = new ConcurrentLinkedQueue<>();

    public void addEvent(ImmutablePair<Long, RedisEvent> event) {
        queue.add(event);
    }

    public ImmutablePair<Long, RedisEvent> getNextEvent() {
        return queue.poll();
    }
}
