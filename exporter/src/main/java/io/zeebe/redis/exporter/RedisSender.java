package io.zeebe.redis.exporter;

import io.camunda.zeebe.exporter.api.context.Controller;
import io.lettuce.core.*;
import io.lettuce.core.api.async.RedisStreamAsyncCommands;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;

public class RedisSender {

  private final Logger logger;
  private final Controller controller;
  private final UniversalRedisConnection<String, ?> redisConnection;

  private final AtomicBoolean redisConnected = new AtomicBoolean(true);

  private final int batchSize;

  private final List<ImmutablePair<Long, RedisEvent>> deQueue = new ArrayList<>();

  public RedisSender(
      ExporterConfiguration configuration,
      Controller controller,
      UniversalRedisConnection<String, ?> redisConnection,
      Logger logger) {
    this.batchSize = configuration.getBatchSize();
    this.controller = controller;
    this.redisConnection = redisConnection;
    this.logger = logger;
    this.redisConnection.setAutoFlushCommands(false);
    this.redisConnection.addListener(
        new RedisConnectionStateListener() {
          @Override
          public void onRedisConnected(
              RedisChannelHandler<?, ?> connection, SocketAddress socketAddress) {
            redisConnected.set(true);
          }

          @Override
          public void onRedisDisconnected(RedisChannelHandler<?, ?> connection) {
            redisConnected.set(false);
          }
        });
  }

  void sendFrom(EventQueue eventQueue) {
    if (!redisConnected.get() || !sendDeQueue()) {
      return;
    }
    try {
      Long positionOfLastRecordInBatch = -1L;
      RedisStreamAsyncCommands<String, ?> commands = redisConnection.asyncStreamCommands();
      List<RedisFuture<?>> futures = new ArrayList<>();
      ImmutablePair<Long, RedisEvent> nextEvent = eventQueue.getNextEvent();
      while (nextEvent != null) {
        for (int i = 0; i < batchSize; i++) {
          deQueue.add(nextEvent);
          var eventValue = nextEvent.getValue();
          futures.add(
              commands.xadd(eventValue.stream, String.valueOf(eventValue.key), eventValue.value));
          positionOfLastRecordInBatch = nextEvent.getKey();
          nextEvent = eventQueue.getNextEvent();
          if (nextEvent == null) {
            break;
          }
        }
        if (futures.size() > 0) {
          redisConnection.flushCommands();
          boolean result =
              LettuceFutures.awaitAll(
                  7, TimeUnit.SECONDS, futures.toArray(new RedisFuture[futures.size()]));
          if (result) {
            controller.updateLastExportedRecordPosition(positionOfLastRecordInBatch);
            deQueue.clear();
            logger.debug("Exported {} events to Redis", futures.size());
            futures.clear();
          } else {
            break;
          }
        }
      }
    } catch (RedisCommandTimeoutException | RedisConnectionException ex) {
      logger.error(
          "Error when sending events to Redis due to possible Redis unavailability: {}",
          ex.getMessage());
    } catch (Exception ex) {
      logger.error("Error when sending events to Redis", ex);
    }
  }

  private boolean sendDeQueue() {
    if (deQueue.isEmpty()) {
      return true;
    }
    try {
      Long positionOfLastRecordInBatch = -1L;
      RedisStreamAsyncCommands<String, ?> commands = redisConnection.asyncStreamCommands();
      List<RedisFuture<?>> futures = new ArrayList<>();
      for (var nextEvent : deQueue) {
        var eventValue = nextEvent.getValue();
        futures.add(
            commands.xadd(eventValue.stream, String.valueOf(eventValue.key), eventValue.value));
        positionOfLastRecordInBatch = nextEvent.getKey();
      }
      redisConnection.flushCommands();
      boolean result =
          LettuceFutures.awaitAll(
              7, TimeUnit.SECONDS, futures.toArray(new RedisFuture[futures.size()]));
      if (result) {
        controller.updateLastExportedRecordPosition(positionOfLastRecordInBatch);
        logger.debug("Exported {} dequeued events to Redis", futures.size());
        deQueue.clear();
        return true;
      }
    } catch (RedisCommandTimeoutException | RedisConnectionException ex) {
      logger.error(
          "Error when sending dequeued events to Redis due to possible Redis unavailability: {}",
          ex.getMessage());
    } catch (Exception ex) {
      logger.error("Error when sending dequeued events to Redis", ex);
    }
    return false;
  }
}
