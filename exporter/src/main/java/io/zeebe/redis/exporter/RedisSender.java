package io.zeebe.redis.exporter;

import io.camunda.zeebe.exporter.api.context.Controller;
import io.lettuce.core.*;
import io.lettuce.core.api.async.RedisStreamAsyncCommands;
import io.micrometer.core.instrument.MeterRegistry;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;

public class RedisSender {

  private final Logger logger;
  private final RedisMetrics redisMetrics;
  private final Controller controller;
  private final UniversalRedisConnection<String, ?> redisConnection;

  private final AtomicBoolean redisConnected = new AtomicBoolean(true);

  private final int batchSize;

  private final List<ImmutablePair<Long, RedisEvent>> deQueue = new ArrayList<>();

  private final AtomicBoolean metricsBulkRecorded = new AtomicBoolean(false);
  private final AtomicLong lastRecordedBulk = new AtomicLong(Long.MAX_VALUE);
  // best practice default scrape interval for metrics is 60 seconds, hence wait 60s before
  // resetting to 0
  private static final long RESET_METRICS_AFTER_MILLIS = 60000L;

  public RedisSender(
      ExporterConfiguration configuration,
      Controller controller,
      UniversalRedisConnection<String, ?> redisConnection,
      MeterRegistry meterRegistry,
      Logger logger) {
    this.batchSize = configuration.getBatchSize();
    this.controller = controller;
    this.redisConnection = redisConnection;
    this.logger = logger;
    this.redisMetrics = new RedisMetrics(meterRegistry);
    this.redisConnection.setAutoFlushCommands(false);
    this.redisConnection.addListener(
        new RedisConnectionStateListener() {
          private final AtomicLong disconnectAtSystemTime = new AtomicLong(-1);

          @Override
          public void onRedisConnected(
              RedisChannelHandler<?, ?> connection, SocketAddress socketAddress) {
            redisConnected.set(true);
            if (disconnectAtSystemTime.get() >= 0) {
              logger.info(
                  "Redis connection re-established to {} after {} ms",
                  configuration.getRemoteAddress().get(),
                  System.currentTimeMillis() - disconnectAtSystemTime.get());
              disconnectAtSystemTime.set(-1);
            } else {
              logger.info(
                  "Redis connection re-established to {}", configuration.getRemoteAddress().get());
            }
          }

          @Override
          public void onRedisDisconnected(RedisChannelHandler<?, ?> connection) {
            redisConnected.set(false);
            disconnectAtSystemTime.set(System.currentTimeMillis());
            logger.warn("Redis connection failure to {}", configuration.getRemoteAddress().get());
          }
        });
  }

  void sendFrom(EventQueue eventQueue) {
    if (!redisConnected.get() || !sendDeQueue() || eventQueue.isEmpty()) {
      if (metricsBulkRecorded.get() && isMetricsWatchStopped()) {
        // set back bulk metric values to 0 once because there is nothing to send
        redisMetrics.recordBulkSize(0);
        redisMetrics.recordBulkMemorySize(0);
        metricsBulkRecorded.set(false);
      }
      return;
    }
    int recordBulkSize = 0;
    int recordBulkMemorySize = 0;
    try (final var ignored = redisMetrics.measureFlushDuration()) {
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
          recordBulkSize++;
          recordBulkMemorySize += eventValue.memorySize;
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
      redisMetrics.recordBulkSize(recordBulkSize);
      redisMetrics.recordBulkMemorySize(recordBulkMemorySize);
      metricsBulkRecorded.set(true);
      startMetricsStopWatch();
    } catch (RedisCommandTimeoutException | RedisConnectionException ex) {
      redisMetrics.recordFailedFlush();
      logger.error(
          "Error when sending events to Redis due to possible Redis unavailability: {}",
          ex.getMessage());
    } catch (Exception ex) {
      redisMetrics.recordFailedFlush();
      logger.error("Error when sending events to Redis", ex);
    }
  }

  private boolean sendDeQueue() {
    if (deQueue.isEmpty()) {
      return true;
    }
    int recordBulkSize = 0;
    int recordBulkMemorySize = 0;
    try (final var ignored = redisMetrics.measureFlushDuration()) {
      Long positionOfLastRecordInBatch = -1L;
      RedisStreamAsyncCommands<String, ?> commands = redisConnection.asyncStreamCommands();
      List<RedisFuture<?>> futures = new ArrayList<>();
      for (var nextEvent : deQueue) {
        var eventValue = nextEvent.getValue();
        futures.add(
            commands.xadd(eventValue.stream, String.valueOf(eventValue.key), eventValue.value));
        positionOfLastRecordInBatch = nextEvent.getKey();
        recordBulkSize++;
        recordBulkMemorySize += eventValue.memorySize;
      }
      redisConnection.flushCommands();
      boolean result =
          LettuceFutures.awaitAll(
              7, TimeUnit.SECONDS, futures.toArray(new RedisFuture[futures.size()]));
      if (result) {
        controller.updateLastExportedRecordPosition(positionOfLastRecordInBatch);
        logger.debug("Exported {} dequeued events to Redis", futures.size());
        deQueue.clear();
        redisMetrics.recordBulkSize(recordBulkSize);
        redisMetrics.recordBulkMemorySize(recordBulkMemorySize);
        metricsBulkRecorded.set(true);
        startMetricsStopWatch();
        return true;
      }
    } catch (RedisCommandTimeoutException | RedisConnectionException ex) {
      redisMetrics.recordFailedFlush();
      logger.error(
          "Error when sending dequeued events to Redis due to possible Redis unavailability: {}",
          ex.getMessage());
    } catch (Exception ex) {
      redisMetrics.recordFailedFlush();
      logger.error("Error when sending dequeued events to Redis", ex);
    }
    return false;
  }

  private void startMetricsStopWatch() {
    lastRecordedBulk.set(System.currentTimeMillis());
  }

  private boolean isMetricsWatchStopped() {
    return System.currentTimeMillis() - lastRecordedBulk.get() > RESET_METRICS_AFTER_MILLIS;
  }
}
