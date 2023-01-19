package io.zeebe.redis;

import io.lettuce.core.RedisClient;
import io.zeebe.redis.connect.java.ZeebeRedis;
import io.zeebe.redis.testcontainers.ZeebeTestContainer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
public class ZeebeRedisClientTest {

  private static final Duration CONNECTION_TIMEOUT = Duration.ofSeconds(5);

  private ZeebeRedis zeebeRedis;

  @Container
  public ZeebeTestContainer zeebeContainer = ZeebeTestContainer.withDefaultConfig();

  private RedisClient redisClient;

  @BeforeEach
  public void init() {
    redisClient = RedisClient.create(zeebeContainer.getRedisAddress());
  }

  @AfterEach
  public void cleanUp() throws Exception {
    if (zeebeRedis != null) zeebeRedis.close();
    redisClient.shutdown();
  }

  @Test
  public void shouldCloseIfRedisIsUnavailable() {
    // given
    zeebeRedis = ZeebeRedis.newBuilder(redisClient).build();

    // when
    zeebeContainer.getRedisContainer().stop();

    // then
    Awaitility.await()
        .atMost(CONNECTION_TIMEOUT.multipliedBy(2))
        .untilAsserted(() -> assertThat(zeebeRedis.isClosed()).isTrue());
  }
}
