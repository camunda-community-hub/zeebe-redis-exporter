package io.zeebe.redis;

import io.lettuce.core.RedisClient;
import io.zeebe.redis.connect.java.ZeebeRedis;

import java.util.concurrent.CountDownLatch;

public class ExampleApplication {

  public static void main(String[] args) throws Exception {

    var redisClient = RedisClient.create("redis://127.0.0.1:6379");

    ZeebeRedis zeebeRedis =
        ZeebeRedis.newBuilder(redisClient)
            .addDeploymentListener(deployment -> System.out.println("> deployment: " + deployment))
            .addProcessInstanceListener(
                workflowInstance -> System.out.println("> workflow instance: " + workflowInstance))
            .addJobListener(job -> System.out.println("> job: " + job))
            .addIncidentListener(incident -> System.out.println("> incident: " + incident))
            .build();

    try {
      new CountDownLatch(1).await();

    } catch (InterruptedException e) {
      zeebeRedis.close();
      redisClient.shutdown();
    }
  }
}
