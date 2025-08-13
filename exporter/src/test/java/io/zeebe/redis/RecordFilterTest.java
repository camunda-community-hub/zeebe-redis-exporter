package io.zeebe.redis;

import static org.junit.jupiter.api.Assertions.*;

import io.camunda.zeebe.protocol.record.RecordType;
import io.camunda.zeebe.protocol.record.ValueType;
import io.camunda.zeebe.protocol.record.intent.DeploymentIntent;
import io.camunda.zeebe.protocol.record.intent.Intent;
import io.camunda.zeebe.protocol.record.intent.JobIntent;
import io.camunda.zeebe.protocol.record.intent.ProcessIntent;
import io.zeebe.redis.exporter.ExporterConfiguration;
import io.zeebe.redis.exporter.RecordFilter;
import java.util.Arrays;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class RecordFilterTest {

  private RecordFilter recordFilter;
  private TestExporterConfiguration mockConfig;

  @BeforeEach
  public void setUp() {
    mockConfig = new TestExporterConfiguration();
    mockConfig.setEnabledRecordTypes("EVENT,COMMAND");
    mockConfig.setEnabledValueTypes("USER_TASK,JOB");
    mockConfig.setEnabledIntents("CREATED,UPDATED");

    recordFilter = new RecordFilter(mockConfig);
  }

  @Test
  public void testAcceptType() {
    assertTrue(recordFilter.acceptType(RecordType.EVENT));
    assertTrue(recordFilter.acceptType(RecordType.COMMAND));
    assertFalse(recordFilter.acceptType(RecordType.COMMAND_REJECTION));
  }

  @Test
  public void testAcceptValue() {
    assertTrue(recordFilter.acceptValue(ValueType.USER_TASK));
    assertTrue(recordFilter.acceptValue(ValueType.JOB));
    assertFalse(recordFilter.acceptValue(ValueType.VARIABLE));
  }

  @Test
  public void testAcceptIntent() {
    // Test with allowed intents
    assertTrue(recordFilter.acceptIntent(JobIntent.CREATED));
    assertTrue(recordFilter.acceptIntent(JobIntent.UPDATED));
    assertFalse(recordFilter.acceptIntent(JobIntent.COMPLETED));
  }

  @Test
  public void testAcceptIntentFuzzy() {
    TestExporterConfiguration emptyConfig = new TestExporterConfiguration();
    // Intents which are available:
    // JobIntent -> CREATED,UPDATED
    // ProcessIntent -> CREATED
    // DeploymentIntent -> CREATED
    emptyConfig.setEnabledIntents("CREATED,UPDATED");

    RecordFilter recordFilter = new RecordFilter(emptyConfig);

    assertTrue(recordFilter.acceptIntent(JobIntent.CREATED));
    assertTrue(recordFilter.acceptIntent(JobIntent.UPDATED));
    assertFalse(recordFilter.acceptIntent(JobIntent.COMPLETED));
    // Test may be true if Intent declaration can be fuzzy
    assertTrue(recordFilter.acceptIntent(ProcessIntent.CREATED));
    assertTrue(recordFilter.acceptIntent(DeploymentIntent.CREATED));
  }

  @Test
  public void testAcceptIntentStrict() {
    TestExporterConfiguration emptyConfig = new TestExporterConfiguration();
    // Intents which are available:
    // JobIntent -> CREATED,UPDATED
    // ProcessIntent -> CREATED
    // DeploymentIntent -> CREATED
    emptyConfig.setEnabledIntents("JobIntent=CREATED,UPDATED;DeploymentIntent=CREATED");

    RecordFilter recordFilter = new RecordFilter(emptyConfig);

    assertTrue(recordFilter.acceptIntent(JobIntent.CREATED));
    assertTrue(recordFilter.acceptIntent(JobIntent.UPDATED));
    assertFalse(recordFilter.acceptIntent(JobIntent.COMPLETED));
    // Test must be false if Intent declaration should be strict
    assertFalse(recordFilter.acceptIntent(ProcessIntent.CREATED));
    assertTrue(recordFilter.acceptIntent(DeploymentIntent.CREATED));
  }

  @Test
  public void testEmptyConfigurationAcceptsDefault() {
    // Test with empty configuration - should accept all types
    TestExporterConfiguration emptyConfig = new TestExporterConfiguration();
    emptyConfig.setEnabledRecordTypes("");
    emptyConfig.setEnabledValueTypes("");
    emptyConfig.setEnabledIntents("");

    RecordFilter emptyFilter = new RecordFilter(emptyConfig);

    // Should accept all record types when configuration is empty
    for (RecordType recordType : RecordType.values()) {
      assertTrue(emptyFilter.acceptType(recordType));
    }

    // Should accept all value types when configuration is empty
    for (ValueType valueType : ValueType.values()) {
      assertTrue(emptyFilter.acceptValue(valueType));
    }

    for (Intent intent : Intent.INTENT_CLASSES.stream()
        .flatMap(clazz -> Arrays.stream(clazz.getEnumConstants()))
        .collect(Collectors.toList())) {
      // Should accept all intents when configuration is empty
      assertTrue(emptyFilter.acceptIntent(intent));
    }
  }

  @Test
  public void testConfigurationWithWhitespaceHandling() {
    // Test configuration with extra whitespace
    TestExporterConfiguration whitespaceConfig = new TestExporterConfiguration();
    whitespaceConfig.setEnabledRecordTypes(" EVENT , COMMAND ");
    whitespaceConfig.setEnabledValueTypes(" USER_TASK , JOB ");
    whitespaceConfig.setEnabledIntents(" CREATED , UPDATED ");

    RecordFilter whitespaceFilter = new RecordFilter(whitespaceConfig);

    assertTrue(whitespaceFilter.acceptType(RecordType.EVENT));
    assertTrue(whitespaceFilter.acceptValue(ValueType.USER_TASK));
  }

  // Test implementation of ExporterConfiguration
  private static class TestExporterConfiguration extends ExporterConfiguration {
    private String enabledRecordTypes = "";
    private String enabledValueTypes = "";
    private String enabledIntents = "";

    @Override
    public String getEnabledRecordTypes() {
      return enabledRecordTypes;
    }

    public void setEnabledRecordTypes(String enabledRecordTypes) {
      this.enabledRecordTypes = enabledRecordTypes;
    }

    @Override
    public String getEnabledValueTypes() {
      return enabledValueTypes;
    }

    public void setEnabledValueTypes(String enabledValueTypes) {
      this.enabledValueTypes = enabledValueTypes;
    }

    @Override
    public String getEnabledIntents() {
      return enabledIntents;
    }

    public void setEnabledIntents(String enabledIntents) {
      this.enabledIntents = enabledIntents;
    }
  }
}
