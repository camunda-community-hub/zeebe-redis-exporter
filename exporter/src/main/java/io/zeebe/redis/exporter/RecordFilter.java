package io.zeebe.redis.exporter;

import io.camunda.zeebe.exporter.api.context.Context;
import io.camunda.zeebe.protocol.record.RecordType;
import io.camunda.zeebe.protocol.record.ValueType;
import io.camunda.zeebe.protocol.record.intent.Intent;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public final class RecordFilter implements Context.RecordFilter {

  private final List<RecordType> enabledRecordTypes;
  private final List<ValueType> enabledValueTypes;
  private final List<Intent> enabledIntents;
  private final Map<Class<? extends Intent>, List<Intent>> enabledIntentsStrict;

  public RecordFilter(ExporterConfiguration config) {
    final List<String> enabledRecordTypeList =
        ExporterConfiguration.parseAsList(config.getEnabledRecordTypes());
    enabledRecordTypes =
        Arrays.stream(RecordType.values())
            .filter(
                recordType ->
                    enabledRecordTypeList.isEmpty()
                        || enabledRecordTypeList.contains(recordType.name()))
            .collect(Collectors.toList());

    final List<String> enabledValueTypeList =
        ExporterConfiguration.parseAsList(config.getEnabledValueTypes());
    enabledValueTypes =
        Arrays.stream(ValueType.values())
            .filter(
                valueType ->
                    enabledValueTypeList.isEmpty()
                        || enabledValueTypeList.contains(valueType.name()))
            .collect(Collectors.toList());

    final List<String> enabledIntentsList =
        ExporterConfiguration.parseAsList(config.getEnabledIntents());
    enabledIntents =
        Intent.INTENT_CLASSES.stream()
            .flatMap(clazz -> Arrays.stream(clazz.getEnumConstants()))
            .filter(
                intent ->
                    enabledIntentsList.isEmpty() || enabledIntentsList.contains(intent.name()))
            .collect(Collectors.toList());

    final Map<String, List<String>> enabledIntentsMap =
        ExporterConfiguration.parseAsMap(config.getEnabledIntents());
    enabledIntentsStrict =
        enabledIntentsMap.isEmpty()
            ? new HashMap<>()
            : Intent.INTENT_CLASSES.stream()
                .filter(clazz -> enabledIntentsMap.containsKey(clazz.getSimpleName()))
                .collect(
                    Collectors.toMap(
                        clazz -> clazz,
                        clazz -> {
                          List<String> intentNames = enabledIntentsMap.get(clazz.getSimpleName());
                          return Arrays.stream(clazz.getEnumConstants())
                              .filter(intent -> intentNames.contains(intent.name()))
                              .collect(Collectors.toList());
                        }));
  }

  @Override
  public boolean acceptType(RecordType recordType) {
    return enabledRecordTypes.contains(recordType);
  }

  @Override
  public boolean acceptValue(ValueType valueType) {
    return enabledValueTypes.contains(valueType);
  }

  @Override
  public boolean acceptIntent(Intent intent) {
    if (!enabledIntentsStrict.isEmpty()) {
      Class<? extends Intent> intentClass = intent.getClass();

      if (enabledIntentsStrict.containsKey(intentClass)) {
        List<Intent> allowedIntents = enabledIntentsStrict.get(intentClass);
        return allowedIntents.contains(intent);
      }
      return false;
    }
    // Fallback to the fuzzy list-based filtering
    return enabledIntents.contains(intent);
  }
}
