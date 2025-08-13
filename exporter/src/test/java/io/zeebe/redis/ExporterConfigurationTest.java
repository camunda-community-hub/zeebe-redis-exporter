package io.zeebe.redis;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.zeebe.redis.exporter.ExporterConfiguration;
import java.util.Arrays;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class ExporterConfigurationTest {

  @Test
  public void testParseAsList() {

    var valueList = "a, b, c, d";
    var expectedList = Arrays.asList("a", "b", "c", "d");

    assertEquals(expectedList, ExporterConfiguration.parseAsList(valueList));
  }

  @Test
  public void testParseAsMap() {
    var valueMap = "key1=value1,value2;key2=value3";
    var expectedMap =
        Map.of(
            "key1", Arrays.asList("value1", "value2"),
            "key2", Arrays.asList("value3"));

    assertEquals(expectedMap, ExporterConfiguration.parseAsMap(valueMap));
  }
}
