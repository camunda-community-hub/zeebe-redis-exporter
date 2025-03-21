package io.zeebe.redis.testcontainers;

import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.jupiter.api.extension.*;
import org.testcontainers.junit.jupiter.Container;

public class OnFailureExtension implements TestWatcher, BeforeEachCallback, AfterEachCallback {

  private static final Logger LOGGER = Logger.getLogger("ZeebeTestContainer");

  private String containerLogs;

  @Override
  public void testFailed(ExtensionContext context, Throwable cause) {
    if (containerLogs != null) {
      var prefix =
          "\n----------------------------------------------------------------------"
              + "\nZeebe container log for "
              + context.getDisplayName()
              + "\n----------------------------------------------------------------------\n";
      LOGGER.log(Level.WARNING, prefix + containerLogs);
    }
  }

  @Override
  public void afterEach(ExtensionContext extensionContext) {
    var testInstance = extensionContext.getTestInstance();
    if (testInstance.isEmpty()) return;
    var zeebeTestContainerField =
        Arrays.stream(testInstance.get().getClass().getDeclaredFields())
            .filter(
                field ->
                    field.isAnnotationPresent(Container.class)
                        && field.getType().equals(ZeebeTestContainer.class))
            .findFirst();
    if (zeebeTestContainerField.isPresent()) {
      var field = zeebeTestContainerField.get();
      field.setAccessible(true);
      try {
        var zeebeTestContainer = (ZeebeTestContainer) field.get(testInstance.get());
        containerLogs = zeebeTestContainer.getLogs();
      } catch (IllegalAccessException ex) {
        // NOOP
      }
    }
  }

  @Override
  public void beforeEach(ExtensionContext extensionContext) {
    containerLogs = null;
  }
}
