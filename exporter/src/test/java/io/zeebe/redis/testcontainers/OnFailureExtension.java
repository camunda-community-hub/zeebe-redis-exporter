package io.zeebe.redis.testcontainers;

import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

public class OnFailureExtension implements AfterTestExecutionCallback {

    ZeebeTestContainer zeebeTestContainer;

    public void setZeebeTestContainer(ZeebeTestContainer zeebeTestContainer) {
        this.zeebeTestContainer = zeebeTestContainer;
    }

    @Override
    public void afterTestExecution(ExtensionContext extensionContext) throws Exception {
        if (extensionContext.getExecutionException() != null && zeebeTestContainer != null) {
            zeebeTestContainer.writeContainerLogs();
        }
    }
}
