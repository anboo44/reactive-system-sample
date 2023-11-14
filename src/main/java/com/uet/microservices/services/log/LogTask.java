package com.uet.microservices.services.log;

import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;

public class LogTask {
    public final @Serialize(order = 0) TaskType taskType;
    public final @Serialize(order = 1) String   message;

    public LogTask(@Deserialize("taskType") TaskType taskType, @Deserialize("message") String message) {
        this.taskType = taskType;
        this.message  = message;
    }

    public static LogTask create(String message) {
        if (message.length() < 20) {
            return new LogTask(TaskType.INFO, message);
        } else if (message.length() < 40) {
            return new LogTask(TaskType.WARN, message);
        } else {
            return new LogTask(TaskType.ERROR, message);
        }
    }
}
