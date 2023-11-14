package com.uet.microservices.services.worker;

import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;

import java.util.ArrayList;
import java.util.List;

public class WorkerTask {
    public @Serialize(order = 0) int from;
    public @Serialize(order = 1) int to;

    public WorkerTask(@Deserialize("from") int from, @Deserialize("to") int to) {
        this.from = from;
        this.to   = to;
    }

    public static List<WorkerTask> breakBig(int from, int to, int step) {
        var diff = to - from;
        if (diff <= step) {
            return List.of(new WorkerTask(from, to));
        } else {
            var tasks = new ArrayList<WorkerTask>();
            var cur   = from;

            while (true) {
                var next = cur + step;
                if (next >= to) {
                    tasks.add(new WorkerTask(cur, to));
                    break;
                } else {
                    tasks.add(new WorkerTask(cur, next));
                    cur = next + 1;
                }
            }

            return tasks;
        }
    }

    @Override
    public String toString() {
        return "WorkerTask(" + from + ", " + to + ")";
    }
}
