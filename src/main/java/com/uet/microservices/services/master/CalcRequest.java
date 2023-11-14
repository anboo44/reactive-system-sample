package com.uet.microservices.services.master;

import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;

public class CalcRequest {
    public @Serialize(order = 0) int from;
    public @Serialize(order = 1) int to;

    public CalcRequest(@Deserialize("from") int from, @Deserialize("to") int to) {
        this.from = from;
        this.to   = to;
    }

    @Override
    public String toString() {
        return "CalcRequest(" + from + ", " + to + ")";
    }
}
