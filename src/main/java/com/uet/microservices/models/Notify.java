package com.uet.microservices.models;

import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;

public class Notify {
    public @Serialize(order = 0) int     port;
    public @Serialize(order = 1) String  nodeType;
    public @Serialize(order = 2) boolean isUp;

    public Notify(
        @Deserialize("port") int port,
        @Deserialize("nodeType") String nodeType,
        @Deserialize("isUp") boolean isUp
    ) {
        this.port     = port;
        this.nodeType = nodeType;
        this.isUp     = isUp;
    }
}
