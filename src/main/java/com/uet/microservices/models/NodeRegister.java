package com.uet.microservices.models;

import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;

import java.util.List;

public class NodeRegister {
    public @Serialize(order = 0) int          servicePort;
    public @Serialize(order = 1) int          discoveryPort;
    public @Serialize(order = 2) String       nodeType;
    public @Serialize(order = 3) List<String> seedTypes;

    public NodeRegister(
        @Deserialize("servicePort") int servicePort,
        @Deserialize("discoveryPort") int discoveryPort,
        @Deserialize("nodeType") String nodeType,
        @Deserialize("seedTypes") List<String> seedTypes
    ) {
        this.servicePort   = servicePort;
        this.discoveryPort = discoveryPort;
        this.nodeType      = nodeType;
        this.seedTypes     = seedTypes;
    }
}
