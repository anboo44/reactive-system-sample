package com.uet.microservices.lib.model;

import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;

import java.net.InetSocketAddress;

public class NodeAddr {
    public @Serialize(order = 0) String host;
    public @Serialize(order = 1) int    port;

    public NodeAddr(@Deserialize("host") String host, @Deserialize("port") int port) {
        this.host = host;
        this.port = port;
    }

    public InetSocketAddress toSocketAddr() {
        return new InetSocketAddress(host, port);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof NodeAddr other) {
            return this.host.equals(other.host) && this.port == other.port;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return host.hashCode() + port;
    }

    @Override
    public String toString() {
        return "NodeAddr(" + host + ", " + port + ")";
    }
}
