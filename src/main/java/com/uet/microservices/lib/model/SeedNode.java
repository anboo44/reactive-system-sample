package com.uet.microservices.lib.model;

public class SeedNode {
    public final String   name;
    public final NodeType nodeType;
    public final NodeAddr nodeAddr;

    public SeedNode(String name, NodeType nodeType, NodeAddr nodeAddr) {
        this.name     = name;
        this.nodeType = nodeType;
        this.nodeAddr = nodeAddr;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof SeedNode other) {
            return this.name.equals(other.name) &&
                   this.nodeType.equals(other.nodeType) &&
                   this.nodeAddr.equals(other.nodeAddr);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return name.hashCode() + nodeType.hashCode() + nodeAddr.hashCode();
    }

    @Override
    public String toString() {
        return "SeedNode(" + name + ", " + nodeType + ", " + nodeAddr + ")";
    }
}
