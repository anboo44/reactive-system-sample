package com.uet.microservices.lib.protocol;

import com.uet.microservices.lib.model.NodeAddr;
import com.uet.microservices.lib.model.NodeStatus;
import com.uet.microservices.lib.model.NodeType;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;

public class RpcNodeEvent {
    public @Serialize(order = 0) NodeAddr   nodeAddr;
    public @Serialize(order = 1) NodeStatus status;
    public @Serialize(order = 2) NodeType   nodeType;
    public @Serialize(order = 3) String     nodeName;

    public RpcNodeEvent(
        @Deserialize("nodeAddr") NodeAddr nodeAddr,
        @Deserialize("status") NodeStatus status,
        @Deserialize("nodeType") NodeType nodeType,
        @Deserialize("nodeName") String nodeName
    ) {
        this.nodeAddr = nodeAddr;
        this.status   = status;
        this.nodeType = nodeType;
        this.nodeName = nodeName;
    }

    public RpcNodeEvent(RpcNodeInfo nodeInfo, boolean isUp) {
        this.nodeAddr = nodeInfo.mainServerAddr;
        this.status   = isUp ? NodeStatus.UP : NodeStatus.DOWN;
        this.nodeType = nodeInfo.nodeType;
        this.nodeName = nodeInfo.name;
    }

    @Override
    public String toString() {
        return "RpcNodeEvent(" + nodeAddr + ", " + status + ", " + nodeType + ", " + nodeName + ")";
    }

    @Override
    public int hashCode() {
        return nodeAddr.hashCode() + status.hashCode() + nodeType.hashCode() + nodeName.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof RpcNodeEvent other) {
            return this.nodeAddr.equals(other.nodeAddr) &&
                   this.nodeType.equals(other.nodeType) &&
                   this.nodeName.equals(other.nodeName);
        }
        return false;
    }
}
