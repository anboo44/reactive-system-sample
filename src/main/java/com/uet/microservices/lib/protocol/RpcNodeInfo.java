package com.uet.microservices.lib.protocol;

import com.uet.microservices.lib.model.NodeAddr;
import com.uet.microservices.lib.model.NodeType;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;

public class RpcNodeInfo {
    public @Serialize(order = 0) String   name;
    public @Serialize(order = 1) NodeType nodeType;
    public @Serialize(order = 2) boolean  isAlone;

    public @Serialize(order = 3) NodeAddr dscServerAddr;
    public @Serialize(order = 4) NodeAddr mainServerAddr;

    public RpcNodeInfo(
        @Deserialize("name") String name,
        @Deserialize("nodeType") NodeType nodeType,
        @Deserialize("isAlone") boolean isAlone,
        @Deserialize("dscServerAddr") NodeAddr dscServerAddr,
        @Deserialize("mainServerAddr") NodeAddr mainServerAddr
    ) {
        this.name           = name;
        this.nodeType       = nodeType;
        this.isAlone        = isAlone;
        this.dscServerAddr  = dscServerAddr;
        this.mainServerAddr = mainServerAddr;
    }

    @Override
    public String toString() {
        return "RpcNodeInfo(" + name + ", " + nodeType + ", " + dscServerAddr + ", " + mainServerAddr + ", " + isAlone + ")";
    }

    @Override
    public int hashCode() {
        return name.hashCode() + nodeType.hashCode() + dscServerAddr.hashCode() + mainServerAddr.hashCode() + (isAlone ? 1 : 0);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof RpcNodeInfo other) {
            return this.name.equals(other.name) &&
                   this.nodeType.equals(other.nodeType) &&
                   this.dscServerAddr.equals(other.dscServerAddr) &&
                   this.mainServerAddr.equals(other.mainServerAddr);
        }

        if (obj instanceof RpcNodeEvent ev) {
            return this.name.equals(ev.nodeName) &&
                   this.nodeType.equals(ev.nodeType) && (
                       this.dscServerAddr.equals(ev.nodeAddr) ||
                       this.mainServerAddr.equals(ev.nodeAddr)
                   );
        }

        return false;
    }

}
