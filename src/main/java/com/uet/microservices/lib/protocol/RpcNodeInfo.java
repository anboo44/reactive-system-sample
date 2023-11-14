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
}
