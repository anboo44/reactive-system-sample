package com.uet.microservices.lib.protocol;

import java.util.List;

public final class RpcCommon {
    public static List<Class<?>> rpcDscClassTypes = List.of(RpcNodeInfo.class, RpcNodeEvent.class, RpcBasicOperation.class);
}
