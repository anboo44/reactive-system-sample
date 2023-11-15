package com.uet.microservices.lib.service;

import com.uet.microservices.lib.model.NodeType;
import com.uet.microservices.lib.model.SeedNode;
import io.activej.eventloop.Eventloop;
import io.activej.rpc.client.RpcClient;
import io.activej.rpc.client.sender.strategy.RpcStrategies;

import java.time.Duration;
import java.util.*;

public class SeedNodeManager {
    private final Eventloop                     eventloop;
    private final Set<SeedNode>                 seedNodes;
    private final Map<NodeType, RpcClient>      senderGroup;
    private final Map<NodeType, List<Class<?>>> classTypes;

    public SeedNodeManager(Eventloop eventloop, Map<NodeType, List<Class<?>>> classTypes) {
        this.seedNodes   = new HashSet<>();
        this.senderGroup = new HashMap<>();
        this.eventloop   = eventloop;
        this.classTypes  = classTypes;
    }

    public boolean addNewNode(SeedNode node) {
        var targetClassTypes = classTypes.get(node.nodeType);
        if (targetClassTypes == null) {
            throw new RuntimeException("Missing classTypes for nodeType: " + node.nodeType);
        }

        var isAdded = this.seedNodes.add(node);
        if (isAdded) updateSender(node.nodeType, targetClassTypes);

        return isAdded;
    }

    public boolean removeNode(SeedNode targetNode) {
        var targetClassTypes = classTypes.get(targetNode.nodeType);
        if (targetClassTypes == null) {
            throw new RuntimeException("Missing classTypes for nodeType: " + targetNode.nodeType);
        }

        var isRemoved = this.seedNodes.removeIf(node -> node.equals(targetNode));
        if (isRemoved) updateSender(targetNode.nodeType, targetClassTypes);

        return isRemoved;
    }

    public RpcClient getSender(NodeType nodeType) {
        return Optional.ofNullable(this.senderGroup.get(nodeType))
                       .orElseThrow(() -> new RuntimeException("No available sender"));
    }

    private void updateSender(NodeType nodeType, List<Class<?>> classTypes) {
        var seedNodeAddrs =
            this.seedNodes.stream()
                          .filter(sn -> sn.nodeType.equals(nodeType))
                          .map(sn -> sn.nodeAddr.toSocketAddr())
                          .map(RpcStrategies::server)
                          .toList();

        var strategy  = RpcStrategies.roundRobin(seedNodeAddrs);
        var oldSender = Optional.ofNullable(senderGroup.get(nodeType));
        if (oldSender.isPresent()) {
            oldSender.get().changeStrategy(strategy, true);
        } else {
            var newSender =
                RpcClient.builder(eventloop)
                         .withMessageTypes(classTypes)
                         .withStrategy(strategy)
                         .withReconnectInterval(Duration.ofSeconds(10))
                         .withForcedShutdown()
                         .build();
            newSender.start().map($ -> this.senderGroup.put(nodeType, newSender));
        }
    }
}
