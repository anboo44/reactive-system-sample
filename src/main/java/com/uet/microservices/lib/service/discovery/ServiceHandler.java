package com.uet.microservices.lib.service.discovery;

import com.uet.microservices.lib.protocol.RpcBasicOperation;
import com.uet.microservices.lib.protocol.RpcCommon;
import com.uet.microservices.lib.protocol.RpcNodeEvent;
import com.uet.microservices.lib.protocol.RpcNodeInfo;
import io.activej.common.function.RunnableEx;
import io.activej.eventloop.Eventloop;
import io.activej.http.HttpRequest;
import io.activej.http.HttpResponse;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.rpc.client.RpcClient;
import io.activej.rpc.client.sender.strategy.RpcStrategies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

class ServiceHandler {

    private final Eventloop         eventloop;
    private final List<RpcNodeInfo> allNodes;
    private final Logger            logger;

    ServiceHandler(Eventloop eventloop) {
        this.eventloop = eventloop;
        this.allNodes  = new ArrayList<>();
        this.logger    = LoggerFactory.getLogger(ServiceHandler.class);

        doRpcHealthCheck();
    }

    // ===============/ WEB-ACTION /===============//
    public Promise<HttpResponse> ping(HttpRequest req) {
        return Promise.of(
            HttpResponse.ok200().withPlainText("[Health] discovery-service: OK").build()
        );
    }

    // ===============/ RPC-ACTION /===============//
    public Promise<RpcBasicOperation> acceptNode(RpcNodeInfo nodeInfo) {
        var oldSeedNodes = new ArrayList<>(this.allNodes);
        notifyToNodes(oldSeedNodes, nodeInfo, true);  // -> Notify all other nodes in cluster about this new node
        notifyOtherNodesToNewNode(oldSeedNodes, nodeInfo); // -> Notify this new node about all other nodes in cluster
        this.allNodes.add(nodeInfo);
        return Promise.of(RpcBasicOperation.ACCEPT)
                      .whenComplete(() -> logger.info(">> Accept new node: {}", nodeInfo.name));
    }

    private void notifyOtherNodesToNewNode(List<RpcNodeInfo> seedNodes, RpcNodeInfo nodeInfo) {
        if (seedNodes.isEmpty()) return;

        var newNodeAddr = nodeInfo.dscServerAddr.toSocketAddr();
        var strategy    = RpcStrategies.roundRobin(RpcStrategies.server(newNodeAddr));
        var rpcClient = RpcClient.builder(eventloop)
                                 .withMessageTypes(RpcCommon.rpcDscClassTypes)
                                 .withStrategy(strategy)
                                 .withForcedShutdown()
                                 .build();

        rpcClient.start().whenComplete(() -> {
            var promises = seedNodes.stream()
                                    .map(sn -> new RpcNodeEvent(sn, true))
                                    .map(rpcClient::sendRequest)
                                    .toList();
            Promises.all(promises).whenComplete(rpcClient::stop);
        });
    }

    private void notifyToNodes(List<RpcNodeInfo> seedNodes, RpcNodeInfo nodeInfo, boolean isUp) {
        var notifiedAddr =
            getNotifiedNodes(seedNodes)
                .stream()
                .filter(node -> !node.isAlone)
                .map(node -> node.dscServerAddr.toSocketAddr())
                .toList();

        // -> Only notify to nodes that are not alone
        if (!notifiedAddr.isEmpty()) {
            var strategy = RpcStrategies.roundRobin(RpcStrategies.servers(notifiedAddr));
            var event    = new RpcNodeEvent(nodeInfo, isUp);
            var rpcClient = RpcClient.builder(eventloop)
                                     .withMessageTypes(RpcCommon.rpcDscClassTypes)
                                     .withStrategy(strategy)
                                     .withForcedShutdown()
                                     .build();
            rpcClient.start().whenComplete(() -> {
                broadcastMessage(rpcClient, event, rpcClient::stop);
            });
        }
    }

    private List<RpcNodeInfo> getNotifiedNodes(List<RpcNodeInfo> allNodes) {
        return allNodes.stream().filter(node -> !node.isAlone).toList();
    }

    private <T> Promise<Void> broadcastMessage(RpcClient client, T message, RunnableEx callback) {
        var promises = client.getConnectsStatsPerAddress() // -> Get all connected nodes
                             .keySet().stream()            // -> Get all address of connected nodes
                             .map(
                                 node -> client.sendRequest(message)
                                               .map(($, e) -> {
                                                   if (e != null) {
                                                       var msg = String.format("Cannot broadcast msg to node: %s", node.getHostName());
                                                       this.logger.error(">> {} with error: {}", msg, e.getMessage());
                                                   }
                                                   return true;
                                               })
                             )
                             .toList();

        return Promises.all(promises).whenComplete(callback);
    }

    private void doRpcHealthCheck() {
        this.eventloop.scheduleBackground(
            Instant.now().plusSeconds(60), // -> Run every 60 seconds
            () -> {
                if (this.allNodes.isEmpty()) doRpcHealthCheck();
                else {
                    var nodeAddrs = this.allNodes.stream()
                                                 .map(node -> node.dscServerAddr.toSocketAddr())
                                                 .toList();
                    var strategy = RpcStrategies.roundRobin(RpcStrategies.servers(nodeAddrs));
                    var rpcClient = RpcClient.builder(eventloop)
                                             .withMessageTypes(RpcCommon.rpcDscClassTypes)
                                             .withStrategy(strategy)
                                             .withForcedShutdown()
                                             .build();

                    rpcClient.start().whenComplete(() -> {
                        var activeAddrs = rpcClient.getConnectsStatsPerAddress()
                                                   .keySet().stream()
                                                   .toList();
                        var deadNodes = this.allNodes.stream().filter(node -> {
                            var addr = node.dscServerAddr.toSocketAddr();
                            return !activeAddrs.contains(addr);
                        }).toList();

                        if (!deadNodes.isEmpty()) {
                            var deadAddrs = deadNodes.stream()
                                                     .map(node -> node.dscServerAddr.toSocketAddr())
                                                     .toList();
                            this.allNodes.removeIf(node -> deadAddrs.contains(node.dscServerAddr.toSocketAddr()));
                            logger.info(">> Un-track dead nodes: {}", deadAddrs);

                            // -> Notify to all nodes
                            deadNodes.forEach(node -> notifyToNodes(this.allNodes, node, false));
                        } else {
                            logger.info(">> All nodes are live");
                        }

                        rpcClient.stop()
                                 .whenComplete(this::doRpcHealthCheck);
                    });
                }
            }
        );
    }
}
