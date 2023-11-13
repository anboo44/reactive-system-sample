package com.uet.microservices.services.discovery;

import com.uet.microservices.models.NodeRegister;
import com.uet.microservices.models.Notify;
import io.activej.eventloop.Eventloop;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.rpc.client.RpcClient;
import io.activej.rpc.client.sender.strategy.RpcStrategies;
import io.activej.rpc.server.RpcServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class DiscoveryService {
    public static final  int       RPC_SERVER_PORT = 9000;
    private static final Eventloop eventloop       = Eventloop.create();

    private static List<NodeRegister> seedNodes = new ArrayList<>();

    private static void notify(NodeRegister nodeRegister, boolean isUp) {
        // -> Send notify to all exited nodes base on seedTypes
        var nodeAddrs = seedNodes.stream()
                                 .filter(node -> node.seedTypes.contains(nodeRegister.nodeType))
                                 .map(node -> node.discoveryPort)
                                 .filter(port -> isUp || port != nodeRegister.discoveryPort)
                                 .map(InetSocketAddress::new)
                                 .toList();
        if (!nodeAddrs.isEmpty()) {
            var strategy     = RpcStrategies.roundRobin(RpcStrategies.servers(nodeAddrs));
            var broadcastMsg = new Notify(nodeRegister.servicePort, nodeRegister.nodeType, isUp);
            var rpcClient = RpcClient.builder(eventloop)
                                     .withMessageTypes(Notify.class, Boolean.class)
                                     .withStrategy(strategy)
                                     .withForcedShutdown()
                                     .build();
            rpcClient.start().whenComplete(() -> {
                Promises.all(
                    nodeAddrs.stream().map(
                        node -> rpcClient.sendRequest(broadcastMsg)
                                         .then(Promise::of, e -> {
                                             System.out.println("Cannot notify to node " + node.getPort());
                                             return Promise.of(false);
                                         })
                    ).toList()
                ).whenComplete(rpcClient::stop);
            });
        }

        // -> When new node is up and require seedNodes, find current seedNodes to send back
        if (isUp && !nodeRegister.seedTypes.isEmpty()) {
            var notifies = seedNodes.stream()
                                    .filter(node -> nodeRegister.seedTypes.contains(node.nodeType))
                                    .map(node -> new Notify(node.servicePort, node.nodeType, true))
                                    .toList();
            if (!notifies.isEmpty()) {
                var strategy = RpcStrategies.server(new InetSocketAddress(nodeRegister.discoveryPort));
                var rpcClient = RpcClient.builder(eventloop)
                                         .withMessageTypes(Notify.class, Boolean.class)
                                         .withStrategy(strategy)
                                         .withForcedShutdown()
                                         .build();
                rpcClient.start()
                         .whenResult(() -> {
                             Promises.all(
                                 notifies.stream()
                                         .map(rpcClient::sendRequest)
                                         .toList()
                             ).whenComplete(rpcClient::stop);
                         });
            }
        }
    }

    private static Promise<Boolean> registerNewNode(NodeRegister nodeRegister) {
        notify(nodeRegister, true);
        seedNodes.add(nodeRegister);
        return Promise.of(true);
    }

    private static void checkNodeAlive() {
        eventloop.scheduleBackground(
            Instant.now().plusSeconds(10),
            () -> {
                if (seedNodes.isEmpty()) checkNodeAlive();
                else {
                    var nodeAddrs = seedNodes.stream()
                                             .map(node -> node.discoveryPort)
                                             .map(InetSocketAddress::new)
                                             .toList();
                    var strategy = RpcStrategies.roundRobin(RpcStrategies.servers(nodeAddrs));
                    var rpcClient = RpcClient.builder(eventloop)
                                             .withMessageTypes(Notify.class, Boolean.class)
                                             .withStrategy(strategy)
                                             .withForcedShutdown()
                                             .build();
                    rpcClient.start()
                             .whenComplete(() -> {
                                 var activePorts = rpcClient.getConnectsStatsPerAddress()
                                                            .keySet().stream()
                                                            .map(InetSocketAddress::getPort)
                                                            .toList();
                                 var deadPorts = seedNodes.stream()
                                                          .map(v -> v.discoveryPort)
                                                          .filter(v -> !activePorts.contains(v))
                                                          .toList();
                                 var alivePorts = seedNodes.stream()
                                                           .map(v -> v.discoveryPort)
                                                           .filter(activePorts::contains)
                                                           .toList();
                                 if (!deadPorts.isEmpty()) {
                                     System.out.println("Dead nodes: " + deadPorts);

                                     var msgList = seedNodes.stream().filter(v -> deadPorts.contains(v.discoveryPort))
                                                         .map(v -> new Notify(v.servicePort, v.nodeType, false))
                                                         .toList();
                                     Promises.all(
                                         msgList.stream().map(rpcClient::sendRequest).toList()
                                     ).whenComplete(rpcClient::stop);
                                     seedNodes.removeIf(node -> deadPorts.contains(node.discoveryPort));
                                 } else {
                                     rpcClient.stop();
                                 }

                                 System.out.println("Nodes are alive: " + alivePorts);
                                 checkNodeAlive();
                             });
                }
            }
        );
    }

    public static void main(String[] args) throws IOException {
        RpcServer.builder(eventloop)
                 .withMessageTypes(NodeRegister.class, Boolean.class)
                 .withHandler(NodeRegister.class, DiscoveryService::registerNewNode)
                 .withListenPort(RPC_SERVER_PORT)
                 .build()
                 .listen();

        checkNodeAlive();
        eventloop.keepAlive(true);
        eventloop.run();
    }
}
