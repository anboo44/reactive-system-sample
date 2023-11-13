package com.uet.microservices.services.worker;

import com.uet.microservices.models.NodeRegister;
import com.uet.microservices.models.Notify;
import com.uet.microservices.services.discovery.DiscoveryService;
import io.activej.eventloop.Eventloop;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.rpc.client.RpcClient;
import io.activej.rpc.client.sender.strategy.RpcStrategies;
import io.activej.rpc.server.RpcServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class LeadedWorkerService {
    public static final  int       DISCOVERY_PORT  = 9310;
    public static final  int       RPC_SERVER_PORT = 9300;
    private static final Eventloop eventloop       = Eventloop.create();

    private static final RpcClient rpcClusterClient =
        RpcClient.builder(eventloop)
                 .withMessageTypes(String.class, Boolean.class)
                 .withStrategy(RpcStrategies.server(new InetSocketAddress(RPC_SERVER_PORT)))
                 .withReconnectInterval(Duration.ofSeconds(5))
                 .build();

    private static void register() {
        // -> Register to discovery service
        var registerMsg = new NodeRegister(RPC_SERVER_PORT, DISCOVERY_PORT, "working-leader", List.of("worker"));
        var rpcClient = RpcClient.builder(eventloop)
                                 .withMessageTypes(NodeRegister.class, Boolean.class)
                                 .withStrategy(RpcStrategies.server(new InetSocketAddress(DiscoveryService.RPC_SERVER_PORT)))
                                 .withForcedShutdown()
                                 .build();
        rpcClient.start()
                 .whenResult($ -> System.out.println("Start to register to discovery service"))
                 .whenComplete(() -> {
                     rpcClient.sendRequest(registerMsg)
                              .whenResult($ -> rpcClient.stop().whenComplete(() -> System.out.println("Register to discovery service successfully")))
                              .whenException(e -> {
                                  rpcClient.stop()
                                           .whenComplete(() -> {
                                               System.out.println("Cannot register to discovery service => Retry in 5s");
                                               eventloop.scheduleBackground(
                                                   Instant.now().plusSeconds(5),
                                                   LeadedWorkerService::register
                                               );
                                           });
                              });
                 });
    }

    public static void main(String[] args) throws IOException {
        // -> Main RPC server
        RpcServer.builder(eventloop)
                 .withMessageTypes(String.class, Boolean.class)
                 .withHandler(String.class, req -> {
                     System.out.println(">> Received request: [" + req + "] <<");
                     var values = req.split("-");
                     var from   = Integer.parseInt(values[0]);
                     var to     = Integer.parseInt(values[1]);
                     var diff   = to - from;

                     if (diff < 3 || (values.length == 3 && values[2].equals("no"))) {
                         return Promise.ofBlocking(Utils.executor, () -> "" + Utils.sumOf(from, to));
                     } else {
                         System.out.println(">> This is big request. Splitting it <<");
                         var list  = new ArrayList<String>();
                         var start = from;

                         while (true) {
                             var end = start + 2;

                             if (end <= to) list.add(start + "-" + end);
                             else list.add(start + "-" + to);

                             if (end >= to) break;
                             start = end + 1;
                         }

                         return Promises.toList(
                             list.stream().map(
                                 msg -> rpcClusterClient.sendRequest(msg)
                                                        .cast(String.class)
                                                        .map(v -> Integer.parseInt(v))
                             ).toList()
                         ).map(
                             lst -> lst.stream().mapToInt(Integer::intValue).sum()
                         ).map(v -> "" + v);
                     }
                 })
                 .withListenPort(RPC_SERVER_PORT)
                 .build()
                 .listen();

        // -> Discovery RPC server
        RpcServer.builder(eventloop)
                 .withMessageTypes(Notify.class, Boolean.class)
                 .withHandler(Notify.class, req -> {
                     var oddAddrs = new HashSet<>(rpcClusterClient.getConnectsStatsPerAddress().keySet());
                     if (req.isUp) {
                         oddAddrs.add(new InetSocketAddress(req.port));
                         var strategy = RpcStrategies.roundRobin(
                             oddAddrs.stream().map(RpcStrategies::server).toList()
                         );
                         rpcClusterClient.changeStrategy(strategy, true);
                     }

                     if (!req.isUp) {
                         oddAddrs.removeIf(addr -> addr.getPort() == req.port);
                         var strategy = RpcStrategies.roundRobin(
                             oddAddrs.stream().map(RpcStrategies::server).toList()
                         );
                         rpcClusterClient.changeStrategy(strategy, true);
                     }

                     return Promise.of(true);
                 })
                 .withHandler(Boolean.class, req -> Promise.of(true))
                 .withListenPort(DISCOVERY_PORT)
                 .build()
                 .listen();

        register();
        eventloop.submit(rpcClusterClient::start);

        // -> Start eventloop
        eventloop.keepAlive(true);
        eventloop.run();
    }
}
