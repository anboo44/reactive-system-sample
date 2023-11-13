package com.uet.microservices.services.master;

import com.uet.microservices.models.NodeRegister;
import com.uet.microservices.models.Notify;
import com.uet.microservices.services.discovery.DiscoveryService;
import io.activej.eventloop.Eventloop;
import io.activej.http.HttpMethod;
import io.activej.http.HttpResponse;
import io.activej.http.HttpServer;
import io.activej.http.RoutingServlet;
import io.activej.promise.Promise;
import io.activej.rpc.client.RpcClient;
import io.activej.rpc.client.sender.strategy.RpcStrategies;
import io.activej.rpc.server.RpcServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

public class MasterService {
    public static final  int       DISCOVERY_PORT  = 9210;
    public static final  int       RPC_SERVER_PORT = 9200;
    private static final Eventloop eventloop       = Eventloop.create();

    private static final Map<String, RpcClient> rpcClusterClients = new HashMap<>();

    private static void register() {
        // -> Register to discovery service
        var registerMsg = new NodeRegister(RPC_SERVER_PORT, DISCOVERY_PORT, "master", List.of("log", "working-leader"));
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
                                                   MasterService::register
                                               );
                                           });
                              });
                 });
    }

    public static void main(String[] args) throws IOException {
        // -> Main RPC server
        RpcServer.builder(eventloop)
                 .withMessageTypes(String.class, Boolean.class)
                 .withHandler(String.class, req -> Promise.of("Hello1 " + req))
                 .withListenPort(RPC_SERVER_PORT)
                 .build()
                 .listen();

        // -> Discovery RPC server
        RpcServer.builder(eventloop)
                 .withMessageTypes(Notify.class, Boolean.class)
                 .withHandler(Notify.class, req -> {
                     var targetClient = rpcClusterClients.get(req.nodeType);
                     if (targetClient == null && req.isUp) {
                         var newRpcClient = RpcClient.builder(eventloop)
                                                     .withMessageTypes(String.class)
                                                     .withStrategy(RpcStrategies.server(new InetSocketAddress(req.port)))
                                                     .withReconnectInterval(Duration.ofSeconds(5))
                                                     .build();
                         rpcClusterClients.put(req.nodeType, newRpcClient);
                         newRpcClient.start();
                     }

                     if (targetClient != null && req.isUp) {
                         var oddAddrs = new HashSet<>(targetClient.getConnectsStatsPerAddress().keySet());
                         oddAddrs.add(new InetSocketAddress(req.port));
                         var strategy = RpcStrategies.roundRobin(
                             oddAddrs.stream().map(RpcStrategies::server).toList()
                         );
                         targetClient.changeStrategy(strategy, true);
                     }

                     if (targetClient != null && !req.isUp) {
                         var oddAddrs = new HashSet<>(targetClient.getConnectsStatsPerAddress().keySet());
                         oddAddrs.removeIf(addr -> addr.getPort() == req.port);
                         if (oddAddrs.isEmpty()) {
                             rpcClusterClients.remove(req.nodeType);
                             targetClient.stop();
                         } else {
                             var strategy = RpcStrategies.roundRobin(
                                 oddAddrs.stream().map(RpcStrategies::server).toList()
                             );
                             targetClient.changeStrategy(strategy, true);
                         }
                     }

                     return Promise.of(true);
                 })
                 .withHandler(Boolean.class, req -> Promise.of(true))
                 .withListenPort(DISCOVERY_PORT)
                 .build()
                 .listen();

        register();

        // -> Start Http server
        var servlet = RoutingServlet.builder(eventloop)
                                    .with(HttpMethod.GET, "/", req -> {
                                        Optional.ofNullable(rpcClusterClients.get("log"))
                                                .map(client -> client.sendRequest("From master-service: Hello log-service"));

                                        return Promise.of(HttpResponse.ok200().withPlainText("OK").build());
                                    })
                                    .with(HttpMethod.GET, "/calc", req -> {
                                        var start      = System.currentTimeMillis();
                                        var range      = req.getQueryParameter("range");
                                        var useCluster = req.getQueryParameter("useCluster");

                                        Optional.ofNullable(rpcClusterClients.get("log"))
                                                .map(client -> client.sendRequest("From master-service: Calc request " + range));

                                        var rpcCLient = rpcClusterClients.get("working-leader");
                                        if (rpcCLient == null) {
                                            return Promise.of(HttpResponse.ok200().withPlainText("No worker node available").build());
                                        } else {
                                            return rpcCLient.sendRequest(range + "-" + useCluster)
                                                            .cast(String.class)
                                                            .then(res -> {
                                                                var end      = System.currentTimeMillis();
                                                                var duration = end - start;
                                                                return Promise.of(
                                                                    HttpResponse.ok200()
                                                                                .withPlainText("Value = " + res + " in " + duration + "ms")
                                                                                .build()
                                                                );
                                                            }, e -> {
                                                                return Promise.of(
                                                                    HttpResponse.ok200().withPlainText("Happened error" + e.getMessage()).build()
                                                                );
                                                            });
                                        }
                                    })
                                    .build();
        var server = HttpServer.builder(eventloop, servlet)
                               .withListenPort(3000)
                               .build();
        server.listen();

        // -> Start eventloop
        eventloop.keepAlive(true);
        eventloop.run();
    }
}
