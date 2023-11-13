package com.uet.microservices.services.log;

import com.uet.microservices.models.NodeRegister;
import com.uet.microservices.models.Notify;
import com.uet.microservices.services.discovery.DiscoveryService;
import io.activej.eventloop.Eventloop;
import io.activej.promise.Promise;
import io.activej.rpc.client.RpcClient;
import io.activej.rpc.client.sender.strategy.RpcStrategies;
import io.activej.rpc.server.RpcServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.List;

public class LogService {
    public static final  int       DISCOVERY_PORT  = 9110;
    public static final  int       RPC_SERVER_PORT = 9100;
    private static final Eventloop eventloop       = Eventloop.create();

    private static void register() {
        // -> Register to discovery service
        var registerMsg = new NodeRegister(RPC_SERVER_PORT, DISCOVERY_PORT, "log", List.of());
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
                                                   LogService::register
                                               );
                                           });
                              });
                 });
    }

    public static void main(String[] args) throws IOException {
        // -> Main RPC server
        RpcServer.builder(eventloop)
                 .withMessageTypes(String.class)
                 .withHandler(String.class, req -> {
                     System.out.println(req);
                     return Promise.of("From log-service: Hello master-service");
                 })
                 .withListenPort(RPC_SERVER_PORT)
                 .build()
                 .listen();

        // -> Discovery RPC server
        RpcServer.builder(eventloop)
                 .withMessageTypes(Notify.class, Boolean.class)
                 .withHandler(Boolean.class, req -> Promise.of(true))
                 .withListenPort(DISCOVERY_PORT)
                 .build()
                 .listen();

        register();
        // -> Start eventloop
        eventloop.keepAlive(true);
        eventloop.run();
    }
}
