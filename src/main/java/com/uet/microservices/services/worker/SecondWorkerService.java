package com.uet.microservices.services.worker;

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

public class SecondWorkerService {
    public final  int       discoveryPort;
    public final  int       rpcServerPort;
    private final Eventloop eventloop;

    public SecondWorkerService() {
        this.discoveryPort = Utils.findAvailablePort();
        this.rpcServerPort = Utils.findAvailablePort();
        this.eventloop     = Eventloop.create();
    }

    private void register() {
        // -> Register to discovery service
        var registerMsg = new NodeRegister(rpcServerPort, discoveryPort, "worker", List.of());
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
                                                   this::register
                                               );
                                           });
                              });
                 });
    }

    public void laugh() throws IOException {
        // -> Main RPC server
        RpcServer.builder(eventloop)
                 .withMessageTypes(String.class, Boolean.class)
                 .withHandler(String.class, req -> {
                     System.out.println(">> Received request: [" + req + "] <<");
                     var values = req.split("-");
                     var from   = Integer.parseInt(values[0]);
                     var to     = Integer.parseInt(values[1]);

                     return Promise.ofBlocking(Utils.executor, () -> "" + Utils.sumOf(from, to));
                 })
                 .withListenPort(rpcServerPort)
                 .build()
                 .listen();

        // -> Discovery RPC server
        RpcServer.builder(eventloop)
                 .withMessageTypes(Notify.class, Boolean.class)
                 .withHandler(Notify.class, req -> Promise.of(true))
                 .withHandler(Boolean.class, req -> Promise.of(true))
                 .withListenPort(discoveryPort)
                 .build()
                 .listen();

        register();

        // -> Start eventloop
        eventloop.keepAlive(true);
        eventloop.run();
    }

    public static void main(String[] args) throws IOException {
        var ins = new SecondWorkerService();
        ins.laugh();
    }
}
