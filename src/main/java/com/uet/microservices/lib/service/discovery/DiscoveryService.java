package com.uet.microservices.lib.service.discovery;

import com.uet.microservices.lib.protocol.RpcCommon;
import com.uet.microservices.lib.protocol.RpcNodeInfo;
import com.uet.microservices.utils.MyUtils;
import io.activej.eventloop.Eventloop;
import io.activej.http.HttpServer;
import io.activej.http.RoutingServlet;
import io.activej.http.StaticServlet;
import io.activej.http.loader.IStaticLoader;
import io.activej.rpc.server.RpcServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static io.activej.http.HttpMethod.GET;

public final class DiscoveryService {

    private final ServiceHandler serviceHandler;
    private final Eventloop      eventloop;
    private final int            webPort;
    private final int            rpcServerPort;
    private final Logger         logger;

    public DiscoveryService(Eventloop eventloop, int webPort, int rpcServerPort) {
        this.eventloop      = eventloop;
        this.webPort        = webPort;
        this.rpcServerPort  = rpcServerPort;
        this.serviceHandler = new ServiceHandler(eventloop);
        this.logger         = LoggerFactory.getLogger(DiscoveryService.class);
    }

    private void startWebServer() throws IOException {
        var staticLoader = IStaticLoader.ofClassPath(eventloop, MyUtils.blockingExecutor, "static/");
        var servlet = RoutingServlet.builder(eventloop)
                                    .with("/*", StaticServlet.builder(eventloop, staticLoader).withIndexHtml().build())
                                    .with(GET, "/", serviceHandler::ping)
                                    .build();
        var server = HttpServer.builder(eventloop, servlet)
                               .withListenPort(webPort)
                               .build();
        try {
            server.listen();
            logger.info(">> Web-server stated at port: {}", webPort);
        } catch (Exception e) {
            logger.error("Cannot start web-server at port: {} with error: {}", webPort, e.getMessage());
            throw e;
        }
    }

    private void startRpcServer() throws IOException {
        var server = RpcServer.builder(eventloop)
                              .withMessageTypes(RpcCommon.rpcDscClassTypes)
                              .withHandler(RpcNodeInfo.class, serviceHandler::acceptNode)
                              .withListenPort(rpcServerPort)
                              .build();
        try {
            server.listen();
            logger.info(">> Rpc-server stated at port: {}", rpcServerPort);
        } catch (Exception e) {
            logger.error("Cannot start rpc-server at port: {} with error: {}", rpcServerPort, e.getMessage());
            throw e;
        }
    }

    public void startService() throws IOException {
        startWebServer();
        startRpcServer();
    }
}
