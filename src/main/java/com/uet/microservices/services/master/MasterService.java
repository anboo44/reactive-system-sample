package com.uet.microservices.services.master;

import com.uet.microservices.lib.model.NodeType;
import com.uet.microservices.lib.protocol.RpcBasicOperation;
import com.uet.microservices.lib.service.AbstractClusterService;
import io.activej.eventloop.Eventloop;
import io.activej.http.AsyncServlet;
import io.activej.http.HttpResponse;
import io.activej.http.HttpServer;
import io.activej.http.RoutingServlet;
import io.activej.rpc.server.RpcRequestHandler;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

import static io.activej.http.HttpMethod.GET;

public class MasterService extends AbstractClusterService {
    private final int webPort;

    protected MasterService(
        Eventloop eventloop,
        InetSocketAddress discoveryAddr,
        String serviceName,
        NodeType nodeType,
        List<NodeType> seedTypes,
        int webPort
    ) {
        super(eventloop, discoveryAddr, serviceName, nodeType, seedTypes);
        this.webPort = webPort;
    }

    public static MasterService create(Eventloop eventloop, InetSocketAddress discoveryAddr) {
        return new MasterService(
            eventloop,
            discoveryAddr,
            "master-service",
            NodeType.MASTER_API,
            List.of(NodeType.WORKER, NodeType.LOG),
            9080
        );
    }

    @Override
    protected Map<Class, RpcRequestHandler> makeRpcRequestHandlers() {
        return Map.of();
    }

    @Override
    protected Map<NodeType, List<Class<?>>> getConnectionClassTypes() {
        return Map.of(
            NodeType.WORKER, List.of(CalcRequest.class, Integer.class),
            NodeType.LOG, List.of(String.class, RpcBasicOperation.class)
        );
    }

    @Override
    public void startService() throws IOException {
        super.startService();
        startWebServer();
    }

    private void startWebServer() throws IOException {
        AsyncServlet logHandler = req -> {
            var sender = this.seedNodeManager.getSender(NodeType.LOG);
            var msg    = req.getQueryParameter("msg");
            return sender.sendRequest(msg)
                         .map($ -> HttpResponse.ok200().withPlainText("OK").build());
        };

        AsyncServlet calcHandler = req -> {
            var range  = req.getQueryParameter("range");
            var values = range.split("-");
            var calcRequest = new CalcRequest(
                Integer.parseInt(values[0]),
                Integer.parseInt(values[1])
            );

            var sender = this.seedNodeManager.getSender(NodeType.WORKER);
            var start  = System.currentTimeMillis();
            return sender.sendRequest(calcRequest)
                         .cast(Integer.class)
                         .map(res -> {
                             var end      = System.currentTimeMillis();
                             var duration = end - start;
                             var msg      = String.format("Value = %d in %dms", res, duration);

                             return HttpResponse.ok200().withPlainText(msg).build();
                         });
        };

        var servlet = RoutingServlet.builder(eventloop)
                                    .with(GET, "/test-log", logHandler)
                                    .with(GET, "/test-worker", calcHandler)
                                    .build();
        var server = HttpServer.builder(eventloop, servlet)
                               .withListenPort(webPort)
                               .build();
        server.listen();
        logger.info(">> Web-server stated at port: {}", webPort);
    }

    public static void main(String[] args) throws IOException {
        var eventloop     = Eventloop.create();
        var discoveryAddr = new InetSocketAddress("localhost", 9000);
        var masterService = MasterService.create(eventloop, discoveryAddr);

        masterService.startService();
        eventloop.run();
    }
}
