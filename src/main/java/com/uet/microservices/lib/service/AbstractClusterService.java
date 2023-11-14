package com.uet.microservices.lib.service;

import com.uet.microservices.lib.model.NodeAddr;
import com.uet.microservices.lib.model.NodeType;
import com.uet.microservices.lib.model.SeedNode;
import com.uet.microservices.lib.protocol.RpcBasicOperation;
import com.uet.microservices.lib.protocol.RpcCommon;
import com.uet.microservices.lib.protocol.RpcNodeEvent;
import com.uet.microservices.lib.protocol.RpcNodeInfo;
import com.uet.microservices.utils.MyUtils;
import io.activej.eventloop.Eventloop;
import io.activej.promise.Promise;
import io.activej.rpc.client.RpcClient;
import io.activej.rpc.client.sender.strategy.RpcStrategies;
import io.activej.rpc.server.RpcRequestHandler;
import io.activej.rpc.server.RpcServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.List;
import java.util.Map;

public abstract class AbstractClusterService {
    private final int      rpcDscPort;
    private final int      rpcMainPort;
    private final String   serviceName;
    private final NodeType nodeType;

    protected final   Eventloop         eventloop;
    protected final Logger            logger;
    private final   InetSocketAddress dscAddr;
    private final   List<NodeType>    seedTypes;
    protected final SeedNodeManager   seedNodeManager;
    private final   Map<NodeType, List<Class<?>>> classTypes;

    protected AbstractClusterService(
        Eventloop eventloop,
        InetSocketAddress discoveryAddr,
        String serviceName,
        NodeType nodeType,
        List<NodeType> seedTypes
    ) {
        this.rpcDscPort      = MyUtils.findAvailablePort();
        this.rpcMainPort     = MyUtils.findAvailablePort();
        this.eventloop       = eventloop;
        this.logger          = LoggerFactory.getLogger(this.getClass());
        this.dscAddr         = discoveryAddr;
        this.serviceName     = serviceName;
        this.nodeType        = nodeType;
        this.seedTypes       = seedTypes;
        this.classTypes      = getConnectionClassTypes();
        this.seedNodeManager = new SeedNodeManager(eventloop, this.classTypes);
    }

    private void startRpcDiscoveryServer() throws IOException {
        RpcRequestHandler<RpcNodeEvent, RpcBasicOperation> handleRpcNodeEvent =
            req -> {
                if (this.seedTypes.contains(req.nodeType)) {
                    var reqSN = new SeedNode(req.nodeName, req.nodeType, req.nodeAddr);

                    return switch (req.status) {
                        case UP -> {
                            var isAdded = this.seedNodeManager.addNewNode(reqSN);
                            if (isAdded) {
                                logger.info(">> Added new node to seedNodes: {}", reqSN);
                            }
                            yield Promise.of(RpcBasicOperation.ACCEPT);
                        }
                        case DOWN -> {
                            var isDeleted = this.seedNodeManager.removeNode(reqSN);
                            if (isDeleted) {
                                logger.info(">> Removed a node from seedNodes: {}", reqSN);
                            }
                            yield Promise.of(RpcBasicOperation.ACCEPT);
                        }
                        default -> Promise.of(RpcBasicOperation.ACCEPT);
                    };
                }

                return Promise.of(RpcBasicOperation.ACCEPT);
            };

        RpcServer.builder(eventloop)
                 .withMessageTypes(RpcCommon.rpcDscClassTypes)
                 .withHandler(RpcNodeEvent.class, handleRpcNodeEvent)
                 .withHandler(RpcBasicOperation.class, $ -> Promise.of(RpcBasicOperation.ACCEPT))
                 .withListenPort(rpcDscPort)
                 .build()
                 .listen();
    }

    private void startRpcMainServer() throws IOException {
        var handlers = makeRpcRequestHandlers();
        if (handlers.isEmpty()) {
            logger.info(">> No handlers for main RPC-SERVER");
            return;
        }

        var targetClassTypes = this.classTypes.get(nodeType);
        if (targetClassTypes == null) {
            throw new RuntimeException("Missing classTypes for nodeType: " + nodeType);
        }

        var rpcServer = RpcServer.builder(eventloop)
                                 .withMessageTypes(targetClassTypes)
                                 .withListenPort(rpcMainPort);
        handlers.forEach(rpcServer::withHandler);
        rpcServer.build().listen();
        this.seedNodeManager.addNewNode(
            new SeedNode(serviceName, nodeType, new NodeAddr("localhost", rpcMainPort))
        );
        this.logger.info(">> == Start main RPC-SERVER at port {} ==", rpcMainPort);
    }

    private void registerToDiscoveryService() {
        var rpcClient = RpcClient.builder(eventloop)
                                 .withMessageTypes(RpcCommon.rpcDscClassTypes)
                                 .withStrategy(RpcStrategies.server(dscAddr))
                                 .withForcedShutdown()
                                 .build();
        var msg = new RpcNodeInfo(
            this.serviceName,
            this.nodeType,
            this.seedTypes.isEmpty(),
            new NodeAddr("localhost", rpcDscPort),
            new NodeAddr("localhost", rpcMainPort)
        );

        rpcClient.start().whenComplete(() -> {
            rpcClient.sendRequest(msg)
                     .map(($, e) -> {
                         if (e == null) {
                             logger.info(">> Connect to discovery-service successfully");
                         } else {
                             logger.error(">> Cannot connect to discovery-service with error: {}", e.getMessage());
                             logger.info(">> Retry to connect to discovery-service in 15s");

                             eventloop.scheduleBackground(
                                 Instant.now().plusSeconds(15),
                                 this::registerToDiscoveryService
                             );
                         }

                         return true;
                     })
                     .whenComplete(rpcClient::stop);
        });
    }

    public void startService() throws IOException {
        startRpcDiscoveryServer();
        startRpcMainServer();
        registerToDiscoveryService();
    }

    protected abstract Map<Class, RpcRequestHandler> makeRpcRequestHandlers();

    /* Notice: List of classTypes must be same (values & order) between RpcClient and RpcServer */
    protected abstract Map<NodeType, List<Class<?>>> getConnectionClassTypes();
}
