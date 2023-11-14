package com.uet.microservices.services.log;

import com.uet.microservices.lib.service.AbstractClusterService;
import com.uet.microservices.lib.protocol.RpcBasicOperation;
import com.uet.microservices.lib.model.NodeType;
import io.activej.eventloop.Eventloop;
import io.activej.promise.Promise;
import io.activej.rpc.server.RpcRequestHandler;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

public class LogService extends AbstractClusterService {
    protected LogService(
        Eventloop eventloop,
        InetSocketAddress discoveryAddr,
        String serviceName,
        NodeType nodeType,
        List<NodeType> seedTypes
    ) {
        super(eventloop, discoveryAddr, serviceName, nodeType, seedTypes);
    }

    public static LogService create(Eventloop eventloop, InetSocketAddress discoveryAddr) {
        return new LogService(
            eventloop,
            discoveryAddr,
            "log-service",
            NodeType.LOG,
            List.of()
        );
    }

    @Override
    protected Map<Class, RpcRequestHandler> makeRpcRequestHandlers() {
        RpcRequestHandler<String, RpcBasicOperation> messageHandler =
            msg -> {
                logger.info(">> Received message: {}", msg);
                var task   = LogTask.create(msg);
                var sender = this.seedNodeManager.getSender(NodeType.LOG);
                return sender.sendRequest(task)
                             .map($ -> RpcBasicOperation.ACCEPT);
            };

        RpcRequestHandler<LogTask, RpcBasicOperation> taskHandler =
            task -> {
                switch (task.taskType) {
                    case INFO -> logger.info(">> Received a task: {}", task.message);
                    case WARN -> logger.warn(">> Received a task: {}", task.message);
                    case ERROR -> logger.error(">> Received a task: {}", task.message);
                }

                return Promise.of(RpcBasicOperation.ACCEPT);
            };

        return Map.of(
            String.class, messageHandler,
            LogTask.class, taskHandler
        );
    }

    @Override
    protected Map<NodeType, List<Class<?>>> getConnectionClassTypes() {
        return Map.of(
            NodeType.LOG, List.of(String.class, RpcBasicOperation.class, LogTask.class)
        );
    }

    public static void main(String[] args) throws IOException {
        var eventloop     = Eventloop.create();
        var discoveryAddr = new InetSocketAddress("localhost", 9000);
        var logService = LogService.create(eventloop, discoveryAddr);

        logService.startService();
        eventloop.run();
    }
}
