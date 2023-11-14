package com.uet.microservices.services.worker;

import com.uet.microservices.lib.model.NodeType;
import com.uet.microservices.lib.service.AbstractClusterService;
import com.uet.microservices.services.master.CalcRequest;
import com.uet.microservices.utils.MyUtils;
import io.activej.eventloop.Eventloop;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.rpc.server.RpcRequestHandler;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

public class WorkerService extends AbstractClusterService {
    protected WorkerService(
        Eventloop eventloop,
        InetSocketAddress discoveryAddr,
        String serviceName,
        NodeType nodeType,
        List<NodeType> seedTypes
    ) {
        super(eventloop, discoveryAddr, serviceName, nodeType, seedTypes);
    }

    public static WorkerService create(Eventloop eventloop, InetSocketAddress discoveryAddr) {
        return new WorkerService(
            eventloop,
            discoveryAddr,
            "worker-service-1",
            NodeType.WORKER,
            List.of(NodeType.WORKER)
        );
    }

    @Override
    protected Map<Class, RpcRequestHandler> makeRpcRequestHandlers() {
        var executor = Executors.newFixedThreadPool(2);

        RpcRequestHandler<CalcRequest, Integer> calcRequestHandler =
            req -> {
                if (req.to < 4) {
                    logger.info(">> This is a small task: {}. Calculate it right now", req);
                    return Promise.ofBlocking(
                        executor,
                        () -> MyUtils.sumOf(req.from, req.to)
                    );
                } else {
                    logger.info(">> This is a big task: {}. Split it and send them to cluster", req);
                    var sender = this.seedNodeManager.getSender(NodeType.WORKER);
                    var tasks  = WorkerTask.breakBig(req.from, req.to, 2);

                    var promises = tasks.stream().map(task -> sender.sendRequest(task).cast(Integer.class))
                                        .toList();
                    return Promises.toList(promises)
                                   .map(lst -> lst.stream().mapToInt(v -> v).sum());
                }
            };

        RpcRequestHandler<WorkerTask, Integer> workerTaskHandler =
            task -> {
                logger.info(">> Received a task: {}", task);
                return Promise.ofBlocking(
                    executor,
                    () -> MyUtils.sumOf(task.from, task.to)
                );
            };

        return Map.of(
            CalcRequest.class, calcRequestHandler,
            WorkerTask.class, workerTaskHandler
        );
    }

    @Override
    protected Map<NodeType, List<Class<?>>> getConnectionClassTypes() {
        return Map.of(
            NodeType.WORKER, List.of(CalcRequest.class, Integer.class, WorkerTask.class)
        );
    }

    public static void main(String[] args) throws IOException {
        var eventloop     = Eventloop.create();
        var discoveryAddr = new InetSocketAddress("localhost", 9000);
        var workerService = WorkerService.create(eventloop, discoveryAddr);

        workerService.startService();
        eventloop.run();
    }
}
