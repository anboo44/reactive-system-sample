package com.uet.microservices.services.worker;

import com.uet.microservices.services.master.CalcRequest;
import com.uet.microservices.utils.MyUtils;
import io.activej.eventloop.Eventloop;
import io.activej.rpc.client.RpcClient;
import io.activej.rpc.client.sender.strategy.RpcStrategies;

import java.net.InetSocketAddress;

public class TestWorker {
    public static void main(String[] args) {
        var eventloop = Eventloop.create();
        var client = RpcClient.builder(eventloop)
                              .withMessageTypes(CalcRequest.class, Integer.class)
                              .withStrategy(RpcStrategies.server(new InetSocketAddress(59412)))
                              .build();
        eventloop.submit(() -> {
            client.start().whenComplete(() -> {
                var start = System.currentTimeMillis();
                client.sendRequest(new CalcRequest(0, 3))
                      .cast(Integer.class)
                      .whenResult(res -> {
                          var end      = System.currentTimeMillis();
                          var duration = end - start;
                          System.out.println(">> [Cluster] Total of 0 -> 3 = " + res + " in " + duration + "ms");
                      });

                client.sendRequest(new CalcRequest(1, 40))
                      .cast(Integer.class)
                      .whenResult(res -> {
                          var end      = System.currentTimeMillis();
                          var duration = end - start;
                          System.out.println(">> [Cluster] Total of 1 -> 40 = " + res + " in " + duration + "ms");
                      });
            });
        });

        // -> Test without RPC
        new Thread(() -> {
            var start    = System.currentTimeMillis();
            var res      = MyUtils.sumOf(1, 20);
            var end      = System.currentTimeMillis();
            var duration = end - start;
            System.out.println(">> [Without cluster]Total of 1 -> 20 = " + res + " in " + duration + "ms");
        });

        eventloop.keepAlive(true);
        eventloop.run();


    }
}
