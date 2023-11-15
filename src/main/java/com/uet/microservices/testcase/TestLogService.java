package com.uet.microservices.testcase;

import com.uet.microservices.lib.protocol.RpcBasicOperation;
import io.activej.eventloop.Eventloop;
import io.activej.rpc.client.RpcClient;
import io.activej.rpc.client.sender.strategy.RpcStrategies;

import java.net.InetSocketAddress;

public class TestLogService {
    public static void main(String[] args) {
        var eventloop = Eventloop.create();

        var client = RpcClient.builder(eventloop)
                              .withMessageTypes(String.class, RpcBasicOperation.class)
                              .withStrategy(RpcStrategies.server(new InetSocketAddress(56483)))
                              .build();
        eventloop.submit(() -> {
            client.start().whenComplete(() -> {
                client.sendRequest("Hello World !");
                client.sendRequest("My name is Hung_pt from UET.");
                client.sendRequest("I'm a software engineer. I'm working at FlintersVN");
            });
        });

        eventloop.keepAlive(true);
        eventloop.run();
    }
}
