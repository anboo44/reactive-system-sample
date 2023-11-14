package com.uet.microservices.services.discovery;

import com.uet.microservices.lib.service.discovery.DiscoveryService;
import io.activej.eventloop.Eventloop;

import java.io.IOException;

public class MyDiscoveryService {
    public static void main(String[] args) throws IOException {
        var eventloop  = Eventloop.create();
        var dscService = new DiscoveryService(eventloop, 8080, 9000);

        dscService.startService();
        eventloop.run();
    }
}
