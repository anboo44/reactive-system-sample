package com.uet.microservices.services;

import com.uet.microservices.lib.model.NodeType;

public class ServiceType {
    public static NodeType LOG    = new NodeType("LOG");
    public static NodeType WORKER = new NodeType("WORKER");
    public static NodeType MASTER = new NodeType("MASTER");
}
