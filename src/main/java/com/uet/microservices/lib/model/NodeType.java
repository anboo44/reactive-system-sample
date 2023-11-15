package com.uet.microservices.lib.model;

import io.activej.serializer.annotations.SerializeRecord;

@SerializeRecord
public record NodeType(String value){}
