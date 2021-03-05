package com.poc.inputorder;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class InputOrderTest {

    @Test
    void shouldBeAbleToSerializeAndDeserializeKey() {
        InputOrderKey key = new InputOrderKey();
        InputOrderKeySerializer serializer = new InputOrderKeySerializer();
        InputOrderKeyDeserializer deserializer = new InputOrderKeyDeserializer();

        byte[] bytes = serializer.serialize("foo", key);
        assertEquals(key.id, ((InputOrderKey)deserializer.deserialize("foo", bytes)).id);
    }

    @Test
    void shouldBeAbleToSerializeAndDeserializeOrder() {
        InputOrder order = new InputOrder();
        InputOrderSerializer serializer = new InputOrderSerializer();
        InputOrderDeserializer deserializer = new InputOrderDeserializer();

        byte[] bytes = serializer.serialize("foo", order);
        assertEquals(order.detail.length, ((InputOrder)deserializer.deserialize("foo", bytes)).detail.length);
    }
}