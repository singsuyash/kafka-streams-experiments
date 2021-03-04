package com.poc.inputorder;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class InputOrderTest {

    @Test
    void shouldBeAbleToSerializeAndDeserialize() {
        InputOrder order = new InputOrder();

        InputOrderSerializer serializer = new InputOrderSerializer();
        InputOrderDeserializer deserializer = new InputOrderDeserializer();

        byte[] bytes = serializer.serialize("foo", order);
        System.out.printf("bytes length" + bytes.length);
        InputOrder orderBackAgain = (InputOrder)deserializer.deserialize("foo", bytes);

        assertEquals(order.key, orderBackAgain.key);
        assertEquals(order.value.length, orderBackAgain.value.length);

    }
}