package com.poc.inputorder;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class InputOrderTest {

    @Test
    void shouldBeAbleToSerializeAndDeserialize() {
        InputOrder order = new InputOrder();
        order.key = "key";
        order.value = "value";
        
        InputOrderSerializer serializer = new InputOrderSerializer();
        InputOrderDeserializer deserializer = new InputOrderDeserializer();

        byte[] bytes = serializer.serialize("foo", order);
        InputOrder orderBackAgain = (InputOrder)deserializer.deserialize("foo", bytes);

        assertEquals(order.key, orderBackAgain.key);
        assertEquals(order.value, orderBackAgain.value);
    }
}