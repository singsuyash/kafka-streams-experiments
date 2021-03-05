package com.poc.inputorder;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class InputOrderAvroTest {

    @Test
    void shouldBeAbleToSerializeAndDeserialize() {
        InputOrderAvro order = new InputOrderAvro("hello", IntStream.range(1,250).boxed().collect(Collectors.toList()));
        SpecificAvroSerializer<InputOrderAvro> serializer = new SpecificAvroSerializer<>();
        byte[] bytes = serializer.serialize("foo", order);
        int x = 10;
    }
}