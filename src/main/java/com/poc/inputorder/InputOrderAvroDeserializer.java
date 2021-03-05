package com.poc.inputorder;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

public class InputOrderAvroDeserializer implements Deserializer {
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public Object deserialize(String topic, byte[] data) {
        try {
            return mapper.readValue(data, InputOrderAvro.class);
        } catch (IOException e) {
            e.printStackTrace();
            throw new SerializationException(e);
        }
    }
}
