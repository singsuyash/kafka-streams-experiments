package com.poc.outputorder;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
public class OutputOrderKeyDeserializer implements Deserializer<OutputOrderKey> {
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public OutputOrderKey deserialize(String topic, byte[] data) {
        try {
            return mapper.readValue(data, OutputOrderKey.class);
        } catch (IOException e) {
            e.printStackTrace();
            throw new SerializationException(e);
        }
    }
}
