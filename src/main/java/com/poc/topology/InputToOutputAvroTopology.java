package com.poc.topology;

import com.poc.AppConfig;
import com.poc.inputorder.InputOrderConstants;
import com.poc.inputorder.avro.InputOrderAvro;
import com.poc.inputorder.avro.InputOrderAvroKey;
import com.poc.outputorder.OutputOrderConstants;
import com.poc.outputorder.avro.OutputOrderAvro;
import com.poc.outputorder.avro.OutputOrderAvroKey;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class InputToOutputAvroTopology {
    private final StreamsBuilder builder = new StreamsBuilder();
    private KafkaStreams stream;

    public Properties getConfig() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, AppConfig.APPLICATION_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfig.BOOTSTRAP_SERVER);
        return props;
    }

    private Map<String, Object> getConfigMap() {
        Map<String, Object> props = new HashMap<>();
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, AppConfig.SCHEMA_REGISTRY_URL);
        return props;
    }

    public Topology getTopology() {
        SpecificAvroSerde<InputOrderAvroKey> inputOrderAvroKeySerde = new SpecificAvroSerde<>();
        SpecificAvroSerde<InputOrderAvro> inputOrderAvroSerde = new SpecificAvroSerde<>();
        SpecificAvroSerde<OutputOrderAvroKey> outputOrderAvroKeySerde = new SpecificAvroSerde<>();
        SpecificAvroSerde<OutputOrderAvro> outputOrderAvroSerde = new SpecificAvroSerde<>();

        inputOrderAvroKeySerde.configure(getConfigMap(), true);
        inputOrderAvroSerde.configure(getConfigMap(), false);
        outputOrderAvroKeySerde.configure(getConfigMap(), true);
        outputOrderAvroSerde.configure(getConfigMap(), false);

        builder
                .stream(
                        InputOrderConstants.INPUT_ORDER_TOPIC_AVRO,
                        Consumed
                                .with(inputOrderAvroKeySerde, inputOrderAvroSerde)
                                .withName("CONSUMER-INPUT-ORDER")
                )
                .map((inputOrderKey, inputOrder) -> {
                    OutputOrderAvroKey outputOrderKey = new OutputOrderAvroKey();
                    outputOrderKey.setId(inputOrderKey.getId());
                    OutputOrderAvro outputOrder = new OutputOrderAvro();
                    outputOrder.setDetail(inputOrder.getDetail());
                    return KeyValue.pair(outputOrderKey, outputOrder);
                })
                .to(
                        OutputOrderConstants.OUTPUT_ORDER_TOPIC_AVRO,
                        Produced
                                .with(outputOrderAvroKeySerde, outputOrderAvroSerde)
                                .withName("PRODUCER-OUTPUT-ORDER")
                );
        return builder.build();
    }

    public void start() {
        Properties props = getConfig();
        Topology topology = getTopology();
        stream = new KafkaStreams(topology, props);
        stream.start();
    }

    public void stop() {
        stream.close();
    }
}
