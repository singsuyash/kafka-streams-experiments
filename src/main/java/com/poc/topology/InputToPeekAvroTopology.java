package com.poc.topology;

import com.poc.AppConfig;
import com.poc.inputorder.InputOrderConstants;
import com.poc.inputorder.avro.InputOrderAvro;
import com.poc.inputorder.avro.InputOrderAvroKey;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Slf4j
public class InputToPeekAvroTopology {
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

        inputOrderAvroKeySerde.configure(getConfigMap(), true);
        inputOrderAvroSerde.configure(getConfigMap(), false);

        builder
                .stream(
                        InputOrderConstants.INPUT_ORDER_TOPIC_AVRO,
                        Consumed.with(
                                new SpecificAvroSerde<InputOrderAvroKey>(),
                                new SpecificAvroSerde<InputOrderAvro>()
                        ))
                .peek(
                        (key, value) -> {
                            //do nothing
                        }
                );

        return builder.build();
    }

    public void start() {
        Properties props = getConfig();
        Topology topology = getTopology();
        stream = new KafkaStreams(topology, props);
        log.info("Starting Streams...");
        stream.start();
    }

    public void stop() {
        log.info("Stopping Streams...");
        stream.close();
    }
}
