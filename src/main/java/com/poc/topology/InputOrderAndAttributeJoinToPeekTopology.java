package com.poc.topology;

import com.poc.AppConfig;
import com.poc.inputorder.InputOrderConstants;
import com.poc.inputorder.avro.InputOrderAttributeAvro;
import com.poc.inputorder.avro.InputOrderAvro;
import com.poc.inputorder.avro.InputOrderAvroKey;
import com.poc.outputorder.avro.OutputOrderAvro;
import com.poc.outputorder.avro.OutputOrderAvroKey;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class InputOrderAndAttributeJoinToPeekTopology {
    private final StreamsBuilder builder = new StreamsBuilder();
    private static KafkaStreams stream;

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
        SpecificAvroSerde<InputOrderAttributeAvro> inputOrderAttributeAvroSerde = new SpecificAvroSerde<>();
        SpecificAvroSerde<OutputOrderAvroKey> outputOrderAvroKeySerde = new SpecificAvroSerde<>();
        SpecificAvroSerde<OutputOrderAvro> outputOrderAvroSerde = new SpecificAvroSerde<>();

        inputOrderAvroKeySerde.configure(getConfigMap(), true);
        inputOrderAvroSerde.configure(getConfigMap(), false);
        inputOrderAttributeAvroSerde.configure(getConfigMap(), false);
        outputOrderAvroKeySerde.configure(getConfigMap(), true);
        outputOrderAvroSerde.configure(getConfigMap(), false);

        KStream<InputOrderAvroKey, InputOrderAvro> orders = builder.stream(
                InputOrderConstants.INPUT_ORDER_TOPIC_AVRO,
                Consumed
                        .with(inputOrderAvroKeySerde, inputOrderAvroSerde)
                        .withName("CONSUMER-INPUT-ORDER")
        );

        KTable<InputOrderAvroKey, InputOrderAttributeAvro> attributes =
                builder
                        .stream(
                                InputOrderConstants.INPUT_ORDER_ATTRIBUTE_TOPIC_AVRO,
                                Consumed
                                        .with(inputOrderAvroKeySerde, inputOrderAttributeAvroSerde)
                                        .withName("CONSUMER-INPUT-ORDER-DIAGNOSTIC")
                        )
                        .toTable();

        ValueJoiner<InputOrderAvro, InputOrderAttributeAvro, OutputOrderAvro> joiner = (value1, value2) -> {
            OutputOrderAvro outputOrderAvro = new OutputOrderAvro();
            outputOrderAvro.setDetail(value2 != null ? value2.getDetail() : value1.getDetail());
            return outputOrderAvro;
        };

        Joined<InputOrderAvroKey, InputOrderAvro, InputOrderAttributeAvro> joined =
                Joined.with(inputOrderAvroKeySerde, inputOrderAvroSerde, inputOrderAttributeAvroSerde);

        orders
                .leftJoin(
                        attributes,
                        joiner,
                        joined
                )
                .peek((key, value) -> {
                    //do nothing
                });

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

    public static KafkaStreams getKafkaStream() {
        return stream;
    }
}
