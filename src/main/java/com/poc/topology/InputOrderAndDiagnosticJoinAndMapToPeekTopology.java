package com.poc.topology;

import com.poc.AppConfig;
import com.poc.inputorder.InputOrderConstants;
import com.poc.inputorder.avro.InputOrderAvro;
import com.poc.inputorder.avro.InputOrderAvroKey;
import com.poc.inputorder.avro.InputOrderDiagnosticAvro;
import com.poc.outputorder.avro.OutputOrderAvro;
import com.poc.outputorder.avro.OutputOrderAvroKey;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class InputOrderAndDiagnosticJoinAndMapToPeekTopology {
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
        SpecificAvroSerde<InputOrderDiagnosticAvro> inputOrderDiagnosticAvroSerde = new SpecificAvroSerde<>();
        SpecificAvroSerde<OutputOrderAvroKey> outputOrderAvroKeySerde = new SpecificAvroSerde<>();
        SpecificAvroSerde<OutputOrderAvro> outputOrderAvroSerde = new SpecificAvroSerde<>();

        inputOrderAvroKeySerde.configure(getConfigMap(), true);
        inputOrderAvroSerde.configure(getConfigMap(), false);
        inputOrderDiagnosticAvroSerde.configure(getConfigMap(), false);
        outputOrderAvroKeySerde.configure(getConfigMap(), true);
        outputOrderAvroSerde.configure(getConfigMap(), false);

        KStream<InputOrderAvroKey, InputOrderAvro> orders = builder.stream(
                InputOrderConstants.INPUT_ORDER_TOPIC_AVRO,
                Consumed
                        .with(inputOrderAvroKeySerde, inputOrderAvroSerde)
                        .withName("CONSUMER-INPUT-ORDER")
        );

        KStream<InputOrderAvroKey, InputOrderDiagnosticAvro> diagnostics = builder.stream(
                InputOrderConstants.INPUT_ORDER_DIAGNOSTIC_TOPIC_AVRO,
                Consumed
                        .with(inputOrderAvroKeySerde, inputOrderDiagnosticAvroSerde)
                        .withName("CONSUMER-INPUT-ORDER-DIAGNOSTIC")
        );

        ValueJoiner<InputOrderAvro, InputOrderDiagnosticAvro, OutputOrderAvro> joiner = (order, diagnostic) -> {
            OutputOrderAvro outputOrderAvro = new OutputOrderAvro();
            outputOrderAvro.setDetail(IntStream.range(1,250).boxed().collect(Collectors.toList()));
            return outputOrderAvro;
        };

        JoinWindows joinWindows = JoinWindows.of(Duration.ofMinutes(1));

        StreamJoined<InputOrderAvroKey, InputOrderAvro, InputOrderDiagnosticAvro> streamJoined =
                StreamJoined.with(inputOrderAvroKeySerde, inputOrderAvroSerde, inputOrderDiagnosticAvroSerde);

        KeyValueMapper<InputOrderAvroKey, OutputOrderAvro, KeyValue<OutputOrderAvroKey, OutputOrderAvro>> mapper = (key, value) -> {
            OutputOrderAvroKey newKey = new OutputOrderAvroKey();
            newKey.setId(key.getId());
            return KeyValue.pair(newKey, value);
        };

        orders
                .join(
                        diagnostics,
                        joiner,
                        joinWindows,
                        streamJoined
                )
                .map(mapper)
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
