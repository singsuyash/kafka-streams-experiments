package com.poc.topology;

import com.poc.inputorder.InputOrderConstants;
import com.poc.inputorder.avro.InputOrderAttributeAvro;
import com.poc.inputorder.avro.InputOrderAvro;
import com.poc.inputorder.avro.InputOrderAvroKey;
import com.poc.inputorder.avro.InputOrderDiagnosticAvro;
import com.poc.outputorder.OutputOrderConstants;
import com.poc.outputorder.avro.OutputOrderAvro;
import com.poc.outputorder.avro.OutputOrderAvroKey;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class InputOrderAndDiagnosticAndAttributeJoinToMapToOutputAvroTopology extends BaseTopology {
    public Topology getTopology() {
        SpecificAvroSerde<InputOrderAvroKey> inputOrderAvroKeySerde = new SpecificAvroSerde<>();
        SpecificAvroSerde<InputOrderAvro> inputOrderAvroSerde = new SpecificAvroSerde<>();
        SpecificAvroSerde<InputOrderDiagnosticAvro> inputOrderDiagnosticAvroSerde = new SpecificAvroSerde<>();
        SpecificAvroSerde<InputOrderAttributeAvro> inputOrderAttributeAvroSerde = new SpecificAvroSerde<>();
        SpecificAvroSerde<OutputOrderAvroKey> outputOrderAvroKeySerde = new SpecificAvroSerde<>();
        SpecificAvroSerde<OutputOrderAvro> outputOrderAvroSerde = new SpecificAvroSerde<>();

        inputOrderAvroKeySerde.configure(getConfigMap(), true);
        inputOrderAvroSerde.configure(getConfigMap(), false);
        inputOrderDiagnosticAvroSerde.configure(getConfigMap(), false);
        inputOrderAttributeAvroSerde.configure(getConfigMap(), false);
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

        KTable<InputOrderAvroKey, InputOrderAttributeAvro> attributes =
                builder
                        .stream(
                                InputOrderConstants.INPUT_ORDER_ATTRIBUTE_TOPIC_AVRO,
                                Consumed
                                        .with(inputOrderAvroKeySerde, inputOrderAttributeAvroSerde)
                                        .withName("CONSUMER-INPUT-ORDER-ATTRIBUTE")
                        )
                        .toTable();

        ValueJoiner<InputOrderAvro, InputOrderDiagnosticAvro, InputOrderAvro> orderAndDiagnosticJoiner = (order, diagnostic) -> {
            InputOrderAvro inputOrderAvro = new InputOrderAvro();
            inputOrderAvro.setDetail(IntStream.range(1, 250).boxed().collect(Collectors.toList()));
            return inputOrderAvro;
        };

        JoinWindows joinWindows = JoinWindows.of(Duration.ofMinutes(1));

        StreamJoined<InputOrderAvroKey, InputOrderAvro, InputOrderDiagnosticAvro> streamJoined =
                StreamJoined.with(inputOrderAvroKeySerde, inputOrderAvroSerde, inputOrderDiagnosticAvroSerde);

        ValueJoiner<InputOrderAvro, InputOrderAttributeAvro, OutputOrderAvro> orderAndAttributeJoiner = (value1, value2) -> {
            OutputOrderAvro outputOrderAvro = new OutputOrderAvro();
            outputOrderAvro.setDetail(value2 != null ? value2.getDetail() : value1.getDetail());
            return outputOrderAvro;
        };

        Joined<InputOrderAvroKey, InputOrderAvro, InputOrderAttributeAvro> joined =
                Joined.with(inputOrderAvroKeySerde, inputOrderAvroSerde, inputOrderAttributeAvroSerde);

        KeyValueMapper<InputOrderAvroKey, OutputOrderAvro, KeyValue<OutputOrderAvroKey, OutputOrderAvro>> mapper = (key, value) -> {
            OutputOrderAvroKey newKey = new OutputOrderAvroKey();
            newKey.setId(key.getId());
            return KeyValue.pair(newKey, value);
        };

        orders
                .join(
                        diagnostics,
                        orderAndDiagnosticJoiner,
                        joinWindows,
                        streamJoined
                )
                .leftJoin(
                        attributes,
                        orderAndAttributeJoiner,
                        joined
                )
                .map(mapper)
                .to(
                        OutputOrderConstants.OUTPUT_ORDER_TOPIC_AVRO,
                        Produced
                                .with(outputOrderAvroKeySerde, outputOrderAvroSerde)
                                .withName("PRODUCER-OUTPUT-ORDER")
                );

        return builder.build();
    }
}
