package com.poc.topology;

import com.poc.inputorder.InputOrderConstants;
import com.poc.inputorder.avro.InputOrderAttributeAvro;
import com.poc.inputorder.avro.InputOrderAvro;
import com.poc.inputorder.avro.InputOrderAvroKey;
import com.poc.outputorder.avro.OutputOrderAvro;
import com.poc.outputorder.avro.OutputOrderAvroKey;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

public class InputOrderAndAttributeJoinToPeekTopology extends BaseTopology {
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
                                        .withName("CONSUMER-INPUT-ORDER-ATTRIBUTE1")
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
}
