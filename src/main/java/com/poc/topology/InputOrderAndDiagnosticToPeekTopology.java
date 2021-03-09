package com.poc.topology;

import com.poc.inputorder.InputOrderConstants;
import com.poc.inputorder.avro.InputOrderAvro;
import com.poc.inputorder.avro.InputOrderAvroKey;
import com.poc.inputorder.avro.InputOrderDiagnosticAvro;
import com.poc.outputorder.avro.OutputOrderAvro;
import com.poc.outputorder.avro.OutputOrderAvroKey;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

public class InputOrderAndDiagnosticToPeekTopology extends BaseTopology {
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

        orders.peek((key, value) -> {
            //do nothing
        });

        diagnostics.peek((key, value) -> {
           //do nothing
        });


        return builder.build();
    }
}
