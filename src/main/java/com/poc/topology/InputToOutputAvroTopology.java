package com.poc.topology;

import com.poc.inputorder.InputOrderConstants;
import com.poc.inputorder.avro.InputOrderAvro;
import com.poc.inputorder.avro.InputOrderAvroKey;
import com.poc.outputorder.OutputOrderConstants;
import com.poc.outputorder.avro.OutputOrderAvro;
import com.poc.outputorder.avro.OutputOrderAvroKey;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

public class InputToOutputAvroTopology extends BaseTopology {
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
}
