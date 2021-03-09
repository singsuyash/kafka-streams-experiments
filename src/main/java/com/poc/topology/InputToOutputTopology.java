package com.poc.topology;

import com.poc.inputorder.*;
import com.poc.outputorder.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

public class InputToOutputTopology extends BaseTopology {
    public Topology getTopology() {
        builder
                .stream(
                        InputOrderConstants.INPUT_ORDER_TOPIC_JSON,
                        Consumed
                                .with(
                                        Serdes.serdeFrom(
                                                new InputOrderKeySerializer(),
                                                new InputOrderKeyDeserializer()
                                        ),
                                        Serdes.serdeFrom(
                                                new InputOrderSerializer(),
                                                new InputOrderDeserializer()
                                        )
                                )
                                .withName("CONSUMER-INPUT-ORDER")
                )
                .map((key, inputOrder) -> {
                    OutputOrderKey outputOrderKey = new OutputOrderKey();
                    OutputOrder outputOrder = new OutputOrder();
                    outputOrder.detail = inputOrder.detail;
                    return KeyValue.pair(outputOrderKey, outputOrder);
                })
                .to(
                        OutputOrderConstants.OUTPUT_ORDER_TOPIC_JSON,
                        Produced
                                .with(
                                        Serdes.serdeFrom(
                                                new OutputOrderKeySerializer(),
                                                new OutputOrderKeyDeserializer()
                                        ),
                                        Serdes.serdeFrom(
                                                new OutputOrderSerializer(),
                                                new OutputOrderDeserializer()
                                        )
                                )
                                .withName("PRODUCER-OUTPUT-ORDER")
                );
        return builder.build();
    }
}
