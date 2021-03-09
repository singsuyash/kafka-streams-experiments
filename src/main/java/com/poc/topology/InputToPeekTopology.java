package com.poc.topology;

import com.poc.inputorder.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;

@Slf4j
public class InputToPeekTopology extends BaseTopology {
    public Topology getTopology() {
        builder
                .stream(
                        InputOrderConstants.INPUT_ORDER_TOPIC_JSON,
                        Consumed.with(
                                Serdes.serdeFrom(
                                        new InputOrderKeySerializer(),
                                        new InputOrderKeyDeserializer()
                                ),
                                Serdes.serdeFrom(
                                        new InputOrderSerializer(),
                                        new InputOrderDeserializer()
                                )
                        ))
                .peek(
                        (key, value) -> {
                            //do nothing
                        }
                );

        return builder.build();
    }
}
