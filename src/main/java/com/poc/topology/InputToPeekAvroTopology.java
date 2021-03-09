package com.poc.topology;

import com.poc.inputorder.InputOrderConstants;
import com.poc.inputorder.avro.InputOrderAvro;
import com.poc.inputorder.avro.InputOrderAvroKey;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;

@Slf4j
public class InputToPeekAvroTopology extends BaseTopology {
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
}
