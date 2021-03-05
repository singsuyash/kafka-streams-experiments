package com.poc.topology;

import com.poc.inputorder.*;
import com.poc.outputorder.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

public class InputToOutputTopology {
    private final StreamsBuilder builder = new StreamsBuilder();
    private KafkaStreams stream;
    private Topology topology;

    public Properties getConfig() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "input-to-output");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        return props;
    }

    public Topology getTopology() {
        KStream<String, InputOrder> stream = builder
                .stream(
                        InputOrderConstants.INPUT_ORDER_TOPIC_NAME,
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
                );
        stream
                .map((key, inputOrder) -> {
                    OutputOrderKey outputOrderKey = new OutputOrderKey();
                    OutputOrder outputOrder = new OutputOrder();
                    outputOrder.detail = inputOrder.detail;
                    return KeyValue.pair(outputOrderKey, outputOrder);
                })
                .to(
                        OutputOrderConstants.OUTPUT_ORDER_TOPIC_NAME,
                        Produced.as("PRODUCER-OUTPUT-ORDER")
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

    public void start() {
        Properties props = getConfig();
        topology = getTopology();
        stream = new KafkaStreams(topology, props);
        stream.start();
    }

    public void stop() {
        stream.close();
    }
}
