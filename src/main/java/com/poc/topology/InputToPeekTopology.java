package com.poc.topology;

import com.poc.inputorder.InputOrder;
import com.poc.inputorder.InputOrderConstants;
import com.poc.inputorder.InputOrderDeserializer;
import com.poc.inputorder.InputOrderSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

import static java.lang.System.out;

public class InputToPeekTopology {
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
                                .with(Serdes.String(), Serdes.serdeFrom(new InputOrderSerializer(), new InputOrderDeserializer()))
                                .withName("CONSUMER-INPUT-ORDER")
                );
        stream
                .peek((key, value) -> {
                    //do nothing
                });
        return builder.build();
    }

    public void start() {
        Properties props = getConfig();
        topology = getTopology();
        stream = new KafkaStreams(topology, props);
        out.println("Starting Streams...");
        stream.start();
    }

    public void stop() {
        out.println("Stopping Streams...");
        stream.close();
    }
}
