package com.poc.topology;

import com.poc.AppConfig;
import com.poc.inputorder.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;

import java.util.Properties;

@Slf4j
public class InputToPeekTopology {
    private final StreamsBuilder builder = new StreamsBuilder();
    private KafkaStreams stream;

    public Properties getConfig() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, AppConfig.APPLICATION_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfig.BOOTSTRAP_SERVER);
        return props;
    }

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

    public void start() {
        Properties props = getConfig();
        Topology topology = getTopology();
        stream = new KafkaStreams(topology, props);
        log.info("Starting Streams...");
        stream.start();
    }

    public void stop() {
        log.info("Stopping Streams...");
        stream.close();
    }
}
