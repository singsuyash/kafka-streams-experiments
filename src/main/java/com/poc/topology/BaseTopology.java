package com.poc.topology;

import com.poc.AppConfig;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public abstract class BaseTopology {
    protected final StreamsBuilder builder = new StreamsBuilder();
    private static KafkaStreams stream;

    public static KafkaStreams getStream() {
        return stream;
    }

    private Properties getConfig() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, AppConfig.APPLICATION_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfig.BOOTSTRAP_SERVER);
        return props;
    }

    protected Map<String, Object> getConfigMap() {
        Map<String, Object> props = new HashMap<>();
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, AppConfig.SCHEMA_REGISTRY_URL);
        return props;
    }

    protected abstract Topology getTopology();

    public void start() {
        Properties props = getConfig();
        Topology topology = getTopology();
        stream = new KafkaStreams(topology, props);
        stream.start();

        Runtime
                .getRuntime()
                .addShutdownHook(
                        new Thread("STREAMS-SHUTDOWN-HOOK") {
                            @Override
                            public void run() {
                                stream.close();
                            }
                        }
                );
    }
}
