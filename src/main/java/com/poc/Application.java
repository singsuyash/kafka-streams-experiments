package com.poc;

import com.poc.inputorder.InputOrderProducer;
import com.poc.topology.InputToOutputTopology;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.List;
import java.util.concurrent.Future;

import static java.lang.System.out;

public class Application {

    public static void main(String[] args) {
        InputOrderProducer inputOrderProducer = new InputOrderProducer();
        inputOrderProducer.start();

        InputToOutputTopology topology = new InputToOutputTopology();
        topology.start();
        out.println("Starting Streams...");

        Runtime.getRuntime().addShutdownHook(new Thread("STREAMS-SHUTDOWN-HOOK") {
            @Override
            public void run() {
                topology.stop();
                out.println("Stopping Streams...");
            }
        });
    }
}
