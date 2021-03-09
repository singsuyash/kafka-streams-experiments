package com.poc;

import com.poc.topology.InputOrderAndDiagnosticAndAttributeJoinToMapToOutputAvroTopology;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class Application {

    public static void main(String[] args) throws InterruptedException {
        SpringApplication.run(Application.class, args);
//        int recordCount = Integer.parseInt(args[0]);

//        InputOrderProducer inputOrderProducer = new InputOrderProducer();
//        inputOrderProducer.start(recordCount);
//
//        InputOrderAvroProducer inputOrderAvroProducer = new InputOrderAvroProducer();
//        inputOrderAvroProducer.start(recordCount);

//        InputOrderAndDiagnosticAvroProducer inputOrderAndDiagnosticAvroProducer = new InputOrderAndDiagnosticAvroProducer();
//        inputOrderAndDiagnosticAvroProducer.start(recordCount);

//        InputOrderAndDiagnosticAndAttributeAvroProducer inputOrderAndDiagnosticAndAttributeAvroProducer = new InputOrderAndDiagnosticAndAttributeAvroProducer();
//        inputOrderAndDiagnosticAndAttributeAvroProducer.start(recordCount);

//        InputToOutputTopology topology = new InputToOutputTopology();
//        InputToPeekTopology topology = new InputToPeekTopology();
//        InputToPeekAvroTopology topology = new InputToPeekAvroTopology();
//        InputToOutputAvroTopology topology = new InputToOutputAvroTopology();
//        InputOrderAndDiagnosticJoinToPeekTopology topology = new InputOrderAndDiagnosticJoinToPeekTopology();
//        InputOrderAndDiagnosticToPeekTopology topology = new InputOrderAndDiagnosticToPeekTopology();
//        InputOrderAndDiagnosticJoinAndMapToPeekTopology topology = new InputOrderAndDiagnosticJoinAndMapToPeekTopology();
//        InputOrderAndDiagnosticJoinAndMapToOutputAvroTopology topology = new InputOrderAndDiagnosticJoinAndMapToOutputAvroTopology();
//        InputOrderAndAttributeJoinToPeekTopology topology = new InputOrderAndAttributeJoinToPeekTopology();
//        InputOrderAndDiagnosticAndAttributeJoinToPeekTopology topology = new InputOrderAndDiagnosticAndAttributeJoinToPeekTopology();
//        InputOrderAndDiagnosticAndAttributeJoinToMapToPeekTopology topology = new InputOrderAndDiagnosticAndAttributeJoinToMapToPeekTopology();
        InputOrderAndDiagnosticAndAttributeJoinToMapToOutputAvroTopology topology = new InputOrderAndDiagnosticAndAttributeJoinToMapToOutputAvroTopology();

        topology.start();

        Runtime
                .getRuntime()
                .addShutdownHook(
                        new Thread("STREAMS-SHUTDOWN-HOOK") {
                            @Override
                            public void run() {
                                topology.stop();
                            }
                        }
                );
    }
}
