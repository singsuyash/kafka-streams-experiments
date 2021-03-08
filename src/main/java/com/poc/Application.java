package com.poc;

import com.poc.topology.InputOrderAndDiagnosticJoinToPeekTopology;

public class Application {

    public static void main(String[] args) throws InterruptedException {
//        int recordCount = Integer.parseInt(args[0]);

//        InputOrderProducer inputOrderProducer = new InputOrderProducer();
//        inputOrderProducer.start(recordCount);
//
//        InputOrderAvroProducer inputOrderAvroProducer = new InputOrderAvroProducer();
//        inputOrderAvroProducer.start(recordCount);

//        InputOrderAndDiagnosticAvroProducer inputOrderAndDiagnosticAvroProducer = new InputOrderAndDiagnosticAvroProducer();
//        inputOrderAndDiagnosticAvroProducer.start(recordCount);

//        InputToOutputTopology topology = new InputToOutputTopology();
//        InputToPeekTopology topology = new InputToPeekTopology();
//        InputToPeekAvroTopology topology = new InputToPeekAvroTopology();
//        InputToOutputAvroTopology topology = new InputToOutputAvroTopology();
        InputOrderAndDiagnosticJoinToPeekTopology topology = new InputOrderAndDiagnosticJoinToPeekTopology();

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
