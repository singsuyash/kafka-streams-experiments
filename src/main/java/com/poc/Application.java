package com.poc;

import com.poc.topology.InputToPeekTopology;

public class Application {

    public static void main(String[] args) throws InterruptedException {
//        int recordCount = Integer.parseInt(args[0]);

//        InputOrderProducer inputOrderProducer = new InputOrderProducer();
//        inputOrderProducer.start(recordCount);
//
//        InputOrderAvroProducer inputOrderAvroProducer = new InputOrderAvroProducer();
//        inputOrderAvroProducer.start(recordCount);

//        InputToOutputTopology topology = new InputToOutputTopology();
        InputToPeekTopology topology = new InputToPeekTopology();
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
