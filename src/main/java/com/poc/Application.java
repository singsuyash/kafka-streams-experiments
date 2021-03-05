package com.poc;

import com.poc.inputorder.InputOrderAvroProducer;
import com.poc.inputorder.InputOrderProducer;
import com.poc.inputorder.avro.InputOrderAvro;
import com.poc.topology.InputToOutputTopology;
import com.poc.topology.InputToPeekTopology;

import static java.lang.System.out;

public class Application {

    public static void main(String[] args) {
        int recordCount = Integer.parseInt(args[0]);

//        InputOrderProducer inputOrderProducer = new InputOrderProducer();
//        inputOrderProducer.start(recordCount);

        InputOrderAvroProducer inputOrderAvroProducer = new InputOrderAvroProducer();
        inputOrderAvroProducer.start(recordCount);

//        InputToOutputTopology topology = new InputToOutputTopology();
        InputToPeekTopology topology = new InputToPeekTopology();
//        topology.start();

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
