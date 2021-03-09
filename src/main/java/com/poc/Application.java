package com.poc;

import com.poc.inputorder.InputOrderAndDiagnosticAndAttributeAvroProducer;
import com.poc.inputorder.InputOrderProducer;
import com.poc.topology.*;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class Application {

    public static void main(String[] args) throws InterruptedException {
        SpringApplication.run(Application.class, args);
        int num = Integer.parseInt(args[0]);
        if (num >= 100) {
            produceMessages(num);
            return;
        }
        startTopology(num);
    }

    private static void startTopology(int num) {
        BaseTopology topology;
        switch (num) {
            case 0:
                topology = new InputToPeekTopology();
                break;
            case 1:
                topology = new InputToOutputTopology();
                break;
            case 2:
                topology = new InputToPeekAvroTopology();
                break;
            case 3:
                topology = new InputToOutputAvroTopology();
                break;
            case 4:
                topology = new InputOrderAndDiagnosticToPeekTopology();
                break;
            case 5:
                topology = new InputOrderAndDiagnosticJoinToPeekTopology();
                break;
            case 6:
                topology = new InputOrderAndAttributeJoinToPeekTopology();
                break;
            case 7:
                topology = new InputOrderAndDiagnosticAndAttributeJoinToPeekTopology();
                break;
            case 8:
                topology = new InputOrderAndDiagnosticAndAttributeJoinToMapToPeekTopology();
                break;
            case 9:
                topology = new InputOrderAndDiagnosticAndAttributeJoinToMapToOutputAvroTopology();
                break;
            default:
                topology = new InputToPeekAvroTopology();
                break;
        }
        topology.start();
    }

    private static void produceMessages(int recordCount) throws InterruptedException {

        InputOrderProducer inputOrderProducer = new InputOrderProducer();
        inputOrderProducer.start(recordCount);

        InputOrderAndDiagnosticAndAttributeAvroProducer inputOrderAndDiagnosticAndAttributeAvroProducer = new InputOrderAndDiagnosticAndAttributeAvroProducer();
        inputOrderAndDiagnosticAndAttributeAvroProducer.start(recordCount);
    }
}
