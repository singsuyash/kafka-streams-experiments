package com.poc.inputorder;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;

@Slf4j
public class InputOrderProducer {

    public Properties getConfig() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, InputOrderKeySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, InputOrderSerializer.class.getName());
        return props;
    }

    public void start(int num) throws InterruptedException {
        String topic = InputOrderConstants.INPUT_ORDER_TOPIC_JSON;
        log.info("Producing {} input orders to {}", num, topic);
        Properties config = getConfig();
        try (Producer<InputOrderKey, InputOrder> producer = new KafkaProducer<>(config)) {

            List<Future<RecordMetadata>> sends = new ArrayList<>();

            for (int i = 1; i <= num; i++) {
                InputOrderKey inputOrderKey = new InputOrderKey();
                InputOrder order = new InputOrder();
                ProducerRecord<InputOrderKey, InputOrder> record =
                        new ProducerRecord<>(topic, inputOrderKey, order);
                sends.add(producer.send(record));
            }

            while (!sends.parallelStream().allMatch(Future::isDone)) {
                try {
                    Thread.sleep(5);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    throw e;
                }
            }
        }
    }
}
