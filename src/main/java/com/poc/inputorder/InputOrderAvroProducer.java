package com.poc.inputorder;

import com.poc.AppConfig;
import com.poc.inputorder.avro.InputOrderAvro;
import com.poc.inputorder.avro.InputOrderAvroKey;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
public class InputOrderAvroProducer {

    public Properties getConfig() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfig.BOOTSTRAP_SERVER);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, SpecificAvroSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, SpecificAvroSerializer.class.getName());
        props.put("schema.registry.url", AppConfig.SCHEMA_REGISTRY_URL);
        return props;
    }

    public void start(int num) throws InterruptedException {
        String topic = InputOrderConstants.INPUT_ORDER_TOPIC_AVRO;
        log.info("Producing {} input orders to {}", num, topic);

        Properties config = getConfig();
        List<Future<RecordMetadata>> sends;
        try (Producer<InputOrderAvroKey, InputOrderAvro> producer = new KafkaProducer<>(config)) {

            sends = new ArrayList<>();

            for (int i = 1; i <= num; i++) {
                InputOrderAvroKey inputOrderAvroKey = new InputOrderAvroKey(UUID.randomUUID().toString());
                InputOrderAvro order = new InputOrderAvro(IntStream.range(1, 250).boxed().collect(Collectors.toList()));
                ProducerRecord<InputOrderAvroKey, InputOrderAvro> record =
                        new ProducerRecord<>(topic, inputOrderAvroKey, order);
                sends.add(producer.send(record));
            }
        }

        while (sends.parallelStream().anyMatch(r -> !r.isDone())) {
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                e.printStackTrace();
                throw e;
            }
        }
    }
}
