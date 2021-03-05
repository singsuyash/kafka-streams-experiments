package com.poc.inputorder;

import com.poc.AppConfig;
import com.poc.inputorder.avro.InputOrderAvro;
import com.poc.inputorder.avro.InputOrderAvroKey;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.*;

import java.util.*;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.lang.String.format;

public class InputOrderAvroProducer {

    public Properties getConfig() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfig.BOOTSTRAP_SERVER);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, SpecificAvroSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, SpecificAvroSerializer.class.getName());
        props.put("schema.registry.url", AppConfig.SCHEMA_REGISTRY_URL);
        return props;
    }

    public void start(int num) {
        String topic = InputOrderConstants.INPUT_ORDER_TOPIC_AVRO;
        System.out.println(format("Producing %s input orders to %s", num, topic));

        Properties config = getConfig();
        Producer<InputOrderAvroKey, InputOrderAvro> producer = new KafkaProducer<>(config);

        List<Future<RecordMetadata>> sends = new ArrayList<>();

        for(int i = 1; i <= num; i++) {
            InputOrderAvroKey inputOrderAvroKey = new InputOrderAvroKey(UUID.randomUUID().toString());
            InputOrderAvro order = new InputOrderAvro(IntStream.range(1,250).boxed().collect(Collectors.toList()));
            ProducerRecord<InputOrderAvroKey, InputOrderAvro> record =
                    new ProducerRecord<>(topic, inputOrderAvroKey, order);
            sends.add(producer.send(record));
        }

        while(true) {
            if (sends.parallelStream().noneMatch(r -> !r.isDone())) break;
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
