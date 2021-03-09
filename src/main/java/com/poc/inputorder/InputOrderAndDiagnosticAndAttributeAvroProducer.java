package com.poc.inputorder;

import com.poc.AppConfig;
import com.poc.inputorder.avro.InputOrderAttributeAvro;
import com.poc.inputorder.avro.InputOrderAvro;
import com.poc.inputorder.avro.InputOrderAvroKey;
import com.poc.inputorder.avro.InputOrderDiagnosticAvro;
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
public class InputOrderAndDiagnosticAndAttributeAvroProducer {

    public Properties getConfig() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfig.BOOTSTRAP_SERVER);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, SpecificAvroSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, SpecificAvroSerializer.class.getName());
        props.put("schema.registry.url", AppConfig.SCHEMA_REGISTRY_URL);
        return props;
    }

    public void start(int num) throws InterruptedException {
        String inputOrderTopicAvro = InputOrderConstants.INPUT_ORDER_TOPIC_AVRO;
        String inputOrderDiagnosticTopicAvro = InputOrderConstants.INPUT_ORDER_DIAGNOSTIC_TOPIC_AVRO;
        String inputOrderAttributeTopicAvro = InputOrderConstants.INPUT_ORDER_ATTRIBUTE_TOPIC_AVRO;

        log.info("Producing {} input orders to {} and {} and {}", num, inputOrderTopicAvro, inputOrderDiagnosticTopicAvro, inputOrderAttributeTopicAvro);

        Properties config = getConfig();
        List<Future<RecordMetadata>> sends;
        try (
                Producer<InputOrderAvroKey, InputOrderAvro> orderProducer = new KafkaProducer<>(config);
                Producer<InputOrderAvroKey, InputOrderDiagnosticAvro> diagnosticProducer = new KafkaProducer<>(config);
                Producer<InputOrderAvroKey, InputOrderAttributeAvro> attributeProducer = new KafkaProducer<>(config)
        ) {

            sends = new ArrayList<>();

            for (int i = 1; i <= num; i++) {
                InputOrderAvroKey inputOrderAvroKey = new InputOrderAvroKey(UUID.randomUUID().toString());
                InputOrderAvro order = new InputOrderAvro(IntStream.range(1, 250).boxed().collect(Collectors.toList()));
                InputOrderDiagnosticAvro diagnostic = new InputOrderDiagnosticAvro(IntStream.range(1, 250).boxed().collect(Collectors.toList()));
                InputOrderAttributeAvro attribute = new InputOrderAttributeAvro(IntStream.range(1,250).boxed().collect(Collectors.toList()));

                ProducerRecord<InputOrderAvroKey, InputOrderAvro> orderRecord =
                        new ProducerRecord<>(inputOrderTopicAvro, inputOrderAvroKey, order);
                sends.add(orderProducer.send(orderRecord));

                ProducerRecord<InputOrderAvroKey, InputOrderDiagnosticAvro> diagnosticRecord =
                        new ProducerRecord<>(inputOrderDiagnosticTopicAvro, inputOrderAvroKey, diagnostic);
                sends.add(diagnosticProducer.send(diagnosticRecord));

                ProducerRecord<InputOrderAvroKey, InputOrderAttributeAvro> attributeRecord =
                        new ProducerRecord<>(inputOrderAttributeTopicAvro, inputOrderAvroKey, attribute);
                sends.add(attributeProducer.send(attributeRecord));
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
