package com.poc.inputorder;

import com.poc.inputorder.avro.InputOrderAvro;
import com.poc.inputorder.avro.InputOrderAvroKey;
import com.poc.inputorder.avro.InputOrderDiagnosticAvro;
import com.poc.topology.InputOrderAndDiagnosticJoinToPeekTopology;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BinaryOperator;

@RestController
@RequestMapping("/v1/stateStore")
public class InputOrderStateStoreController {

    private static final String ORDER_STATE_STORE = "KSTREAM-JOINTHIS-0000000004-store";
    private static final String DIAGNOSTIC_STATE_STORE = "KSTREAM-JOINOTHER-0000000005-store";

    private ReadOnlyWindowStore<InputOrderAvroKey, InputOrderAvro> getOrderStore() {
        return InputOrderAndDiagnosticJoinToPeekTopology.getKafkaStream()
                .store(StoreQueryParameters.fromNameAndType(ORDER_STATE_STORE, QueryableStoreTypes.windowStore()));
    }

    private ReadOnlyWindowStore<InputOrderAvroKey, InputOrderDiagnosticAvro> getDiagnosticStore() {
        return InputOrderAndDiagnosticJoinToPeekTopology.getKafkaStream()
                .store(StoreQueryParameters.fromNameAndType(DIAGNOSTIC_STATE_STORE, QueryableStoreTypes.windowStore()));
    }

    @GetMapping("/order/window/{seconds}/keys")
    public List<String> getOrderKeys(@PathVariable("seconds") int duration) {
        List<String> keys = new ArrayList<>();
        Instant to = Instant.now();
        Instant from = to.minusSeconds(duration);
        KeyValueIterator<Windowed<InputOrderAvroKey>, InputOrderAvro> iterator = getOrderStore().fetchAll(from, to);
        iterator.forEachRemaining(x -> keys.add(x.key.key().toString()));
        return keys;
    }

    @GetMapping("/order/window/{seconds}/count")
    public long getOrderCount(@PathVariable("seconds") int duration) {
        return getOrderKeys(duration).size();
    }

    @GetMapping("/diagnostic/window/{seconds}/keys")
    public List<String> getDiagnosticKeys(@PathVariable("seconds") int duration) {
        List<String> keys = new ArrayList<>();
        Instant to = Instant.now();
        Instant from = to.minusSeconds(duration);
        KeyValueIterator<Windowed<InputOrderAvroKey>, InputOrderDiagnosticAvro> iterator = getDiagnosticStore().fetchAll(from, to);
        iterator.forEachRemaining(x -> keys.add(x.key.key().toString()));
        return keys;
    }

    @GetMapping("/diagnostic/window/{seconds}/count")
    public long getDiagnosticCount(@PathVariable("seconds") int duration) {
        return getDiagnosticKeys(duration).size();
    }

    @GetMapping("/order/diagnostic/window/{seconds}/joinAverage/")
    public String getOrderAndDiagnosticJoinAverage(@PathVariable("seconds") int duration) {
        List<String> keys = new ArrayList<>();
        Instant to = Instant.now();
        Instant from = to.minusSeconds(duration);
        ReadOnlyWindowStore<InputOrderAvroKey, InputOrderAvro> orderStore = getOrderStore();
        ReadOnlyWindowStore<InputOrderAvroKey, InputOrderDiagnosticAvro> diagnosticStore = getDiagnosticStore();

        KeyValueIterator<Windowed<InputOrderAvroKey>, InputOrderAvro> iterator = getOrderStore().fetchAll(from, to);
        AtomicInteger num = new AtomicInteger();
        List<AtomicLong> fetchSensor = new ArrayList<>();

        iterator.forEachRemaining(x -> {
            num.getAndIncrement();
            long startTime = System.currentTimeMillis();
            getDiagnosticStore().fetch(x.key.key(), from.minusSeconds(duration), to.plusSeconds(duration));
            long endTime = System.currentTimeMillis();
            fetchSensor.add(new AtomicLong(endTime - startTime));
        });

        BinaryOperator<AtomicLong> accumulator = (left, right) -> new AtomicLong(left.get() + right.get());

        long l = fetchSensor.stream().reduce(accumulator).get().get();
        return String.format("Sum: %s ms, Average: %s ms", l, Double.valueOf(l)/fetchSensor.size());
    }
//
//    @GetMapping("/crorder/window/{seconds}/count")
//    public long getCrorderCount(@PathVariable("seconds") int duration) {
//        return getCrorderKeys(duration).size();
//    }
//
//    @GetMapping("/crorder/window/{seconds}/duplicates/keys")
//    public List<String> getCrorderDuplicatesKeys(@PathVariable("seconds") int duration) {
//        List<String> outputKeys = new ArrayList<>();
//        getCrorderKeys(duration)
//                .stream()
//                .collect(Collectors.groupingBy(x -> x))
//                .forEach((k, v) -> {
//                    if (v.size() > 1) {
//                        outputKeys.add(k);
//                    }
//                });
//        return outputKeys;
//    }
//
//    @GetMapping("/crorder/window/{seconds}/duplicates/count")
//    public int getCrorderDuplicatesKeysCount(@PathVariable("seconds") int duration) {
//        return getCrorderDuplicatesKeys(duration).size();
//    }
//
//    @GetMapping("/crorder/window/{seconds}/runID/{runID}/calculationDate/{calculationDate}/storeNumber/{storeNumber}/productNumber/{productNumber}/requiredDeliveryDate/{requiredDeliveryDate}")
//    public List<String> getCrorder(
//            @PathVariable("seconds") int duration,
//            @PathVariable("runID") String runID,
//            @PathVariable("calculationDate") String calculationDate,
//            @PathVariable("storeNumber") int storeNumber,
//            @PathVariable("productNumber") int productNumber,
//            @PathVariable("requiredDeliveryDate") String requiredDeliveryDate) {
//        List<String> crorders = new ArrayList<>();
//        Instant to = Instant.now();
//        Instant from = to.minusSeconds(duration);
//        CROrderKey key = CROrderKey
//                .newBuilder()
//                .setRunID(runID)
//                .setCalculationDate(calculationDate)
//                .setStoreNumber(storeNumber)
//                .setProductNumber(productNumber)
//                .setRequiredDeliveryDate(requiredDeliveryDate)
//                .build();
//        WindowStoreIterator<CROrder> iterator = getCrorderStore().fetch(key, from, to);
//        iterator.forEachRemaining(x -> crorders.add(x.value.toString()));
//        return crorders;
//    }
}
