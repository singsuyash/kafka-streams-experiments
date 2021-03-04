package com.poc.inputorder;

import java.util.UUID;
import java.util.stream.IntStream;

public class InputOrder {
    public String key = UUID.randomUUID().toString();
    public int[] value = IntStream.range(0,249).map(x -> 1).toArray();
}
