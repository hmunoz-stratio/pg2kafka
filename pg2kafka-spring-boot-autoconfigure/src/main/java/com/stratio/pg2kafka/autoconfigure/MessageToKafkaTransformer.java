package com.stratio.pg2kafka.autoconfigure;

import java.util.Map;

@FunctionalInterface
public interface MessageToKafkaTransformer<T> {

    T transform(Map<String, Object> input);

}
