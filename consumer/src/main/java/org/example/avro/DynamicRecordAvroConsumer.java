package org.example.avro;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class DynamicRecordAvroConsumer {

    public void consumeUserEvents() {
        Properties properties = loadProperties();
        Map<String, Object> consumerConfigs = new HashMap<>();
        properties.forEach((key, value) -> consumerConfigs.put((String) key, value));
        consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, "dynamic-avro-consumer");
        consumerConfigs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        consumerConfigs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfigs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        consumerConfigs.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false );

        try(Consumer<String, GenericRecord> consumer = new KafkaConsumer<>(consumerConfigs)){
            consumer.subscribe(Collections.singletonList("user-events-avro"));
            while (true) {
                ConsumerRecords<String, GenericRecord> consumerRecords = consumer.poll(Duration.ofSeconds(2));
                consumerRecords.forEach(consumerRecord -> {
                    GenericRecord user = consumerRecord.value();
//                    System.out.print("User details { ");
                    System.out.printf("Id: %s, ", user.get("userId"));
                    System.out.printf("Email: %s, ", user.get("email"));
                    System.out.printf("Age: %d, ", user.get("age"));
//                    System.out.printf("Active: %b } %n", user.get("active"));
                    System.out.printf("Online: %b } %n", user.get("online"));


                    System.out.print("Dynamic Loop   -> { ");
                    for (org.apache.avro.Schema.Field field : user.getSchema().getFields()) {
                        System.out.printf("%s: %s, ", field.name(), user.get(field.name()).toString());
                    }
                    System.out.println(" }");
                });

            }
        }
    }

    Properties loadProperties() {
        try (InputStream inputStream = this.getClass()
                .getClassLoader()
                .getResourceAsStream("confluent.properties")) {
            Properties props = new Properties();
            props.load(inputStream);
            return props;
        } catch (IOException exception) {
            throw new RuntimeException(exception);
        }
    }

    public static void main(String[] args) {
        DynamicRecordAvroConsumer eventConsumer = new DynamicRecordAvroConsumer();
        eventConsumer.consumeUserEvents();
    }
}
