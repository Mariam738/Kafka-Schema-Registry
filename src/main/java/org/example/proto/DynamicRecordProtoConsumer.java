package org.example.proto;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
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

public class DynamicRecordProtoConsumer {

    public void consumeUserEvents() {
        Properties properties = loadProperties();
        Map<String, Object> consumerConfigs = new HashMap<>();
        properties.forEach((key, value) -> consumerConfigs.put((String) key, value));
        consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, "schema-registry-consumer-proto");
        consumerConfigs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        consumerConfigs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfigs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaProtobufDeserializer.class);

        try(Consumer<String, DynamicMessage> consumer = new KafkaConsumer<>(consumerConfigs)){
            consumer.subscribe(Collections.singletonList("user-events-proto"));
            while (true) {
                ConsumerRecords<String, DynamicMessage> consumerRecords = consumer.poll(Duration.ofSeconds(2));
                consumerRecords.forEach(consumerRecord -> {
                    DynamicMessage user = consumerRecord.value();

                    Descriptors.Descriptor descriptor = user.getDescriptorForType();
                    Object userId = user.getField(descriptor.findFieldByName("user_id"));
                    Object email = user.getField(descriptor.findFieldByName("email"));
                    Object age = user.getField(descriptor.findFieldByName("age"));
                    Object active = user.getField(descriptor.findFieldByName("active"));

                    System.out.printf("User details { Id: %s, Email: %s, Age: %s, Active: %s } %n",
                            userId, email, age, active);


                    System.out.println("Dynamic loop: ");
                    Map<Descriptors.FieldDescriptor, Object> fields = user.getAllFields();

                    for (Map.Entry<Descriptors.FieldDescriptor, Object> entry : fields.entrySet()) {
                        String fieldName = entry.getKey().getName();
                        Object fieldValue = entry.getValue();

                        System.out.printf(" %s = %s, ", fieldName, fieldValue);
                    }
                    System.out.println();

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
        DynamicRecordProtoConsumer eventConsumer = new DynamicRecordProtoConsumer();
        eventConsumer.consumeUserEvents();
    }
}


