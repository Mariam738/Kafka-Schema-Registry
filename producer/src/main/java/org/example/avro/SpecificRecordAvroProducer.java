package org.example.avro;

import com.example.avro.UserEvent;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;


public class SpecificRecordAvroProducer {
    private final Random random = new Random();

    public void produceUserEvents() {
        UserEvent.Builder userBuilder = UserEvent.newBuilder();
        Properties properties = loadProperties();
        Map<String, Object> producerConfigs = new HashMap<>();
        properties.forEach((key, value) -> producerConfigs.put((String) key, value));

        producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        producerConfigs.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, true);


        System.out.printf("Producer now configured for using SchemaRegistry %n");
        try (final Producer<String, UserEvent> producer = new KafkaProducer<>(producerConfigs)) {
            String topic = "user-events-avro";
            List<UserEvent> userEvents = new ArrayList<>();
            UserEvent user = getuserObject(userBuilder);
            UserEvent userII = getuserObject(userBuilder);

            userEvents.add(user);
            userEvents.add(userII);

            userEvents.forEach(event -> producer.send(new ProducerRecord<>(topic, event.getUserId().toString(), event), ((metadata, exception) -> {
                if (exception != null) {
                    System.err.printf("Producing %s resulted in error %s %n", event, exception);
                } else {
                    System.out.printf("Produced record at offset %s with timestamp %d %n", metadata.offset(), metadata.timestamp());
                }
            })));
        }
    }

    UserEvent getuserObject(UserEvent.Builder userBuilder) {
        userBuilder.clearUserId().clearEmail().clearAge().clearCountry();
        userBuilder.setUserId("U" + random.nextInt(1000))
                .setEmail("user@gmail.com")
                .setAge(random.nextInt(80))
//                .setActive(true);
                .setCountry("Egypt");
        return userBuilder.build();
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
        SpecificRecordAvroProducer eventProducer = new SpecificRecordAvroProducer();
        eventProducer.produceUserEvents();
    }

}
