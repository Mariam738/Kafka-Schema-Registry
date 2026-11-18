package org.example.proto;

import com.example.kafka.protobuf.UserEvent;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import org.apache.avro.Schema;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;


public class SpecificRecordProtoProducer {
    private final Random random = new Random();

    public void produceUserEvents() {
        Properties properties = loadProperties();
        Map<String, Object> producerConfigs = new HashMap<>();
        properties.forEach((key, value) -> producerConfigs.put((String) key, value));

        producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class);
        producerConfigs.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, true);


        System.out.printf("Producer now configured for using SchemaRegistry %n");
        try (final Producer<String, UserEvent> producer = new KafkaProducer<>(producerConfigs)) {


            String topic = "user-events-proto";
            List<UserEvent> userEvents = new ArrayList<>();

            UserEvent user = getUserObject();
            UserEvent userII = getUserObject();

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

    UserEvent getUserObject() {
        return  UserEvent.newBuilder()
                .setUserId("U" + random.nextInt(1000))
                .setEmail("user@gmail.com")
                .setAge(random.nextInt(80))
//                .setActive(true)
                .setCountry("EGP")
                .build();
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
        SpecificRecordProtoProducer eventProducer = new SpecificRecordProtoProducer();
        eventProducer.produceUserEvents();
    }

}
