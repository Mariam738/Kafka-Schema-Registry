package org.example.avro;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
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


public class DynamicRecordAvroProducer {
    private final Random random = new Random();

    public void produceUserEvents() {
        Properties properties = loadProperties();
        Map<String, Object> producerConfigs = new HashMap<>();
        properties.forEach((key, value) -> producerConfigs.put((String) key, value));

        producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        producerConfigs.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, true);


        System.out.printf("Producer now configured for using SchemaRegistry %n");
        try (final Producer<String, GenericRecord> producer = new KafkaProducer<>(producerConfigs)) {

            Schema schema = new Schema.Parser().parse(Files.newInputStream(Paths.get("src/main/avro/user_event.avsc")));

            String topic = "user-events-avro";
            List<GenericRecord> userEvents = new ArrayList<>();

            GenericRecord user = getUserObject(schema);
            GenericRecord userII = getUserObject(schema);

            userEvents.add(user);
            userEvents.add(userII);

            userEvents.forEach(event -> producer.send(new ProducerRecord<>(topic, event.get("userId").toString(), event), ((metadata, exception) -> {
                if (exception != null) {
                    System.err.printf("Producing %s resulted in error %s %n", event, exception);
                } else {
                    System.out.printf("Produced record at offset %s with timestamp %d %n", metadata.offset(), metadata.timestamp());
                }
            })));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    GenericRecord getUserObject(Schema schema) {
        return new GenericRecordBuilder(schema)
                .set("userId", "U" + random.nextInt(1000))
                .set("email", "user@gmail.com")
                .set("age", random.nextInt(80))
                .set("active", true)
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
        DynamicRecordAvroProducer eventProducer = new DynamicRecordAvroProducer();
        eventProducer.produceUserEvents();
    }

}
