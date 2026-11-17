package org.example.proto;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;


public class DynamicRecordProtoProducer {
    private final Random random = new Random();

    public void produceUserEvents() {
        Properties properties = loadProperties();
        Map<String, Object> producerConfigs = new HashMap<>();
        properties.forEach((key, value) -> producerConfigs.put((String) key, value));

        producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class);
        producerConfigs.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, true);


        System.out.printf("Producer now configured for using SchemaRegistry %n");
        try (final Producer<String, DynamicMessage> producer = new KafkaProducer<>(producerConfigs)) {

            ProtobufSchema schema = new ProtobufSchema(new String (Files.readAllBytes(Paths.get("src/main/proto/user_event.proto"))));
            Descriptors.Descriptor descriptor= schema.toDescriptor();


            String topic = "user-events-proto";
            List<DynamicMessage> userEvents = new ArrayList<>();

            DynamicMessage user = getUserObject(descriptor);
            DynamicMessage userII = getUserObject(descriptor);

            userEvents.add(user);
            userEvents.add(userII);

            userEvents.forEach(event -> producer.send(new ProducerRecord<>(topic, event.getField(descriptor.findFieldByName("user_id")).toString(), event), ((metadata, exception) -> {
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

    DynamicMessage getUserObject(Descriptors.Descriptor descriptor) {
        return  DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("user_id"), "U" + random.nextInt(1000))
                .setField(descriptor.findFieldByName("email"),"user@gmail.com")
                .setField(descriptor.findFieldByName("age"), random.nextInt(80))
                .setField(descriptor.findFieldByName("active"),true)
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
        DynamicRecordProtoProducer eventProducer = new DynamicRecordProtoProducer();
        eventProducer.produceUserEvents();
    }

}
