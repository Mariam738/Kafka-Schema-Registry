package org.example.avro;

import com.example.avro.UserEvent;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
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

public class SpecificRecordAvroConsumer {

   public void consumeUserEvents() {
       Properties properties = loadProperties();
       Map<String, Object> consumerConfigs = new HashMap<>();
       properties.forEach((key, value) -> consumerConfigs.put((String) key, value));
       consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, "specific-avro-consumer");
       consumerConfigs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

       consumerConfigs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
       consumerConfigs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
       consumerConfigs.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true );

       try(Consumer<String, UserEvent> consumer = new KafkaConsumer<>(consumerConfigs)){
           consumer.subscribe(Collections.singletonList("user-events-avro"));
           while (true) {
               ConsumerRecords<String, UserEvent> consumerRecords = consumer.poll(Duration.ofSeconds(2));
               consumerRecords.forEach(consumerRecord -> {
                   UserEvent user = consumerRecord.value();
                   System.out.print("User details { ");
//                   System.out.printf("Id: %s, ", user.getUserId());
                   System.out.printf("Email: %s, ", user.getEmail());
                   System.out.printf("Age: %d, ", user.getAge());
                   System.out.printf("Active: %b } %n", user.getActive());
                   System.out.printf("Online: %b } %n", user.getOnline());
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
       SpecificRecordAvroConsumer eventConsumer = new SpecificRecordAvroConsumer();
       eventConsumer.consumeUserEvents();
   }
}
