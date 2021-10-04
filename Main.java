import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Arrays;
import java.util.Properties;

public class Main {
    public static void main(String[] args) {
        System.out.println("Hello World");
        writeMessage();
    }

    // Write message String-String
    public static void writeMessage() {
        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", Const.BOOTSTRAP_SERVER);
        consumerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        consumerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        String topic = "test";
        int wait = 500;
        Producer<String, String> producer = new KafkaProducer<>(consumerProps);

        ProducerRecord<String, String> record = new ProducerRecord<>(topic, "test1", "this is test");
        producer.send(record);
        producer.flush();


    }

    public static void readMessage() {
        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", Const.BOOTSTRAP_SERVER);
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "group2");

        Consumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Arrays.asList("test"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
        }
    }

}
