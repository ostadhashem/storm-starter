package storm.starter.tools;

import kafka.producer.KeyedMessage;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
import java.util.Properties;


public class KafkaProducer {

    private final Producer<String, String> producer;
    private final String topic;

    public KafkaProducer(Properties properties, String topicName) {
        this.topic = topicName;
        ProducerConfig config = new ProducerConfig(properties);
        producer = new Producer<String, String>(config);
    }

    public void sendMessage(String word, Integer count){
        KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, word, count.toString());
        producer.send(data);
    }

}
