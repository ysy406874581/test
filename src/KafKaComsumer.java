import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class KafKaComsumer extends Thread {
    private String topic;
    public KafKaComsumer(String topic) {
        super();
        this.topic = topic;
    }

    @Override
    public void run() {
        
        
        
        ConsumerConnector consumer = createConsumer();
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();// 指定topic名称
        topicCountMap.put(topic, 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> retStreams = consumer.createMessageStreams(topicCountMap);// messageStreams
        KafkaStream<byte[], byte[]> kafkaStream = retStreams.get(topic).get(0);// 获取指定的topic
        System.out.println(retStreams.get(topic).size() + "----" + kafkaStream.clientId());
        ConsumerIterator<byte[], byte[]> it = kafkaStream.iterator();
        while (it.hasNext()) {
            try {
                byte[] message = it.next().message();
                System.out.println("receive message is " + new String(message, "UTF-8"));
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        }
    }

    private ConsumerConnector createConsumer() {
        Properties consumerPros = new Properties();// 替换linux上的默认Properties 配置
        consumerPros.setProperty("zookeeper.connect", "192.168.75.128:2181");
        consumerPros.setProperty("group.id", "test1");
        return Consumer.createJavaConsumerConnector(new ConsumerConfig(consumerPros));
    }

    public static void main(String[] args) {
        new KafKaComsumer("yangshengyong").start();
    }

}
