package kafka;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.junit.Test;

import java.lang.reflect.MalformedParameterizedTypeException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @ProjectName: scalaWordCount
 * @Package: kafka
 * @ClassName: TestProducer
 * @Description: java类作用描述
 * @Author: gulu
 * @CreateDate: 19-3-18 下午12:10
 * @UpdateUser: 更新者
 * @UpdateDate: 19-3-18 下午12:10
 * @UpdateRemark: 更新说明
 * @Version: 1.0
 * 测试生产者和消费者
 */
public class TestProducer {
    @Test
    public void testSend(){
        //利用api模拟生产者
        Properties props = new Properties();
        props.put("metadata.broker.list","localhost:9092");
        props.put("serializer.class","kafka.serializer.StringEncoder");
        props.put("request.required.acks","1");

        ProducerConfig config = new ProducerConfig(props);
        Producer<String,String> producer = new Producer<String, String>(config);

        KeyedMessage<String,String> msg = new KeyedMessage<String, String>("test","100","yuanze");
        producer.send(msg);
        System.out.println("send over");
    }

    @Test
    public void testConsumer(){
        //利用api模拟消费者
        Properties props = new Properties();
        props.put("zookeeper.connect","localhost:2181");
        props.put("metadata.broker.list","localhost:9092");
        props.put("group.id","g1");
        props.put("zookeeper.session.timeout.ms","500");
        props.put("zookeeper.sync.time.ms","250");
        props.put("auto.commit.interval.ms","1000");
        props.put("auto.offset.reset","smallest");

        ProducerConfig config = new ProducerConfig(props);
        Map<String,Integer> map = new HashMap<>();
        map.put("test",1);
        Map<String, List<KafkaStream<byte[],byte[]>>> msgs =
                Consumer.createJavaConsumerConnector(new ConsumerConfig(props))
                .createMessageStreams(map);
        List<KafkaStream<byte[],byte[]>> msgList = msgs.get("test");
        for(KafkaStream<byte[],byte[]> stream:msgList){
            ConsumerIterator<byte[],byte[]> it = stream.iterator();
            while (it.hasNext()){
                byte[] message = it.next().message();
                System.out.println(new String(message));
            }
        }
    }
}
