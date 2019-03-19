package sanbuquFiles.unit16;



import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.*;

/**
 * @ProjectName: scalaWordCount
 * @Package: sanbuquFiles.unit16
 * @ClassName: MockAdClickedStats
 * @Description: 模拟用户的广告点击行为，向kafka中发送点击的消息
 * @Author: gulu
 * @CreateDate: 19-3-19 上午8:50
 * @UpdateUser: 更新者
 * @UpdateDate: 19-3-19 上午8:50
 * @UpdateRemark: 更新说明
 * @Version: 1.0
 */
public class MockAdClickedStats {
    public static void main(String[] args){
        final Random random = new Random();
        final String[] provinces = new String[]{"Guangdong","Zhejiang",
        "Jiangsu","Fujian"};
        final Map<String,String[]> cities = new HashMap<>();
        cities.put("Guangdong",new String[]{"Guangzhou","Shenzhen","DongGuan"});
        cities.put("Zhejiang",new String[]{"Hangzhou","Wenzhou","Ningbo"});
        cities.put("Jiangsu",new String[]{"Nanjing","Suzhou","Wuxi"});
        cities.put("Fujian",new String[]{"Fuzhou","Xiamen","Sanming"});

        final String[] ips = new String[]{
                "192.168.112.240",
                "192.168.112.241",
                "192.168.112.242",
                "192.168.112.243",
                "192.168.112.244",
                "192.168.112.245",
                "192.168.112.246",
                "192.168.112.247",
                "192.168.112.248",
                "192.168.112.249",
                "192.168.112.250",
                "192.168.112.251",
                "192.168.112.252"};

        //kafka相关的基本配置信息
        Properties kafkaConf = new Properties();
        kafkaConf.put("serializer.class","kafka.serializer.StringEncoder");
        kafkaConf.put("metadata.broker.list","localhost:9092");
        ProducerConfig producerConfig = new ProducerConfig(kafkaConf);

        final Producer<String,String> producer = new Producer<String, String>(producerConfig);

        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true){
                    //广告的基本格式是timestamp,ip,userID,adID,province,city
                    Long timestamp = new Date().getTime();
                    String ip = ips[random.nextInt(13)];
                    int userID = random.nextInt(10000);
                    int adID = random.nextInt(100);
                    String province = provinces[random.nextInt(4)];
                    String city = cities.get(province)[random.nextInt(3)];

                    String clickedAd = timestamp+"\t"+ip+"\t"+userID+"\t"+adID+"\t"+
                            province+"\t"+city;

                    System.out.println(clickedAd);

                    //producer.send(new KeyedMessage<String,String>("AdClicked",clickedAd));

                    KeyedMessage<String,String> msg = new KeyedMessage<String, String>("AdClicked",clickedAd);
                    producer.send(msg);

                    try{
                        Thread.sleep(2000);
                    }catch (InterruptedException e){
                        e.printStackTrace();
                    }
                }
            }
        }).start();
    }
}
