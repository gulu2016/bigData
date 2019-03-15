package sparkStreamingDemo1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;


import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @ProjectName: scalaWordCount
 * @Package: sparkStreamingDemo1
 * @ClassName: JavaSparkStreamingWordCountApp1
 * @Description: java类作用描述
 * @Author: gulu
 * @CreateDate: 19-1-15 上午9:27
 * @UpdateUser: 更新者
 * @UpdateDate: 19-1-15 上午9:27
 * @UpdateRemark: 更新说明
 * @Version: 1.0
 */
public class JavaSparkStreamingWordCountApp1 {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf();
        conf.setAppName("wc");
        conf.setMaster("local[2]");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Seconds.apply(2));

        jsc.checkpoint("file:///home/zhangjiaqian/wordcount");
        JavaReceiverInputDStream sock = jsc.socketTextStream("localhost",9999);


        JavaDStream<String> wordsDS = sock.flatMap(new FlatMapFunction<String,String>() {
            public Iterator call(String str) throws Exception {
                List<String> list = new ArrayList<>();
                String[] arr = str.split(" ");
                for(String s:arr){
                    list.add(s);
                }
                return list.iterator();
            }
        });

        //映射成元组，标1的过程
        JavaPairDStream<String,Integer> pairDS = wordsDS.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s,1);
            }
        });

        JavaPairDStream<String,Integer> countDS = pairDS.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        },new Duration(6*1000),new Duration(4*1000));

        countDS.print();
        jsc.start();
        jsc.awaitTermination();
    }
}
