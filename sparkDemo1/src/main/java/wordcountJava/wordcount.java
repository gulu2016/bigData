package wordcountJava;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class wordcount {
    public static void main(String[] args){
        //SparkConf对象是设置spark运行的各种参数的
        SparkConf conf = new SparkConf();
        //spark运行程序的名称
        conf.setAppName("wordcountJava");
        //设置运行模式，local是本地模式
        conf.setMaster("local");

        //JavaSparkContext是java版的上下文对象
        JavaSparkContext sc = new JavaSparkContext(conf);
        //加载本地文件，此时集合中的元素为{[hello,world],[hello,world1],[hello,world2]}
        JavaRDD<String> rdd1 = sc.textFile("/home/zhangjiaqian/sctext");
        //将集合中的元素直接拆分为一个个单词元素
        //此时的集合为{hello,world,hello,world1,hello,world2}
        //这里使用了匿名内部类和接口回调机制，具体还要研究
        //问题：将FlatMapFunction返回给flatMap作用是什么
        JavaRDD<String> rdd2 = rdd1.flatMap(
                //内部类，相当于在创建FlatMapFunction对象之后执行call方法，再返回给rdd1.flatMap
                //FlatMapFunction[T, U]
            new FlatMapFunction<String, String>() {
                //call方法封装核心方法
                //Iterator<R> call(T t) throws Exception;
            public Iterator<String> call(String s) {
                List<String> list = new ArrayList<String>();
                String[] arr = s.split(" ");
                for(String ss:arr){
                    list.add(ss);
                }
                return list.iterator();
            }
        });
        //将元素变成对偶形式
        //此时的集合为{(hello,1),(world,1),(hello,1),(world1,1),(hello,1),(world2,1)}
        JavaPairRDD<String,Integer> rdd3 = rdd2.mapToPair(
                //这是要传递给mapToPair函数的参数：PairFunction[T, K2, V2]
                new PairFunction<String, String, Integer>() {
                //这是PairFunction抽象类中要实现的接口：Tuple2<K, V> call(T t) throws Exception;
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s,1);
            }
        });
        //将元素聚合
        //此时的集合为{(hello,3),(world,1),(world1,1),(world2,1)}
        JavaPairRDD<String,Integer> rdd4 = rdd3.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        });
        //展示结果
        List<Tuple2<String,Integer>> list = rdd4.collect();
        for(Tuple2<String,Integer> t:list){
            System.out.println(t._1()+":"+t._2());
        }
    }
}
