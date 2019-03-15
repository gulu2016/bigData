package wordCountDemo

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @ProjectName: Default (Template) Project
  * @Package: wordCountDemo
  * @ClassName: wordCountDemo
  * @Description: java类作用描述 
  * @Author: gulu
  * @CreateDate: 19-1-2 上午11:03
  * @UpdateUser: 更新者
  * @UpdateDate: 19-1-2 上午11:03
  * @UpdateRemark: 更新说明
  * @Version: 1.0
  */
object wordCountDemo {
  def main(args: Array[String]): Unit = {
    //创建SparkConf()对象，该对象用来配置各种spark运行的参数
    val conf = new SparkConf();
    //设置spark运行的模式，local代表本地模式
    conf.setMaster("local");
    conf.setAppName("wordcountJava.wordcount")

    //SparkContext对象是spark程序的入口点，这里传入conf配置对象
    val sc = new SparkContext(conf);

    //加载本地文件，此时集合中的元素为{[hello,world],[hello,world1],[hello,world2]}
    val rdd1 = sc.textFile("/home/zhangjiaqian/sctext")
    //将集合中的元素直接拆分为一个个单词元素
    //此时的集合为{hello,world,hello,world1,hello,world2}
    val rdd2 = rdd1.flatMap(line => line.split(" "))
    //将元素变成对偶形式
    //此时的集合为{(hello,1),(world,1),(hello,1),(world1,1),(hello,1),(world2,1)}
    val rdd3 = rdd2.map((_,1))
    //将元素聚合
    //此时的集合为{(hello,3),(world,1),(world1,1),(world2,1)}
    val rdd4 = rdd3.reduceByKey(_ + _)
    //展示结果
    val r = rdd4.collect()
    r.foreach(println)
  }
}
