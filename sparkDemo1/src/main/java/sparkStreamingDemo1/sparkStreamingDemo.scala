package sparkStreamingDemo1

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  *
  * @ProjectName: scalaWordCount
  * @Package: sparkStreamingDemo1
  * @ClassName: sparkStreamingDemo
  * @Description: java类作用描述 
  * @Author: gulu
  * @CreateDate: 19-1-9 下午8:22
  * @UpdateUser: 更新者
  * @UpdateDate: 19-1-9 下午8:22
  * @UpdateRemark: 更新说明
  * @Version: 1.0
  */
object sparkStreamingDemo {
  def main(args: Array[String]): Unit = {
    //注意local[n]的n要大于1,因为一个线程接受流数据，其他线程计算
    val conf = new SparkConf().setMaster("local[2]").setAppName("networkWordCount");

    //创建流上下文，批次时长是1s
    val ssc = new StreamingContext(conf,Seconds(5));

    //创建文本流，流上下文ssc就是处理文本流lines的
    val lines = ssc.socketTextStream("localhost",9999)

    //压扁
    val words = lines.flatMap(_.split(" "))

    val pairs = words.map((_,1))

    val count = pairs.reduceByKey(_+_)
    count.print()

    //开始计算和等待结束
    ssc.start()
    ssc.awaitTermination()

    //要先启动终端输入nc -lk 9999,之后再运行程序，在终端中输入字符，程序会每5s算一次
  }
}
