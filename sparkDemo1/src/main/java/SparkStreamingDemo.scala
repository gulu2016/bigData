import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._

object SparkStreamingDemo {
    def main(args: Array[String]): Unit = {
        //local[n] n > 1
        val conf = new SparkConf().setMaster("local").setAppName("NetworkWordCount")
        //创建Spark流上下文,批次时长是1s
        val ssc = new StreamingContext(conf, Seconds(1))

        //创建socket文本流
        val lines = ssc.socketTextStream("localhost", 9999)
        //压扁
        val words = lines.flatMap(_.split(" "))
        //变换成对偶
        val pairs = words.map((_,1));

        val count = pairs.reduceByKey(_+_) ;
        count.print()

        //启动
        ssc.start()

        //等待结束
        ssc.awaitTermination()
    }
}
