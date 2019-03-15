package sanbuquFiles
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @ProjectName: Default (Template) Project
  * @Package: sanbuquFiles
  * @ClassName: sanbuqu2
  * @Description: java类作用描述 
  * @Author: gulu
  * @CreateDate: 19-1-2 上午10:59
  * @UpdateUser: 更新者
  * @UpdateDate: 19-1-2 上午10:59
  * @UpdateRemark: 更新说明
  * @Version: 1.0
  */
object sanbuqu2 {


  def main(args: Array[String]): Unit = {
    val dataPath = "/home/zhangjiaqian/sanbuqu/"
    //本地执行，并设置程序名字
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD_Movie_Users_Analyzer")

    //获取sparkcontext
    val sc = new SparkContext(conf)

    //设置日志运行级别
    sc.setLogLevel("ERROR")

    //把数据加载进来转换为RDD
    val wordCountRDD = sc.textFile(dataPath+"wordCount.txt")

    //具体数据处理的业务逻辑
    val words = wordCountRDD.flatMap(line => line.split(" "))
    val pairs = words.map(word => (word,1))
    val wordCountsOdered = pairs.reduceByKey(_+_).map(pair => (pair._2,pair._1))
      .sortByKey(false).map(pair => (pair._2,pair._1))

    wordCountsOdered.collect().foreach(wordNumberPair => println(wordNumberPair._1+":"
    +wordNumberPair._2))

    //关闭sparkSession
    sc.stop()
  }
}
