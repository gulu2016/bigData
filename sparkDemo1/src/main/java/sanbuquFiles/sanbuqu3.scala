package sanbuquFiles

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
  *
  * @ProjectName: Default (Template) Project
  * @Package: sanbuquFiles
  * @ClassName: sanbuqu3
  * @Description: java类作用描述 
  * @Author: gulu
  * @CreateDate: 19-1-2 上午10:59
  * @UpdateUser: 更新者
  * @UpdateDate: 19-1-2 上午10:59
  * @UpdateRemark: 更新说明
  * @Version: 1.0
  */
object sanbuqu3 {
  def main(args: Array[String]): Unit = {
    //
    val spark = SparkSession.builder
    .appName("StructuredNetworkCount").getOrCreate()

    import spark.implicits._

    //
    val lines = spark.readStream.format("socker")
    .option("host","localhost").option("port","9999").load()

    //
    val words = lines.as[String].flatMap(_.split(" "))
    val wordcount = words.groupBy("values").count()

    //
    val query = wordcount.writeStream
    .outputMode("complete").format("console").start()

    query.awaitTermination()

  }
}
