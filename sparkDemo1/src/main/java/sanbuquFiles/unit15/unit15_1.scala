package sanbuquFiles.unit15

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


/**
  *
  * @ProjectName: scalaWordCount
  * @Package: sanbuquFiles.unit15
  * @ClassName: unit15_1
  * @Description: java类作用描述 
  * @Author: gulu
  * @CreateDate: 19-3-11 上午9:18
  * @UpdateUser: 更新者
  * @UpdateDate: 19-3-11 上午9:18
  * @UpdateRemark: 更新说明
  * @Version: 1.0
  */
object unit15_1 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setMaster("local[*]").setAppName("NBAPlayer_Analyzer_DateSet")
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
    val sc = spark.sparkContext


  }
}
