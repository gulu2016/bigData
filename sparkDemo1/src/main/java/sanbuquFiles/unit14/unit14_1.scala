package sanbuquFiles.unit14

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
  *
  * @ProjectName: scalaWordCount
  * @Package: sanbuquFiles.unit14
  * @ClassName: unit14_1
  * @Description: java类作用描述 
  * @Author: gulu
  * @CreateDate: 19-3-9 上午8:44
  * @UpdateUser: 更新者
  * @UpdateDate: 19-3-9 上午8:44
  * @UpdateRemark: 更新说明
  * @Version: 1.0
  */
object unit14_1 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setMaster("local[*]").setAppName("HiSpark")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder
      .enableHiveSupport
      .getOrCreate()
    import spark.implicits._
    import org.apache.spark.sql.functions._

    val userInfo = spark.read.format("json").json("sparkDemo1/src/main/java/sanbuquFiles/unit14/user.json")
    val userLog = spark.read.format("json").json("sparkDemo1/src/main/java/sanbuquFiles/unit14/log.json")
    println("用户信息及用户访问记录文件json格式")
    userInfo.printSchema()
    userLog.printSchema()

    println("功能1：统计特定时间段(2016-1-1----2016-9-30)访问次数最多的用户top5")
    val startTime = "2016-1-1"
    val endTime = "2016-9-30"
    userLog.filter("time >= '"+startTime+"'and time <= '"+endTime+"'")
      .join(userInfo,userInfo("userID")===userLog("userID"))
      .groupBy(userInfo("userID"),userInfo("name"))
      .agg(count(userLog("logID")).alias("userLogCount"))
      .sort($"userLogCount".desc)
      .limit(5)
      .show()
  }
}
