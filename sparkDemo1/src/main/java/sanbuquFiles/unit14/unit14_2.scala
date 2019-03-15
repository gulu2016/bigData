package sanbuquFiles.unit14

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
  *
  * @ProjectName: scalaWordCount
  * @Package: sanbuquFiles.unit14
  * @ClassName: unit14_2
  * @Description: java类作用描述 
  * @Author: gulu
  * @CreateDate: 19-3-9 下午4:06
  * @UpdateUser: 更新者
  * @UpdateDate: 19-3-9 下午4:06
  * @UpdateRemark: 更新说明
  * @Version: 1.0
  */
object unit14_2 {
  case class UserLog(logID:Long,userID:Long,time:String,typed:Long,consumed:Double)
  case class LogOnce(logID:Long,userID:Long,count:Long)
  case class ConsumedOnce(logID:Long,userID:Long,consumed:Double)
  def main(args: Array[String]): Unit = {
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

    println("功能二：统计特定时段用户购买进而排名TopN")
    val startTime = "2016-1-1"
    val endTime = "2016-9-30"
    //round(2)函数是保留两位小数的意思
    userLog.filter("time>='"+startTime+"'and time <='"+endTime+"'")
      .join(userInfo,userInfo("userID")===userLog("userID"))
      .groupBy(userInfo("userID"),userInfo("name"))
      .agg(round(sum(userLog("consumed")),2).alias("totalCount"))
      .sort($"totalCount".desc)
      .limit(10)
      .show()

    println("功能三：统计特定时间段内用户访问次数增长排名TopN:")
    val userAccessTemp1 = userLog.as[UserLog]
      .filter("time >='2016-1-10' and time <= '2016-1-20' and typed='0'")
      .map(log => LogOnce(log.logID,log.userID,1))
    val userAccessTemp2 = userLog.as[UserLog]
      .filter("time >='2016-2-10' and time <= '2016-2-20' and typed='0'")
      .map(log => LogOnce(log.logID,log.userID,-1))
    val userAccessTemp = userAccessTemp1.union(userAccessTemp2)

    userAccessTemp.join(userInfo,userInfo("userID")===userAccessTemp("userID"))
      .groupBy(userInfo("userID"),userInfo("name"))
      .agg(sum(userAccessTemp("count")).alias("viewIncreasedTmp"))
      .sort($"viewIncreasedTmp".desc)
      .limit(10)
      .show()
  }
}
