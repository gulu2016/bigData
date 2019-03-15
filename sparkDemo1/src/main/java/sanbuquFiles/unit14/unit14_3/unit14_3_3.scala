package sanbuquFiles.unit14.unit14_3

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
  *
  * @ProjectName: scalaWordCount
  * @Package: sanbuquFiles.unit14.unit14_3
  * @ClassName: unit14_3_3
  * @Description: java类作用描述 
  * @Author: gulu
  * @CreateDate: 19-3-10 上午9:13
  * @UpdateUser: 更新者
  * @UpdateDate: 19-3-10 上午9:13
  * @UpdateRemark: 更新说明
  * @Version: 1.0
  */
object unit14_3_3 {
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

    println("统计特定时段消费金额增长最多的Top5")
    //这里把上周的消费置为-log.consumed,这周访问次数置为log.consumed,两个求和就可以算出增长数量
    val userAccessTemp1 = userLog.as[UserLog]
      .filter("time >='2016-1-10' and time <= '2016-2-10' and typed='1'")
      //这一步相当于投影映射，将userLog中的某些字段投影为ConsumedOnce字段
      .map(log => ConsumedOnce(log.logID,log.userID,log.consumed))
    val userAccessTemp2 = userLog.as[UserLog]
      .filter("time >='2016-2-10' and time <= '2016-3-10' and typed='1'")
      .map(log => ConsumedOnce(log.logID,log.userID,-log.consumed))
    val userAccessTemp = userAccessTemp1.union(userAccessTemp2)

    userAccessTemp.join(userInfo,userInfo("userID")===userAccessTemp("userID"))
      .groupBy(userInfo("userID"),userInfo("name"))
      .agg(sum(userAccessTemp("count")).alias("consumeIncreasedTmp"))
      .sort($"consumeIncreasedTmp".desc)
      .limit(10)
      .show()
  }
}
