package sanbuquFiles.unit14.unit14_3

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
  *
  * @ProjectName: scalaWordCount
  * @Package: sanbuquFiles.unit14.unit14_3
  * @ClassName: unit14_3_4
  * @Description: java类作用描述 
  * @Author: gulu
  * @CreateDate: 19-3-10 上午9:25
  * @UpdateUser: 更新者
  * @UpdateDate: 19-3-10 上午9:25
  * @UpdateRemark: 更新说明
  * @Version: 1.0
  */
object unit14_3_4 {
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


  }
}
