package sanbuquFiles.unit13

import java.io.File

import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext

/**
  *
  * @ProjectName: scalaWordCount
  * @Package: sanbuquFiles.unit13
  * @ClassName: unit13
  * @Description: 使用spark操作hive,这里hive是spark自带的
  * @Author: gulu
  * @CreateDate: 19-3-7 下午3:18
  * @UpdateUser: 更新者
  * @UpdateDate: 19-3-7 下午3:18
  * @UpdateRemark: 更新说明
  * @Version: 1.0
  */
object unit13_3_2 {
  case class Person(name:String,age:Long)
  def main(args: Array[String]): Unit = {
    //System.setProperty("spark.sql.warehouse.dir","/usr/local/hive/warehouse")
    val conf = new SparkConf().setMaster("local[*]").setAppName("HiSpark")
//    val spark = SparkSession
//      .builder()
//      .config(conf)
//      .config("spark.sql.warehouse.dir","/usr/local/hive/warehouse")
//      .enableHiveSupport()
//      .getOrCreate()
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder
      .enableHiveSupport
      .getOrCreate()

    //创建表语句和查询语句
    spark.sql("drop table if exists src1")
    spark.sql("create table if not exists src1(key int,value string)row format delimited fields terminated by ',' stored  as textfile")
    spark.sql("load data local inpath '/home/zhangjiaqian/IdeaProjects/scalaWordCount/sparkDemo1/src/main/java/sanbuquFiles/unit13/v1.txt'" +
      "into table src1")
    spark.sql("select * from src1").show()
    spark.sql("select count(*) from src1").show()
  }
}
