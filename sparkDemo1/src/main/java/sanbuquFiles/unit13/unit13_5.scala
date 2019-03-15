package sanbuquFiles.unit13

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

/**
  *
  * @ProjectName: scalaWordCount
  * @Package: sanbuquFiles.unit13
  * @ClassName: unit13_5
  * @Description: java类作用描述 
  * @Author: gulu
  * @CreateDate: 19-3-8 下午2:30
  * @UpdateUser: 更新者
  * @UpdateDate: 19-3-8 下午2:30
  * @UpdateRemark: 更新说明
  * @Version: 1.0
  */
object unit13_5 {
  case class Person(name:String,age:Long)
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("HiSpark")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder
      .enableHiveSupport
      .getOrCreate()
    import spark.implicits._
    import org.apache.spark.sql.functions._

    //生成DataFrame
    val persons = spark.read.json("sparkDemo1/src/main/java/sanbuquFiles/unit13/people.json")
    //用DataFrame生成DataSet
    val personsDS = persons.as[Person]
    //打印DataSet的结果和结构
    personsDS.show()
    personsDS.printSchema()
    val personDF = personsDS.toDF()

    println("使用dropDuplicate算子删除重复姓名（name字段重复会被删除）")
    personsDS.dropDuplicates("name").show()

    println("使用distinct算子从Dataset中删除重复记录")
    personsDS.distinct().show()

    println("使用repartition算子修改分区数量(变多)：")
    println("原分区数："+personsDS.rdd.partitions.size)
    val repartitionDS = personsDS.repartition(4)
    println("当前分区数："+repartitionDS.rdd.partitions.size)

    println("使用coalesce算子修改分区数量(变少)：")
    //书上说在本地分区数会是1,确实是1,据说在集群上是没问题的
    val coalesced = repartitionDS.coalesce(2)
    println("coalesce修改后分区数"+coalesced.rdd.partitions.size)
    coalesced.show()
  }
}
