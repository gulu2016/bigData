package sanbuquFiles.unit13

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
  *
  * @ProjectName: scalaWordCount
  * @Package: sanbuquFiles.unit13
  * @ClassName: unit13_6
  * @Description: java类作用描述 
  * @Author: gulu
  * @CreateDate: 19-3-8 下午2:55
  * @UpdateUser: 更新者
  * @UpdateDate: 19-3-8 下午2:55
  * @UpdateRemark: 更新说明
  * @Version: 1.0
  */
object unit13_6 {
  case class Person(name:String,age:Long)
  case class PersonScore(n:String,score:Long)
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

    //生成DataFrame
    val personScore = spark.read.json("sparkDemo1/src/main/java/sanbuquFiles/unit13/propleScores.json")
    //用DataFrame生成DataSet
    val personScoreDS = personScore.as[PersonScore]

    println("使用sort算子对年龄进行降序排序")
    personsDS.sort($"age".desc).show()

    println("使用join算子关联企业人员信息、企业人员分数评分信息")
    personsDS.join(personScoreDS, col("name") === col("n")).show()

    println("使用joinWith算子关联企业人员信息、企业人员分数评分信息")
    personsDS.joinWith(personScoreDS, col("name") === col("n")).show()
  }
}
