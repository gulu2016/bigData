package sanbuquFiles.unit13

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
  *
  * @ProjectName: scalaWordCount
  * @Package: sanbuquFiles.unit13
  * @ClassName: unit13_7
  * @Description: java类作用描述 
  * @Author: gulu
  * @CreateDate: 19-3-8 下午3:27
  * @UpdateUser: 更新者
  * @UpdateDate: 19-3-8 下午3:27
  * @UpdateRemark: 更新说明
  * @Version: 1.0
  */
object unit13_7 {
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

    println("使用randomSplit算子进行随机切分：")
    //按照0.3:0.7的权重划分，将personsDS随机分为两个dataset
    personsDS.randomSplit(Array(0.3,0.7))
      .foreach(dataset => dataset.show())

    println("使用sample算计进行随机采样")
    //false表示没有放回的抽样，0.5表示采样率是0.5
    personsDS.sample(false,0.5).show()

    println("使用select算子选择列")
    personsDS.select("name").show()
  }
}
