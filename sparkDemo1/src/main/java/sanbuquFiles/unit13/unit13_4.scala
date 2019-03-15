package sanbuquFiles.unit13

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

/**
  *
  * @ProjectName: scalaWordCount
  * @Package: sanbuquFiles.unit13
  * @ClassName: unit13_4
  * @Description: java类作用描述 
  * @Author: gulu
  * @CreateDate: 19-3-8 下午1:48
  * @UpdateUser: 更新者
  * @UpdateDate: 19-3-8 下午1:48
  * @UpdateRemark: 更新说明
  * @Version: 1.0
  * 1.map函数会对每一条输入进行指定的操作，然后为每一条输入返回一个对象
  *   而flatMap函数则是两个操作的集合——正是“先映射后扁平化”：
  *   操作1：同map函数一样：对每一条输入进行指定的操作，然后为每一条输入返回一个对象
  *   操作2：最后将所有对象合并为一个对象
  * 2.map和mapPartition区别（https://blog.csdn.net/wyqwilliam/article/details/82110897）
  *   总结就是，map对RDD中每条数据进行操作，mapPartition对RDD每个分区进行操作
  */
object unit13_4 {
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

    //使用map算子将age字段的值增加100
    personsDS.map(person =>
      (person.name,person.age+100L)).show()
    //使用flatMap对personDS进行转换，匹配模式如果姓名为zhang,则年龄加100
    //其他员工年龄加10
    personsDS.map(persons => persons match {
      case Person(name, age) if(name == "Zhang") => List((name,age+100))
      case Person(name, age) => List((name,age+30))
    }).show()

    personsDS.flatMap(persons => persons match {
      case Person(name, age) if(name == "Zhang") => List((name,age+100))
      case Person(name, age) => List((name,age+30))
    }).show()
    //使用mapPartitions算子对每个分区的记录进行遍历
    personsDS.mapPartitions( persons => {val result = ArrayBuffer[(String,Long)]()
      while (persons.hasNext){
        val person = persons.next()
        result += ((person.name,person.age+1000))
      }
      result.iterator
    }
    ).show()
  }
}
