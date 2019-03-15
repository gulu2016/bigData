package sanbuquFiles.unit12

import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}

/**
  *
  * @ProjectName: scalaWordCount
  * @Package: sanbuquFiles.unit12
  * @ClassName: Movie_Users_Analyzer_DateFrame_12_7
  * @Description: java类作用描述 
  * @Author: gulu
  * @CreateDate: 19-2-28 下午6:28
  * @UpdateUser: 更新者
  * @UpdateDate: 19-2-28 下午6:28
  * @UpdateRemark: 更新说明
  * @Version: 1.0
  */
object Movie_Users_Analyzer_DateFrame_12_7 {
  def main(args: Array[String]): Unit = {
    val dataPath = "/home/zhangjiaqian/sanbuqu/"
    //本地执行，并设置程序名字
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD_Movie_Users_Analyzer")

    //获取sparkcontext
    val sc = new SparkContext(conf)
    //设置日志运行级别
    sc.setLogLevel("ERROR")

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
    //import spark.implicits._

    //把数据加载进来转换为RDD
    val usersRDD = sc.textFile(dataPath+"users.dat")
    val moviesRDD = sc.textFile(dataPath+"movies.dat")
    val ratingsRDD = sc.textFile(dataPath+"ratings.dat")

    //建立ratings的元数据信息
    val schemaforratings = StructType("UserID::MovieID".split("::")
      .map(column => StructField(column,StringType,true)))
      .add("Rating",DoubleType,true)
      .add("Timestamp",StringType,true)
    val ratingsRDDRows = ratingsRDD.map(_.split("::"))
      .map(line => Row(line(0).trim,line(1).trim,line(2).trim.toDouble,
        line(3).trim))
    val ratingsDataFrame = spark.createDataFrame(ratingsRDDRows,schemaforratings)

    println("通过DataFrame和RDD相结合的方式计算所有电影中平均得分醉倒的电影TopN")
    ratingsDataFrame.select("MovieID","Rating")
      .groupBy("MovieID")
      .avg("Rating").rdd
      .map(row => (row(1),(row(0),row(1))))
      .sortBy(_._1.toString.toDouble,false)
      .map(tuple => tuple._2)
      .collect()
      .take(10)
      .foreach(println)

    import spark.sqlContext.implicits._
    //上边注释的代码书上是有的，可是我加上是不行的，
    //或者注释35行，两个留一个才能运行
    println("通过纯粹使用DataFrame方式计算所有电影中平均得分最高的电影TopN:")
    ratingsDataFrame.select("MovieID","Rating")
      .groupBy("MovieID")
      .avg("Rating")
      .orderBy($"avg(Rating)".desc)
      .show(10)
  }
}
