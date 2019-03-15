package sanbuquFiles.unit12

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

/**
  *
  * @ProjectName: scalaWordCount
  * @Package: sanbuquFiles.unit12
  * @ClassName: Movie_Users_Analyzer_DateFrame_12_10
  * @Description: java类作用描述 
  * @Author: gulu
  * @CreateDate: 19-2-28 下午7:47
  * @UpdateUser: 更新者
  * @UpdateDate: 19-2-28 下午7:47
  * @UpdateRemark: 更新说明
  * @Version: 1.0
  */
object Movie_Users_Analyzer_DateFrame_12_10 {
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
    import spark.implicits._

    //把数据加载进来转换为RDD
    val usersRDD = sc.textFile(dataPath+"users.dat")
    val moviesRDD = sc.textFile(dataPath+"movies.dat")
    val ratingsRDD = sc.textFile(dataPath+"ratings.dat")

    //下面三块相当于建表的过程
    //建立users数据的元数据信息
    val schemaforusers = StructType("UserID::Gender::Age::OccupationID::Zip-code"
      .split("::")
      .map(column => StructField(column,StringType,true)))
    val usersRDDRows: RDD[Row] = usersRDD.map(_.split("::"))
      .map(line => Row(line(0).trim,line(1).trim,line(2).trim,line(3).trim,
        line(4).trim))
    val usersDataFrame = spark.createDataFrame(usersRDDRows,schemaforusers)

    //建立ratings的元数据信息
    val schemaforratings = StructType("UserID::MovieID".split("::")
      .map(column => StructField(column,StringType,true)))
      .add("Rating",DoubleType,true)
      .add("Timestamp",StringType,true)
    val ratingsRDDRows = ratingsRDD.map(_.split("::"))
      .map(line => Row(line(0).trim,line(1).trim,line(2).trim.toDouble,
        line(3).trim))
    val ratingsDataFrame = spark.createDataFrame(ratingsRDDRows,schemaforratings)

    //建立moviess的元数据信息
    val schemaformovies = StructType("MovieID::Title::Genres".split("::")
      .map(column => StructField(column,StringType,true)))
    val moviesRDDRows = moviesRDD.map(_.split("::"))
      .map(line => Row(line(0).trim,line(1).trim,line(2).trim))
    val moviessDataFrame = spark.createDataFrame(moviesRDDRows,schemaformovies)

    println("纯粹使用DataFrame方式实现所有电影中QQ或者微信核心目标用户最喜爱电影TopN")
    ratingsDataFrame.join(usersDataFrame,"UserID")
      .filter("Age = '18'")
      .groupBy("MovieID")
      .count()
      .orderBy($"count".desc).printSchema()
    ratingsDataFrame.join(usersDataFrame,"UserID")
      .filter("Age = '18'")
      .groupBy("MovieID")
      .count()
      .join(moviessDataFrame,"MovieID")
      .select("Title","count")
      .orderBy($"count".desc)
      .show(10)

    println("纯粹使用DataFrame方式实现所有电影中淘宝核心目标用户最喜爱电影TopN")
    ratingsDataFrame.join(usersDataFrame,"UserID")
      .filter("Age = '25'")
      .groupBy("MovieID")
      .count()
      .join(moviessDataFrame,"MovieID")
      .select("Title","count")
      .orderBy($"count".desc)
      .show(10)
  }
}
