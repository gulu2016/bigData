package sanbuquFiles.unit12.Movie_Users_Analyzer_DateFrame_12_11

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

/**
  *
  * @ProjectName: scalaWordCount
  * @Package: sanbuquFiles.unit12.Movie_Users_Analyzer_DateFrame_12_11
  * @ClassName: Movie_Users_Analyzer_DataSet
  * @Description: java类作用描述 
  * @Author: gulu
  * @CreateDate: 19-2-28 下午8:04
  * @UpdateUser: 更新者
  * @UpdateDate: 19-2-28 下午8:04
  * @UpdateRemark: 更新说明
  * @Version: 1.0
  */
object Movie_Users_Analyzer_DataSet {
  //定义用户，评分，电影case class类
  case class User(UserID:String,Gender:String,Age:String,OccupationID:String,
                  Zip_Code:String)
  case class Rating(UserID:String,MovieID:String,Rating:Double,Timestamp:String)
  case class Movie(MovieID:String,Title:String,Genres:String)
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
    val schemaforusers = StructType("UserID::Gender::Age::OccupationID::Zip_Code"
      .split("::")
      .map(column => StructField(column,StringType,true)))
    val usersRDDRows: RDD[Row] = usersRDD.map(_.split("::"))
      .map(line => Row(line(0).trim,line(1).trim,line(2).trim,line(3).trim,
        line(4).trim))
    val usersDataFrame = spark.createDataFrame(usersRDDRows,schemaforusers)
    val usersDataSet = usersDataFrame.as[User]

    //建立ratings的元数据信息
    val schemaforratings = StructType("UserID::MovieID".split("::")
      .map(column => StructField(column,StringType,true)))
      .add("Rating",DoubleType,true)
      .add("Timestamp",StringType,true)
    val ratingsRDDRows = ratingsRDD.map(_.split("::"))
      .map(line => Row(line(0).trim,line(1).trim,line(2).trim.toDouble,
        line(3).trim))
    val ratingsDataFrame = spark.createDataFrame(ratingsRDDRows,schemaforratings)
    val ratingsDataSet = ratingsDataFrame.as[Rating]

    //建立moviess的元数据信息
    val schemaformovies = StructType("MovieID::Title::Genres".split("::")
      .map(column => StructField(column,StringType,true)))
    val moviesRDDRows = moviesRDD.map(_.split("::"))
      .map(line => Row(line(0).trim,line(1).trim,line(2).trim))
    val moviessDataFrame = spark.createDataFrame(moviesRDDRows,schemaformovies)

    ratingsDataSet.filter(s"MovieID = 1193")
      .join(usersDataSet,"UserID")
      .select("Gender","Age")
      .groupBy("Gender","Age")
      .count()
      .show(10)
  }
}
