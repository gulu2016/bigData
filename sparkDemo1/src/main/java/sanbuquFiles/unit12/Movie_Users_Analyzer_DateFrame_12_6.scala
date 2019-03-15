package sanbuquFiles.unit12

import org.apache.avro.generic.GenericData
import org.apache.calcite.avatica.ColumnMetaData
import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}




/**
  *
  * @ProjectName: scalaWordCount
  * @Package: sanbuquFiles.unit12
  * @ClassName: Movie_Users_Analyzer_DateFrame_12_6
  * @Description: java类作用描述 
  * @Author: gulu
  * @CreateDate: 19-2-27 下午4:41
  * @UpdateUser: 更新者
  * @UpdateDate: 19-2-27 下午4:41
  * @UpdateRemark: 更新说明
  * @Version: 1.0
  */
object Movie_Users_Analyzer_DateFrame_12_6 {
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


    //使用sparkSQL
    ratingsDataFrame.filter(s"MovieID=1193")
      .join(usersDataFrame,"UserID")
      .select("Gender","Age")
      .groupBy("Gender","Age")
      .count()
      .show(10)

    println("功能二：用GlobalTempView的SQL语句实现某特定电影观看者中男性和女性不同年两分别有多少人")
    ratingsDataFrame.createGlobalTempView("ratings")
    usersDataFrame.createGlobalTempView("users")

    spark.sql("select Gender,Age,count(*) " +
      "from global_temp.users u join global_temp.ratings as r on u.UserID = r.UserID " +
      "where MovieID = 1193 group by Gender,Age").show(10)

    println("功能二：用LocalTempView的SQL语句实现某特定电影观看者中男性和女性不同年两分别有多少人")
    ratingsDataFrame.createTempView("ratings")
    usersDataFrame.createTempView("users")

    spark.sql("select Gender,Age,count(*) " +
      "from global_temp.users u join global_temp.ratings as r on u.UserID = r.UserID " +
      "where MovieID = 1193 group by Gender,Age").show(10)
  }
}
