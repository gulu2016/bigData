package sanbuquFiles.unit12

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

/**
  *
  * @ProjectName: scalaWordCount
  * @Package: sanbuquFiles.unit12
  * @ClassName: Movie_Users_Analyzer_DateFrame_12_9
  * @Description: java类作用描述 
  * @Author: gulu
  * @CreateDate: 19-2-28 下午7:31
  * @UpdateUser: 更新者
  * @UpdateDate: 19-2-28 下午7:31
  * @UpdateRemark: 更新说明
  * @Version: 1.0
  */
object Movie_Users_Analyzer_DateFrame_12_9 {
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

    println("纯粹使用DataFrame实现所有电影中最受男性喜爱的电影Top10")
    val genderRatingsDataFrame = ratingsDataFrame.join(usersDataFrame,"UserID")
      .cache()

    val maleFilteredRatingsDataFrame = genderRatingsDataFrame.filter("Gender='M'")
      .select("MovieID","Rating")
    maleFilteredRatingsDataFrame.groupBy("MovieID").avg("Rating")
      .orderBy($"avg(Rating)".desc).show(10)

    println("纯粹使用DataFrame实现所有电影中最受女性喜爱的电影Top10")
    val femaleFilteredRatingsDataFrame = genderRatingsDataFrame.filter("Gender='F'")
      .select("MovieID","Rating")
    femaleFilteredRatingsDataFrame.groupBy("MovieID").avg("Rating")
      .orderBy($"avg(Rating)".desc).show(10)
  }
}
