package sanbuquFiles
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @ProjectName: Default (Template) Project
  * @Package: sanbuquFiles
  * @ClassName: sanbuqu
  * @Description: java类作用描述 
  * @Author: gulu
  * @CreateDate: 19-1-2 上午10:59
  * @UpdateUser: 更新者
  * @UpdateDate: 19-1-2 上午10:59
  * @UpdateRemark: 更新说明
  * @Version: 1.0
  */
object sanbuqu {
  def main(args: Array[String]): Unit = {
    val dataPath = "/home/zhangjiaqian/sanbuqu/"
    //本地执行，并设置程序名字
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD_Movie_Users_Analyzer")

    //获取sparkcontext
    val sc = new SparkContext(conf)

    //设置日志运行级别
    sc.setLogLevel("ERROR")

    //把数据加载进来转换为RDD
    val usersRDD = sc.textFile(dataPath+"users.dat")
    val moviesRDD = sc.textFile(dataPath+"movies.dat")
    val ratingsRDD = sc.textFile(dataPath+"ratings.dat")

    //具体数据处理的业务逻辑
    val movieInfo = moviesRDD.map(_.split("::")).map(x =>(x(0),x(1))).cache()
    val ratings = ratingsRDD.map(_.split("::")).map(x => (x(0),x(1),x(2))).cache()
    //1.得到（UserID，Gender）的元组
    val usersGender = usersRDD.map(_.split("::")).map(x => (x(0),x(1)))
    //  ratings.map()执行后的结构(UserID,(UserID,MovieID,Rating))
    //  ratings.map().join()执行后的结构(UserID,((UserID,MovieID,Rating),Gender))
    val genderRatings = ratings.map(x => (x._1,(x._1,x._2,x._3)))
      .join(usersGender).cache()
    genderRatings.take(10).foreach(println)

    //2.连接之后分别过滤出男性和女性用户
    // 过滤之后的元组结构为(UserID,MovieID,Rating)
    val maleFilteredRatings = genderRatings.filter(x => x._2._2.equals("M")).map(x => x._2._1)
    val femaleFilteredRatings = genderRatings.filter(x => x._2._2.equals("F")).map(x => x._2._1)

    //3.男性最喜欢的电影Top10
    //  map()之后的元组结构为(MovieID,(Rating,1))
    //  reduceByKey之后的元组结构为(MovieID,(总分数，总人数))
    //  map()之后的元组结构为(MovieID,总分数/总人数)
    //  join()之后的元组结构为(MovieID,(总分数/总人数,title))
    //  map()之后的元组结构为(总分数/总人数,title)
    println("所有电影中最受男性喜爱的电影Top10:")
    maleFilteredRatings.map(x => (x._2,(x._3.toDouble,1)))
      .reduceByKey((x,y) => (x._1 + y._1,x._2+y._2))
      .map(x => (x._1,x._2._1.toDouble/x._2._2))
      .join(movieInfo)
      .map(item => (item._2._1,item._2._2))
      .sortByKey(false).take(10)
      .foreach(record => println(record._2+"评分为："+record._1))

    //关闭sparkSession
    sc.stop()
  }
}
