package sanbuquFiles.unit12

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @ProjectName: scalaWordCount
  * @Package: sanbuquFiles.unit12
  * @ClassName: Movie_Users_Analyzer_RDD
  * @Description: 类似12.1节中的代码，RDD读取数据并打印出来，
  *              这里做了更多工作：求出最受男性欢迎的前100部电影
  * @Author: gulu
  * @CreateDate: 19-2-26 下午3:43
  * @UpdateUser: 更新者
  * @UpdateDate: 19-2-26 下午3:43
  * @UpdateRemark: 更新说明
  * @Version: 1.0
  *
  *          所用方法汇总
  *          map,join,filter,reduceByKey,sortByKey
  */
object Movie_Users_Analyzer_RDD_12_1 {
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
    // x(0)是MovieID(电影序号)，x(1)是Title(电影名字)
    val movieInfo = moviesRDD.map(_.split("::")).map(x =>(x(0),x(1))).cache()
    // x(0)是UserID(用户序号)，x(1)是MovieID(电影序号)，x(2)是Rating(电影评分)
    val ratings = ratingsRDD.map(_.split("::")).map(x => (x(0),x(1),x(2))).cache()
    //1.得到（UserID，Gender）的元组，x(0)是UserID(用户序号)，x(1)是Gender(用户性别)
    val usersGender = usersRDD.map(_.split("::")).map(x => (x(0),x(1)))

    val genderRatings = ratings
      .map(x => (x._1,(x._1,x._2,x._3)))            //得到(UserID,(UserID,MovieID,Rating))
      .join(usersGender).cache()                    //得到(UserID,((UserID,MovieID,Rating),Gender))
    //打印出(UserID,((UserID,MovieID,Rating)
    println("(UserID,((UserID,MovieID,Rating)")
    genderRatings.take(10).foreach(println)

    //2.连接之后分别过滤出男性和女性用户
    // 过滤之后的元组结构为(UserID,MovieID,Rating)
    val maleFilteredRatings = genderRatings
      .filter(x => x._2._2.equals("M"))             //筛选男性
      .map(x => x._2._1)                            //得到(UserID,MovieID,Rating)
    val femaleFilteredRatings = genderRatings
      .filter(x => x._2._2.equals("F"))             //筛选女性
      .map(x => x._2._1)                            //得到(UserID,MovieID,Rating)

    //3.男性最喜欢的电影Top10
    //  map()之后的元组结构为(MovieID,(Rating,1))
    //  reduceByKey之后的元组结构为(MovieID,(总分数，总人数))
    //  map()之后的元组结构为(MovieID,总分数/总人数)
    //  join()之后的元组结构为(MovieID,(总分数/总人数,title))
    //  map()之后的元组结构为(总分数/总人数,title)
    println("所有电影中最受男性喜爱的电影Top10:")
    maleFilteredRatings.map(x => (x._2,(x._3.toDouble,1)))    //得到(MovieID,(Rating,1))
      .reduceByKey((x,y) => (x._1 + y._1,x._2+y._2))          //得到(MovieID,(总分数，总人数))
      .map(x => (x._1,x._2._1.toDouble/x._2._2))              //得到(MovieID,总分数/总人数)
      .join(movieInfo)                                        //得到(MovieID,(总分数/总人数,title))
      .map(item => (item._2._1,item._2._2))                   //得到(总分数/总人数,title)
      .sortByKey(false).take(10)              //按照从高到低排序
      .foreach(record => println(record._2+"  评分为："+record._1))

    //关闭sparkSession
    sc.stop()
  }
}
