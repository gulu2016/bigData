package sanbuquFiles.unit12

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @ProjectName: scalaWordCount
  * @Package: sanbuquFiles.unit12
  * @ClassName: Movie_Users_Analyzer_RDD_12_3
  * @Description: 找出最受男性喜欢的top10和最受女性喜爱的top10
  * @Author: gulu
  * @CreateDate: 19-2-26 下午6:26
  * @UpdateUser: 更新者
  * @UpdateDate: 19-2-26 下午6:26
  * @UpdateRemark: 更新说明
  * @Version: 1.0
  */
object Movie_Users_Analyzer_RDD_12_3 {
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

    // ratings结构：(x(0),x(1),x(2))
    // x(0)是用户ID，x(1)是电影ID，x(2)是评分
    val ratings = ratingsRDD.map(_.split("::")).map(x => (x(0),x(1),
      x(2))).cache()
    //users结构：(x(0),x(1))
    //x(0)是用户ID,x(1)是用户性别
    val users = usersRDD.map(_.split("::")).map(x => (x(0),x(1))).cache()
    //过滤出点评系统中男性，女性的数据
    val male = "M"
    val female = "F"
    val genderRatings = ratings
      .map(x => (x._1,(x._1,x._2,x._3)))   //得到(用户ID,(用户ID,电影ID,评分))
      .join(users).cache()                 //得到(用户ID,((用户ID,电影ID,评分),性别))
    val maleFilteredRatings:RDD[(String,String,String)] = genderRatings
      .filter(x => x._2._2.equals("M")) //得到(用户ID,((用户ID,电影ID,评分),性别="M"))
      .map(x => x._2._1)                //得到(用户ID,电影ID,评分)

    val femaleFilteredRatings:RDD[(String,String,String)] = genderRatings
      .filter(x => x._2._2.equals("F")) //得到(用户ID,((用户ID,电影ID,评分),性别="F"))
      .map(x => x._2._1)                //得到(用户ID,电影ID,评分)

    println("所有电影中最受男性喜爱的电影top10:")
    maleFilteredRatings.map(x => (x._2,(x._3.toDouble,1))) //得到(电影ID,(评分,1)),1是打分人数
      .reduceByKey((x,y) => (x._1+y._1,x._2+y._2))   //得到(电影ID,(总分,总评论人数))
      .map(x => (x._2._1.toDouble/x._2._2,x._1))     //得到(平均分,电影ID)
      .sortByKey(false)                    //降序排序
      .map(x =>(x._2,x._1))                          //得到(电影ID,平均分)
      .take(10).foreach(println)

    println("所有电影中最受女性喜爱的电影top10:")
    femaleFilteredRatings.map(x => (x._2,(x._3.toDouble,1))) //得到(电影ID,(评分,1)),1是打分人数
      .reduceByKey((x,y) => (x._1+y._1,x._2+y._2))   //得到(电影ID,(总分,总评论人数))
      .map(x => (x._2._1.toDouble/x._2._2,x._1))     //得到(平均分,电影ID)
      .sortByKey(false)                    //降序排序
      .map(x =>(x._2,x._1))                          //得到(电影ID,平均分)
      .take(10).foreach(println)
  }
}
