package sanbuquFiles.unit12

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @ProjectName: scalaWordCount
  * @Package: sanbuquFiles.unit12
  * @ClassName: Movie_Users_Analyzer_RDD_12_2
  * @Description: 找出最高评分的电影和最多被评分的电影
  * @Author: gulu
  * @CreateDate: 19-2-26 下午4:30
  * @UpdateUser: 更新者
  * @UpdateDate: 19-2-26 下午4:30
  * @UpdateRemark: 更新说明
  * @Version: 1.0
  */
object Movie_Users_Analyzer_RDD_12_2 {
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

    println("所有电影中平均分得分最高的电影：")
    // ratings结构：(x(0),x(1),x(2))
    // x(0)是用户ID，x(1)是电影ID，x(2)是评分
    val ratings = ratingsRDD.map(_.split("::")).map(x => (x(0),x(1),
    x(2))).cache()

    ratings.map(x => (x._2,(x._3.toDouble,1)))    //得到 (电影ID,(评分，1))
      .reduceByKey((x,y) => (x._1+y._1,x._2+y._2)) //得到(电影ID,(x的总评分，评分人数))
      .map(x => (x._2._1.toDouble/x._2._2,x._1))   //得到(平均分，电影ID)
      .sortByKey(false)                 //指定降序排列
      .take(10)                              //取前10行
      .foreach(println)

    println("所有电影中粉丝或者观看人数最多的电影")
    ratings.map(x => (x._2,1))                    //得到(电影ID,1)，1代表一个评分
      .reduceByKey(_+_)                           //得到(电影ID,评分总数)
      .map(x => (x._2,x._1))                      //得到(评分总数，电影ID)
      .sortByKey(false)                 //倒序排序
      .take(10).foreach(println)
  }
}
