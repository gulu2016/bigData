package sanbuquFiles.unit12

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.HashSet

/**
  *
  * @ProjectName: scalaWordCount
  * @Package: sanbuquFiles.unit12
  * @ClassName: Movie_Users_Analyzer_RDD_12_4
  * @Description: java类作用描述 
  * @Author: gulu
  * @CreateDate: 19-2-26 下午7:29
  * @UpdateUser: 更新者
  * @UpdateDate: 19-2-26 下午7:29
  * @UpdateRemark: 更新说明
  * @Version: 1.0
  */
object Movie_Users_Analyzer_RDD_12_4 {
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

    val targetQQUsers = usersRDD.map(_.split("::"))
      .map(x => (x(0),x(2)))                         //得到(用户ID,年龄)
      .filter(_._2.equals("18"))                     //得到(用户ID,年龄=18)
    val targetTaobaoUsers = usersRDD.map(_.split("::"))
      .map(x => (x(0),x(2)))                         //得到(用户ID,年龄)
      .filter(_._2.equals("25"))                     //得到(用户ID,年龄=25)

    val targetQQUsersSet = HashSet() ++ targetQQUsers.map(_._1).collect()
    val targetTaobaoUsersSet = HashSet() ++ targetTaobaoUsers.map(_._1).collect()

    val targetQQUsersBroadcast = sc.broadcast(targetQQUsersSet)
    val targetTaobaoUsersBroadcast = sc.broadcast(targetTaobaoUsersSet)

    val movieID2Name = moviesRDD.map(_.split("::"))
      .map(x => (x(0),x(1)))                          //得到(电影ID,电影名)
      .collect.toMap
    println("所有电影中QQ或者微型核心目标用户最喜爱电影TopN分析：")
    ratingsRDD.map(_.split("::"))
      .map(x => (x(0),x(1)))                          //得到(用户ID,电影ID)
      .filter(x => targetQQUsersBroadcast.value.contains(x._1))
      .map(x => (x._2,1))                             //得到(电影ID,1),1是评分个数
      .reduceByKey(_+_)                               //得到(电影ID,总评分个数)
      .map(x => (x._2,x._1))                          //得到(总评分个数，电影ID)
      .sortByKey(false)                     //降序排序
      .map(x => (x._2,x._1))                          //得到(电影ID，总评分个数)
      .take(10)
      .map(x => (movieID2Name.getOrElse(x._1,null),x._2))//得到(电影名称，总评分个数)
      .foreach(println)
  }
}
